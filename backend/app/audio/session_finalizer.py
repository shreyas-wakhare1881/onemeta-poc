import json
import logging
import uuid
import threading
from pathlib import Path
from typing import Dict, Any, List, Optional

from .trace_validator import validate_session_trace

logger = logging.getLogger("onemeta.session_finalizer")

# Sentinel field written into the merged trace to prevent double-merging
_MERGE_MARKER = "merged_sources"

_session_locks = {}
_locks_mutex = threading.Lock()


def get_session_lock(session_dir: Path) -> threading.Lock:
    """Returns a thread-safe Lock specific to the given session directory."""
    path_key = str(Path(session_dir).resolve())
    with _locks_mutex:
        if path_key not in _session_locks:
            _session_locks[path_key] = threading.Lock()
        return _session_locks[path_key]


def finalize_session_trace(session_dir: Path) -> None:
    """
    Merges backend session_trace.json with frontend.log (frontend trace), re-sequences
    events by timestamp, and generates trace_validation.json.

    Safe to call multiple times — uses a merge marker to detect whether a trace has
    already been merged and avoids re-merging backend events that are already present.

    Call order (both are safe):
      - From agent.py finally block (backend events only, no frontend yet)
      - From audio.py upload_artifacts route (triggered when frontend uploads)

    The second call will always produce the definitive merged result.
    """
    session_lock = get_session_lock(session_dir)
    with session_lock:
        backend_path = session_dir / "session_trace.json"
        frontend_path = session_dir / "frontend.log"
        validation_path = session_dir / "trace_validation.json"

        logger.info(f"[Finalizer] Enter finalize_session_trace(session_dir={session_dir}) — module_file={__file__}")
        logger.info("[Finalizer] FINALIZER_STARTED")

        if not backend_path.exists() and not frontend_path.exists():
            logger.warning(f"[Finalizer] No traces found in {session_dir}")
            logger.info("[Finalizer] FINALIZER_COMPLETED — no_traces_found")
            return

        # ── Load backend trace ────────────────────────────────────────────────────
        backend_trace: Optional[Dict[str, Any]] = None
        if backend_path.exists():
            try:
                with open(backend_path, "r", encoding="utf-8") as f:
                    backend_trace = json.load(f)
            except Exception as e:
                logger.exception(f"[Finalizer] Failed to read session_trace.json: {e}")

        # ── Load frontend trace ────────────────────────────────────────────────────
        frontend_trace: Optional[Dict[str, Any]] = None
        if frontend_path.exists():
            try:
                with open(frontend_path, "r", encoding="utf-8") as f:
                    frontend_trace = json.load(f)
            except Exception as e:
                logger.exception(f"[Finalizer] Failed to read frontend.log: {e}")

        if not backend_trace and not frontend_trace:
            logger.warning(f"[Finalizer] Both traces are empty or unreadable in {session_dir}")
            logger.info("[Finalizer] FINALIZER_COMPLETED — traces_unreadable")
            return

        # ── Detect if backend trace is already a merged, final result
        # A trace is considered finalized & immutable only if it already contains
        # both backend and frontend sources in the merge marker. This preserves
        # the ability to overwrite a backend-only trace until the frontend has
        # been merged in.
        already_merged = (
            backend_trace is not None
            and isinstance(backend_trace.get(_MERGE_MARKER), list)
            and "frontend" in (backend_trace.get(_MERGE_MARKER) or [])
        )

        # ── Resolve session metadata ───────────────────────────────────────────────
        session_id = "unknown"
        start_time: float = 0
        end_time: float = 0
        trace_version = 1

        for src in (backend_trace, frontend_trace):
            if not src:
                continue
            trace_version = src.get("trace_version", trace_version)
            si = src.get("session", {})
            if si.get("session_id"):
                session_id = si["session_id"]
            if si.get("start_time_epoch_ms"):
                t = si["start_time_epoch_ms"]
                start_time = t if start_time == 0 else min(start_time, t)
            if si.get("end_time_epoch_ms"):
                end_time = max(end_time, si["end_time_epoch_ms"])

        # ── Build merged event list ────────────────────────────────────────────────
        # If already merged, backend_trace already contains all backend events;
        # just add new frontend events (dedup by checking if any SESSION_STARTED
        # component=session events are duplicated would be complex, so we rely on
        # the fact that frontend.log is source of truth for frontend events and
        # backend_trace's events for backend events when already merged).
        merged_events: List[Dict[str, Any]] = []

        if already_merged:
            # Strip out any previously merged frontend events from backend_trace
            # to avoid duplicates — identify them by component being 'frontend' or 'pcm'
            backend_only_events = [
                ev for ev in (backend_trace.get("events") or [])
                if ev.get("component") not in ("frontend", "pcm")
                   or ev.get("event") in ("SESSION_STARTED", "SESSION_ENDED")
            ]
            merged_events.extend(backend_only_events)
        elif backend_trace:
            merged_events.extend(backend_trace.get("events") or [])

        if frontend_trace:
            # Add frontend events, excluding SESSION_STARTED/SESSION_ENDED duplicates
            # (those are emitted by both sides; keep backend's canonical versions)
            existing_session_events = {
                ev["event"] for ev in merged_events
                if ev.get("event") in ("SESSION_STARTED", "SESSION_ENDED")
            }
            for ev in (frontend_trace.get("events") or []):
                if ev.get("event") in existing_session_events and ev.get("component") == "session":
                    continue  # Skip duplicate session lifecycle events from frontend
                merged_events.append(ev)

        # ── Sort by epoch timestamp, use monotonic as tiebreaker ──────────────────
        merged_events.sort(key=lambda ev: (
            ev.get("timestamp_epoch_ms", 0),
            ev.get("timestamp_monotonic_ns", 0)
        ))

        # ── Ensure every event has a stable `event_id` (generate if missing)
        for ev in merged_events:
            if not ev.get("event_id"):
                ev["event_id"] = f"evt_{uuid.uuid4().hex}"

        # ── Re-sequence from 1 (sequence numbers may change but event_id remains stable)
        for idx, ev in enumerate(merged_events):
            ev["seq"] = idx + 1

        # ── Track merged sources ──────────────────────────────────────────────────
        merged_sources = []
        if backend_trace:
            merged_sources.append("backend")
        if frontend_trace:
            merged_sources.append("frontend")

        # ── Build final trace ─────────────────────────────────────────────────────
        final_trace: Dict[str, Any] = {
            "trace_version": trace_version,
            _MERGE_MARKER: merged_sources,
            "session": {
                "session_id": session_id,
                "start_time_epoch_ms": start_time,
                "end_time_epoch_ms": end_time
            },
            "events": merged_events
        }

        # ── Write merged session_trace.json ───────────────────────────────────────
        try:
            if already_merged:
                # session_trace.json already contains a final merged result that
                # included frontend events — never overwrite the canonical file.
                logger.info(f"[Finalizer] Existing finalized session_trace.json found; not overwriting → {backend_path}")
                # If any events in the existing canonical file lack event_id, write
                # an enriched copy for downstream consumption instead of mutating
                # the immutable canonical artifact.
                existing_missing_ids = any(
                    not (ev.get("event_id")) for ev in (backend_trace.get("events") or [])
                )
                if existing_missing_ids:
                    enriched = dict(backend_trace)
                    enriched_events = []
                    for ev in (enriched.get("events") or []):
                        if not ev.get("event_id"):
                            ev = dict(ev)
                            ev["event_id"] = f"evt_{uuid.uuid4().hex}"
                        enriched_events.append(ev)
                    enriched["events"] = enriched_events
                    enriched_path = session_dir / "session_trace.enriched.json"
                    with open(enriched_path, "w", encoding="utf-8") as f:
                        json.dump(enriched, f, indent=2)
                    logger.info(f"[Finalizer] Wrote enriched session trace (event_ids added) → {enriched_path}")
                # Validate the (immutable) canonical trace in-place
            else:
                with open(backend_path, "w", encoding="utf-8") as f:
                    json.dump(final_trace, f, indent=2)
                logger.info(
                    f"[Finalizer] Saved merged session_trace.json "
                    f"({len(merged_events)} events, sources={merged_sources}) → {backend_path}"
                )
            logger.info(f"[Finalizer] TRACE_MERGED — merged_sources={merged_sources}")
        except Exception as e:
            logger.exception(f"[Finalizer] Failed to write session_trace.json: {e}")
            logger.info("[Finalizer] FINALIZER_COMPLETED — failed_writing_merged_trace")
            return

        # ── Run validation and write trace_validation.json ────────────────────────
        try:
            logger.info("[Finalizer] TRACE_VALIDATION_STARTED")
            # If the canonical file was already finalized, prefer validating the
            # canonical trace (or the enriched copy if we produced one). Otherwise
            # validate the newly-produced final_trace.
            to_validate = final_trace
            if already_merged:
                # If we produced an enriched file, validate that enriched copy; else
                # validate the canonical backend_trace as-is.
                enriched_path = session_dir / "session_trace.enriched.json"
                if enriched_path.exists():
                    with open(enriched_path, "r", encoding="utf-8") as f:
                        to_validate = json.load(f)
                else:
                    to_validate = backend_trace

            report = validate_session_trace(to_validate)
            with open(validation_path, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2)
            status = "PASSED" if report["validation_passed"] else "FAILED"
            logger.info(
                f"[Finalizer] trace_validation.json written ({status}, "
                f"errors={len(report['errors'])}, warnings={len(report['warnings'])}) "
                f"→ {validation_path}"
            )
            logger.info(f"[Finalizer] TRACE_VALIDATION_COMPLETED — status={status} ordering_errors={report.get('ordering_errors')} total_correlations={report.get('total_correlations')} complete_correlations={report.get('complete_correlations')}")
            logger.info(f"[Finalizer] Validation summary: ordering_errors={report.get('ordering_errors')}, total_correlations={report.get('total_correlations')}, complete_correlations={report.get('complete_correlations')}")
        except Exception as e:
            logger.exception(f"[Finalizer] Failed to generate trace_validation.json: {e}")
            logger.info("[Finalizer] FINALIZER_COMPLETED — validation_failed")
        
        # ── Compute metrics.json from the trace (always attempt, even if validation
        #     reported failures). Use the validated/enriched trace when available,
        #     otherwise fall back to the merged final_trace.
        try:
            trace_for_metrics = locals().get("to_validate", final_trace)
            try:
                # Import locally to avoid adding a hard dependency at module import time
                from . import metrics_engine

                logger.info("[Finalizer] METRICS_GENERATION_STARTED")
                logger.info("[Finalizer] Metrics generation starting — invoking metrics_engine.compute_session_metrics()")
                metrics = metrics_engine.compute_session_metrics(trace_for_metrics)
                metrics_path = session_dir / "metrics.json"
                with open(metrics_path, "w", encoding="utf-8") as f:
                    json.dump(metrics, f, indent=2)
                logger.info(f"[Finalizer] METRICS_JSON_WRITTEN — path={metrics_path} count={len(metrics.get('per_correlation', {}))}")
                logger.info("[Finalizer] METRICS_GENERATION_COMPLETED")
                logger.info(f"[Finalizer] metrics.json written ({len(metrics.get('per_correlation', {}))} correlations) → {metrics_path}")

                # Generate benchmark.json from metrics.json (Phase 4)
                try:
                    from . import benchmark_engine
                    benchmark_engine.generate_benchmark(session_dir)
                except Exception as be:
                    logger.exception(f"[Finalizer] Benchmark generation failed: {be}")
            except Exception as me:
                # Print full traceback for diagnostics
                logger.exception(f"[Finalizer] Metrics engine failed: {me}")
                raise
        except Exception as e:
            # Defensive: ensure finalizer never crashes silently — re-raise after logging
            logger.exception(f"[Finalizer] Unexpected error while generating metrics: {e}")
            raise
        logger.info("[Finalizer] FINALIZER_COMPLETED — success")

