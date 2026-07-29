import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Tuple

logger = logging.getLogger("onemeta.trace_validator")

# ─── Valid event sets ─────────────────────────────────────────────────────────

VALID_BACKEND_EVENTS = {
    "SESSION_STARTED",
    "SESSION_ENDED",
    "MIC_FRAME_RECEIVED",
    "VAD_DECISION",
    # VAD observability (Phase 5 additions)
    "SPEECH_SEGMENT_STARTED",
    "SPEECH_SEGMENT_ENDED",
    "AUDIO_STREAM_END_SENT",
    "VAD_FALSE_START",
    # Gemini pipeline
    "AUDIO_SENT_TO_GEMINI",
    "GEMINI_WS_FRAME_RECEIVED",
    "TRANSLATED_TEXT_RECEIVED",
    "TRANSLATED_AUDIO_RECEIVED",
    # Gemini runtime phases
    "GEMINI_FIRST_TOKEN",
    "GEMINI_FIRST_AUDIO",
    "GEMINI_TURN_COMPLETE",
    # Backend publish
    "TEXT_PUBLISHED",
    "AUDIO_PUBLISHED",
    "PIPELINE_ERROR",
    # Server pacing
    "CLIENT_TELEMETRY_RECEIVED",
    "SERVER_PACING_APPLIED",
    "SERVER_PACING_RELEASED",
    "SERVER_PACING_DROPPED",
}

VALID_FRONTEND_EVENTS = {
    "SESSION_STARTED",
    "SESSION_ENDED",
    "TEXT_PACKET_RECEIVED",
    "AUDIO_PACKET_RECEIVED",
    "REACT_RENDER_COMPLETED",
    "PCM_DECODE_STARTED",
    "PCM_DECODE_COMPLETED",
    "AUDIO_SCHEDULED",
    "AUDIO_PLAYBACK_SCHEDULED",
    "AUDIO_PLAYBACK_STARTED",
    "AUDIO_PLAYBACK_COMPLETED",
    # Canonical UX anchors
    "SOURCE_TRANSCRIPT_RENDERED",
    "TARGET_TRANSCRIPT_RENDERED",
    "AUDIO_FIRST_AUDIBLE",
    "AUDIO_CONTEXT_STATE",
    "NEXT_PLAYTIME_UPDATED",
    "PLAYBACK_TRIM_APPLIED",
    "PLAYBACK_DROP_DECISION",
}

ALL_VALID_EVENTS = VALID_BACKEND_EVENTS | VALID_FRONTEND_EVENTS

VALID_COMPONENTS = {"session", "backend", "gemini", "frontend", "pcm", "unknown"}

# ─── Required metadata ────────────────────────────────────────────────────────

REQUIRED_METADATA: Dict[str, List[str]] = {
    "MIC_FRAME_RECEIVED":        ["frame_id", "packet_size", "sample_rate"],
    "VAD_DECISION":              ["is_speech", "frame_id"],
    "SPEECH_SEGMENT_STARTED":    ["speech_start_epoch_ms", "correlation_id"],
    "SPEECH_SEGMENT_ENDED":      ["speech_duration_ms", "silence_frames_elapsed"],
    "AUDIO_STREAM_END_SENT":     ["speech_duration_ms", "debounce_frames"],
    "AUDIO_SENT_TO_GEMINI":      ["frame_id", "packet_size", "sample_rate"],
    "GEMINI_WS_FRAME_RECEIVED":  ["response_type", "chunk_index"],
    "GEMINI_FIRST_TOKEN":        ["text_delta", "epoch_ms"],
    "GEMINI_FIRST_AUDIO":        ["pcm_bytes", "duration_sec", "epoch_ms"],
    "GEMINI_TURN_COMPLETE":      ["turn_audio_chunks"],
    "TRANSLATED_TEXT_RECEIVED":  ["text_length", "cumulative_text_length", "chunk_index"],
    "TRANSLATED_AUDIO_RECEIVED": ["pcm_bytes", "sample_rate", "duration"],
    "TEXT_PUBLISHED":            ["destination", "payload_size"],
    "AUDIO_PUBLISHED":           ["destination", "frame_size", "sample_rate", "duration"],
    "PIPELINE_ERROR":            ["stage", "exception", "message"],
    "TEXT_PACKET_RECEIVED":      ["packet_id", "text_length"],
    "AUDIO_PACKET_RECEIVED":     ["packet_id", "chunk_index"],
    "PCM_DECODE_STARTED":        ["packet_size"],
    "PCM_DECODE_COMPLETED":      ["sample_rate", "channels", "duration_sec"],
    "AUDIO_SCHEDULED":           ["scheduled_time_sec", "delay_sec"],
    "AUDIO_PLAYBACK_SCHEDULED":  ["scheduled_time_sec"],
    "AUDIO_PLAYBACK_STARTED":    ["scheduled_time_sec"],
    "AUDIO_PLAYBACK_COMPLETED":  ["scheduled_time_sec"],
    "AUDIO_FIRST_AUDIBLE":       ["packet_id", "chunk_index", "rms"],
    "AUDIO_CONTEXT_STATE":       ["state"],
    "NEXT_PLAYTIME_UPDATED":     ["next_play_time_before", "next_play_time_after", "increment"],
    "PLAYBACK_DROP_DECISION":    ["reason"],
    "PLAYBACK_TRIM_APPLIED":     ["reason"],
}

# ─── Lifecycle rules ──────────────────────────────────────────────────────────
# (first_event, second_event): second must appear after first in same correlation
LIFECYCLE_RULES: List[Tuple[str, str]] = [
    ("PCM_DECODE_STARTED",       "PCM_DECODE_COMPLETED"),
    ("PCM_DECODE_COMPLETED",     "AUDIO_SCHEDULED"),
    ("AUDIO_SCHEDULED",          "AUDIO_PLAYBACK_SCHEDULED"),
    ("AUDIO_PLAYBACK_SCHEDULED", "AUDIO_PLAYBACK_STARTED"),
    ("AUDIO_PLAYBACK_STARTED",   "AUDIO_PLAYBACK_COMPLETED"),
    ("GEMINI_WS_FRAME_RECEIVED", "TRANSLATED_AUDIO_RECEIVED"),
    ("TRANSLATED_TEXT_RECEIVED", "TEXT_PUBLISHED"),
    ("TRANSLATED_AUDIO_RECEIVED","AUDIO_PUBLISHED"),
    ("AUDIO_PUBLISHED",          "AUDIO_PACKET_RECEIVED"),
    ("TEXT_PUBLISHED",           "TEXT_PACKET_RECEIVED"),
    ("AUDIO_PACKET_RECEIVED",    "PCM_DECODE_STARTED"),
]

# ─── Jitter tolerance ─────────────────────────────────────────────────────────
# Web Audio API performance.now() and Python perf_counter_ns() both use float
# internally. When converting float → int, sub-microsecond precision can produce
# apparent monotonic decreases that are actually floating-point rounding artefacts.
# Anything ≤ MONO_JITTER_TOLERANCE_NS is demoted from ERROR → INFORMATIONAL.
try:
    from .config import AudioConfig
    MONO_JITTER_TOLERANCE_NS = int(AudioConfig().trace_jitter_tolerance_ms * 1_000_000)
except Exception:
    MONO_JITTER_TOLERANCE_NS = 1_000_000  # 1 ms default fallback


# Events that are always client-side (use browser performance.now() timestamps).
# When these arrive with component='unknown' (mis-classified by the frontend tracer),
# they must still be compared only against other frontend monotonic timestamps.
# Cross-comparing browser performance.now() (starts near 0 at page load) against
# Python perf_counter_ns() (system uptime, ~634 trillion ns on Windows) causes
# massive spurious monotonic decrease errors.
_FRONTEND_ONLY_EVENTS = frozenset({
    "PLAYBACK_DROP_DECISION",
    "PLAYBACK_TRIM_APPLIED",
    "PCM_DECODE_STARTED",
    "PCM_DECODE_COMPLETED",
    "AUDIO_SCHEDULED",
    "AUDIO_PLAYBACK_SCHEDULED",
    "AUDIO_PLAYBACK_STARTED",
    "AUDIO_PLAYBACK_COMPLETED",
    "AUDIO_FIRST_AUDIBLE",
    "AUDIO_CONTEXT_STATE",
    "NEXT_PLAYTIME_UPDATED",
    "TEXT_PACKET_RECEIVED",
    "AUDIO_PACKET_RECEIVED",
    "SOURCE_TRANSCRIPT_RENDERED",
    "TARGET_TRANSCRIPT_RENDERED",
    "REACT_RENDER_COMPLETED",
})


def _host_group(component: str, event_name: str = "") -> str:
    c = (component or "").lower()
    if c in ("frontend", "react", "ui", "pcm"):
        return "frontend"
    if c in ("backend", "session", "agent", "processor", "audio", "pipeline"):
        return "backend"
    # Defensive override: if component is 'unknown' but the event is a known
    # client-only event, assign to 'frontend' host group to prevent cross-host
    # clock comparisons (browser performance.now() vs Python perf_counter_ns()).
    if c == "unknown" and event_name in _FRONTEND_ONLY_EVENTS:
        return "frontend"
    return c


def validate_session_trace(trace_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validates a merged session_trace.json.

    Severity classification:
      CRITICAL  — data integrity issues that would corrupt metric calculations
      WARNING   — missing metadata or minor anomalies that reduce confidence
      INFO      — expected artefacts (float jitter, scheduling drift) that are
                  harmless and should NOT mark the session invalid

    Returns a trace_validation.json-compatible report with:
      - trace_valid        (no CRITICAL errors)
      - trace_healthy      (no CRITICAL errors AND no WARNINGS)
      - instrumentation_confidence_pct
      - per-severity buckets: critical_errors, warnings, informational
      - legacy "errors" field kept for backward compatibility (= critical_errors)
    """
    critical_errors: List[str] = []
    warnings: List[str] = []
    informational: List[str] = []

    try:
        session_id_log = trace_data.get("session", {}).get("session_id", "unknown")
        logger.info(
            f"[Validator] Starting validate_session_trace(session_id={session_id_log}) "
            f"— total_events_hint={len(trace_data.get('events', []))} "
            f"— module_file={__file__}"
        )
    except Exception:
        logger.info("[Validator] Starting validate_session_trace")
    logger.info("[Validator] TRACE_VALIDATION_STARTED")

    # ── 1. Envelope schema ────────────────────────────────────────────────────
    if trace_data.get("trace_version") != 1:
        critical_errors.append("Invalid or missing trace_version (expected 1)")

    session = trace_data.get("session", {})
    if not isinstance(session, dict):
        critical_errors.append("session field is missing or not a dictionary")
        session = {}

    if not session.get("session_id"):
        critical_errors.append("session.session_id is missing or empty")
    if not session.get("start_time_epoch_ms"):
        warnings.append("session.start_time_epoch_ms is missing")
    if not session.get("end_time_epoch_ms"):
        warnings.append("session.end_time_epoch_ms is missing")

    events = trace_data.get("events", [])
    if not isinstance(events, list):
        critical_errors.append("events field is missing or not a list")
        events = []

    total_events = len(events)

    # ── 2. Per-event validation ───────────────────────────────────────────────
    last_mono_ns_by_group: Dict[str, int] = {}
    duplicate_events = 0
    ordering_errors = 0   # counts only TRUE ordering errors (not jitter)
    jitter_events = 0     # counts jitter-demoted events

    corr_sequences: Dict[str, List[str]] = {}
    corr_events: Dict[str, List[Dict[str, Any]]] = {}

    component_counts: Dict[str, int] = {c: 0 for c in VALID_COMPONENTS}
    events_per_component: Dict[str, int] = {}
    unique_event_types: set = set()
    seen_event_ids: set = set()
    duplicate_event_ids = 0

    for idx, ev in enumerate(events):
        if not isinstance(ev, dict):
            critical_errors.append(f"Event at index {idx} is not a dictionary")
            continue

        # Required envelope fields
        for field in ("seq", "event", "component", "correlation_id",
                      "timestamp_epoch_ms", "timestamp_monotonic_ns", "event_id"):
            if field not in ev:
                warnings.append(f"Event[{idx}] missing required field '{field}'")

        seq = ev.get("seq", 0)
        event_name = ev.get("event", "")
        component = ev.get("component", "")
        correlation_id = ev.get("correlation_id", "")
        mono_ns = ev.get("timestamp_monotonic_ns", 0)
        host_group = _host_group(component, event_name)

        # Track event types and component counts
        if event_name:
            unique_event_types.add(event_name)
        if component:
            component_counts[component] = component_counts.get(component, 0) + 1
            events_per_component[component] = events_per_component.get(component, 0) + 1

        # event_id uniqueness
        eid = ev.get("event_id")
        if eid is None:
            warnings.append(f"Event[{idx}] missing required field 'event_id'")
        else:
            if eid in seen_event_ids:
                critical_errors.append(f"Event[{idx}] has duplicate event_id: '{eid}'")
                duplicate_event_ids += 1
            else:
                seen_event_ids.add(eid)

        # Sequence continuity
        if seq != idx + 1:
            warnings.append(
                f"Event[{idx}] sequence mismatch: expected seq={idx + 1}, got seq={seq}"
            )
            duplicate_events += 1

        # ── Monotonic timestamp ordering with jitter tolerance ────────────────
        prev_mono = last_mono_ns_by_group.get(host_group, 0)
        if mono_ns < prev_mono:
            decrease = prev_mono - mono_ns
            if decrease <= MONO_JITTER_TOLERANCE_NS:
                # Jitter — Web Audio float precision artefact, not a real violation
                jitter_events += 1
                informational.append(
                    f"Event[{idx}] ({event_name}) monotonic jitter in host_group '{host_group}': "
                    f"decrease={decrease}ns ≤ tolerance={MONO_JITTER_TOLERANCE_NS}ns "
                    f"(float precision artefact, not a real ordering error)"
                )
            else:
                # True ordering violation — flag as critical
                ordering_errors += 1
                critical_errors.append(
                    f"Event[{idx}] ({event_name}) monotonic timestamp decreased for host_group '{host_group}': "
                    f"{mono_ns} < {prev_mono} (decrease={decrease}ns > tolerance={MONO_JITTER_TOLERANCE_NS}ns)"
                )
        last_mono_ns_by_group[host_group] = mono_ns

        # Valid event names
        if event_name and event_name not in ALL_VALID_EVENTS:
            warnings.append(f"Event[{idx}] has unknown event name: '{event_name}'")

        # Valid component names
        if component and component not in VALID_COMPONENTS:
            warnings.append(f"Event[{idx}] ({event_name}) has unknown component: '{component}'")

        # Metadata completeness
        metadata = ev.get("metadata")
        if metadata is None or not isinstance(metadata, dict):
            warnings.append(f"Event[{idx}] ({event_name}) metadata is missing or not a dict")
        else:
            required_fields = REQUIRED_METADATA.get(event_name, [])
            for field in required_fields:
                if field not in metadata:
                    warnings.append(
                        f"Event[{idx}] ({event_name}) missing metadata field: '{field}'"
                    )

        # Collect by correlation ID for lifecycle validation
        if correlation_id:
            if correlation_id not in corr_sequences:
                corr_sequences[correlation_id] = []
                corr_events[correlation_id] = []
            corr_sequences[correlation_id].append(event_name)
            corr_events[correlation_id].append(ev)

    # ── 3. Correlation integrity & lifecycle validation ───────────────────────
    total_correlations = len(corr_sequences)
    complete_correlations = 0
    incomplete_correlations = 0
    orphan_events = 0
    incomplete_reasons: Dict[str, List[str]] = {}

    for corr_id, event_names in corr_sequences.items():
        has_start = any(
            name in ("MIC_FRAME_RECEIVED", "VAD_DECISION", "AUDIO_SENT_TO_GEMINI", "SPEECH_SEGMENT_STARTED")
            for name in event_names
        )
        if not has_start:
            warnings.append(
                f"Correlation '{corr_id}' has no backend start event; possible orphan chain"
            )
            orphan_events += len(event_names)

        # Semantic lifecycle ordering
        def _first_idx(name: str) -> int:
            try:
                return event_names.index(name)
            except ValueError:
                return -1

        def _last_idx(name: str) -> int:
            for i in range(len(event_names) - 1, -1, -1):
                if event_names[i] == name:
                    return i
            return -1

        for first_ev, second_ev in LIFECYCLE_RULES:
            fi = _first_idx(first_ev)
            si = _last_idx(second_ev)
            if fi != -1 and si != -1 and fi > si:
                # Lifecycle rule violation — warning (not critical) because cross-host
                # clock skew can cause apparent inversions that don't affect metric quality
                warnings.append(
                    f"Lifecycle violation in correlation '{corr_id}': "
                    f"'{second_ev}' (pos {si}) occurred before '{first_ev}' (pos {fi})"
                )

        is_complete = (
            "AUDIO_PLAYBACK_SCHEDULED" in event_names
            or "REACT_RENDER_COMPLETED" in event_names
        )
        if is_complete:
            complete_correlations += 1
        else:
            incomplete_correlations += 1
            reasons: List[str] = []
            evs = corr_events.get(corr_id, [])
            for e in evs:
                if e.get("event") == "PIPELINE_ERROR":
                    md = e.get("metadata") or {}
                    reasons.append(f"pipeline_error: {md.get('message') or md.get('exception') or 'PIPELINE_ERROR'}")
            has_published = any(e.get("event") == "AUDIO_PUBLISHED" for e in evs)
            has_received = any(e.get("event") == "AUDIO_PACKET_RECEIVED" for e in evs)
            if has_published and not has_received:
                reasons.append("audio_published_but_not_received_by_frontend")
            has_gemini_final = any(e.get("event") in ("GEMINI_WS_FRAME_RECEIVED", "TRANSLATED_AUDIO_RECEIVED") for e in evs)
            if has_gemini_final and not has_published and not has_received:
                reasons.append("gemini_produced_but_not_published_or_received")
            session_end = trace_data.get("session", {}).get("end_time_epoch_ms")
            last_ev_epoch = None
            for e in reversed(evs):
                if e.get("timestamp_epoch_ms"):
                    last_ev_epoch = e.get("timestamp_epoch_ms")
                    break
            if session_end and last_ev_epoch and session_end <= last_ev_epoch + 1:
                reasons.append("session_ended_before_completion")
            if not reasons:
                event_set = set(event_names)
                minimal_set = {"VAD_DECISION", "MIC_FRAME_RECEIVED", "AUDIO_SENT_TO_GEMINI"}
                if event_set and event_set.issubset(minimal_set):
                    reasons.append("telemetry_insufficient: no downstream frames/publish/receive")
                else:
                    reasons.append("unknown_incompletion")
            warnings.append(f"Correlation '{corr_id}' is incomplete: reasons={', '.join(reasons)}")
            incomplete_reasons[corr_id] = reasons

    # ── 4. Determine Trace Quality ───────────────────────────────────────────
    # Categorical classification:
    #   - EXCELLENT: No critical errors, no warnings, no incomplete correlations
    #   - GOOD: No critical errors, but minor warnings or incomplete correlations exist
    #   - POOR: One or more critical errors, or high warning/incompletion counts
    if len(critical_errors) > 0 or len(warnings) > 10 or incomplete_correlations > 5:
        trace_quality = "POOR"
    elif len(warnings) > 0 or incomplete_correlations > 0 or jitter_events > 5:
        trace_quality = "GOOD"
    else:
        trace_quality = "EXCELLENT"

    # trace_valid = no CRITICAL errors (lifecycle warnings & jitter don't fail validation)
    trace_valid = len(critical_errors) == 0
    # trace_healthy = trace_valid AND no warnings at all
    trace_healthy = trace_valid and len(warnings) == 0

    try:
        logger.info(
            f"[Validator] Completed: trace_valid={trace_valid}, trace_healthy={trace_healthy}, "
            f"critical_errors={len(critical_errors)}, warnings={len(warnings)}, "
            f"informational={len(informational)}, jitter_events={jitter_events}, "
            f"ordering_errors={ordering_errors}, trace_quality={trace_quality}, "
            f"total_correlations={total_correlations}, complete_correlations={complete_correlations}"
        )
    except Exception:
        logger.info("[Validator] Completed validate_session_trace")
    logger.info("[Validator] TRACE_VALIDATION_COMPLETED")

    return {
        "trace_valid": trace_valid,
        "trace_healthy": trace_healthy,
        # Legacy field — kept for backward compatibility with existing consumers
        "validation_passed": trace_valid,
        "total_events": total_events,
        "total_correlations": total_correlations,
        "complete_correlations": complete_correlations,
        "incomplete_correlations": incomplete_correlations,
        "duplicate_events": duplicate_events,
        "ordering_errors": ordering_errors,       # TRUE ordering violations only
        "jitter_events": jitter_events,           # float-precision artefacts (harmless)
        "orphan_events": orphan_events,
        "duplicate_event_ids": duplicate_event_ids,
        "incomplete_reasons": incomplete_reasons,
        "trace_quality": trace_quality,
        "statistics": {
            "backend_events": events_per_component.get("backend", 0),
            "frontend_events": events_per_component.get("frontend", 0),
            "gemini_events": events_per_component.get("gemini", 0),
            "pcm_events": events_per_component.get("pcm", 0),
            "session_events": events_per_component.get("session", 0),
            "unique_event_types": len(unique_event_types),
            "unique_components": len(events_per_component),
            "events_per_component": events_per_component,
        },
        # Severity-bucketed results
        "critical_errors": critical_errors,
        "warnings": warnings,
        "informational": informational,
        # Legacy "errors" field kept for any downstream consumers
        "errors": critical_errors,
    }
