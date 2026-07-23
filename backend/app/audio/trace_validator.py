import json
import logging
from pathlib import Path
from typing import Dict, Any, List

logger = logging.getLogger("onemeta.trace_validator")

# All valid event names across backend and frontend
VALID_BACKEND_EVENTS = {
    "SESSION_STARTED",
    "SESSION_ENDED",
    "MIC_FRAME_RECEIVED",
    "VAD_DECISION",
    "AUDIO_SENT_TO_GEMINI",
    "GEMINI_WS_FRAME_RECEIVED",
    "TRANSLATED_TEXT_RECEIVED",
    "TRANSLATED_AUDIO_RECEIVED",
    "TEXT_PUBLISHED",
    "AUDIO_PUBLISHED",
    "PIPELINE_ERROR",
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
}

ALL_VALID_EVENTS = VALID_BACKEND_EVENTS | VALID_FRONTEND_EVENTS

VALID_COMPONENTS = {"session", "backend", "gemini", "frontend", "pcm", "unknown"}

# Required metadata fields per event type
REQUIRED_METADATA: Dict[str, List[str]] = {
    "MIC_FRAME_RECEIVED":       ["frame_id", "packet_size", "sample_rate"],
    "VAD_DECISION":             ["is_speech", "frame_id"],
    "AUDIO_SENT_TO_GEMINI":     ["frame_id", "packet_size", "sample_rate"],
    "GEMINI_WS_FRAME_RECEIVED": ["response_type", "chunk_index"],
    "TRANSLATED_TEXT_RECEIVED": ["text_length", "cumulative_text_length", "chunk_index"],
    "TRANSLATED_AUDIO_RECEIVED":["pcm_bytes", "sample_rate", "duration"],
    "TEXT_PUBLISHED":           ["destination", "payload_size"],
    "AUDIO_PUBLISHED":          ["destination", "frame_size", "sample_rate", "duration"],
    "PIPELINE_ERROR":           ["stage", "exception", "message"],
    "TEXT_PACKET_RECEIVED":     ["packet_id", "text_length"],
    "AUDIO_PACKET_RECEIVED":    ["packet_id", "chunk_index"],
    "PCM_DECODE_STARTED":       ["packet_size"],
    "PCM_DECODE_COMPLETED":     ["sample_rate", "channels", "duration_sec"],
    "AUDIO_SCHEDULED":          ["scheduled_time_sec", "delay_sec"],
    "AUDIO_PLAYBACK_SCHEDULED": ["scheduled_time_sec"],
}

# Semantic lifecycle rules: (first_event, second_event)
# second_event must occur at or after first_event within the same correlation_id chain
LIFECYCLE_RULES = [
    ("PCM_DECODE_STARTED",        "PCM_DECODE_COMPLETED"),
    ("PCM_DECODE_COMPLETED",      "AUDIO_SCHEDULED"),
    ("AUDIO_SCHEDULED",           "AUDIO_PLAYBACK_SCHEDULED"),
    ("GEMINI_WS_FRAME_RECEIVED",  "TRANSLATED_AUDIO_RECEIVED"),
    ("TRANSLATED_TEXT_RECEIVED",  "TEXT_PUBLISHED"),
    ("TRANSLATED_AUDIO_RECEIVED", "AUDIO_PUBLISHED"),
    ("AUDIO_PUBLISHED",           "AUDIO_PACKET_RECEIVED"),
    ("TEXT_PUBLISHED",            "TEXT_PACKET_RECEIVED"),
    ("AUDIO_PACKET_RECEIVED",     "PCM_DECODE_STARTED"),
]


def validate_session_trace(trace_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validates a merged session_trace.json containing both backend and frontend events.

    Checks:
      1. Envelope schema (trace_version, session fields)
      2. Per-event required fields
      3. Valid event names and component names
      4. Metadata completeness for known event types
      5. Strictly increasing sequence numbers
      6. Non-decreasing monotonic timestamps
      7. Semantic lifecycle ordering within each correlation ID chain
      8. Correlation ID integrity (orphan detection, completeness)

    Returns a trace_validation.json-compatible report dictionary.
    Does NOT calculate any latency metrics — raw data only.
    """
    errors: List[str] = []
    warnings: List[str] = []

    try:
        session_id_log = trace_data.get("session", {}).get("session_id", "unknown")
        logger.info(f"[Validator] Starting validate_session_trace(session_id={session_id_log}) — total_events_hint={len(trace_data.get('events', []))} — module_file={__file__}")
    except Exception:
        logger.info("[Validator] Starting validate_session_trace")
    logger.info("[Validator] TRACE_VALIDATION_STARTED")

    # ── 1. Envelope schema ────────────────────────────────────────────────────
    if trace_data.get("trace_version") != 1:
        errors.append("Invalid or missing trace_version (expected 1)")

    session = trace_data.get("session", {})
    if not isinstance(session, dict):
        errors.append("session field is missing or not a dictionary")
        session = {}

    if not session.get("session_id"):
        errors.append("session.session_id is missing or empty")
    if not session.get("start_time_epoch_ms"):
        errors.append("session.start_time_epoch_ms is missing")
    if not session.get("end_time_epoch_ms"):
        errors.append("session.end_time_epoch_ms is missing")

    events = trace_data.get("events", [])
    if not isinstance(events, list):
        errors.append("events field is missing or not a list")
        events = []

    total_events = len(events)

    # ── 2. Per-event validation ────────────────────────────────────────────────
    # Track last monotonic timestamp per host-group to avoid comparing
    # monotonic clocks from different machines (false positives).
    last_mono_ns_by_group: Dict[str, int] = {}
    duplicate_events = 0
    ordering_errors = 0

    # Group by correlation_id for lifecycle validation
    corr_sequences: Dict[str, List[str]] = {}  # corr_id -> list of event names in order
    corr_events: Dict[str, List[Dict[str, Any]]] = {}  # corr_id -> list of full event dicts in order

    # Telemetry statistics collectors
    component_counts = {c: 0 for c in VALID_COMPONENTS}
    events_per_component: Dict[str, int] = {}
    unique_event_types = set()
    seen_event_ids = set()
    duplicate_event_ids = 0

    for idx, ev in enumerate(events):
        if not isinstance(ev, dict):
            errors.append(f"Event at index {idx} is not a dictionary")
            continue

        # Required envelope fields
        for field in ("seq", "event", "component", "correlation_id",
                      "timestamp_epoch_ms", "timestamp_monotonic_ns", "event_id"):
            if field not in ev:
                errors.append(f"Event[{idx}] missing required field '{field}'")

        seq = ev.get("seq", 0)
        event_name = ev.get("event", "")
        component = ev.get("component", "")
        correlation_id = ev.get("correlation_id", "")
        mono_ns = ev.get("timestamp_monotonic_ns", 0)
        # Determine host-group for this component
        def _host_group_from_component(comp: str) -> str:
            c = (comp or "").lower()
            if c in ("frontend", "react", "ui", "pcm"):
                return "frontend"
            if c in ("backend", "session", "agent", "processor", "audio", "pipeline"):
                return "backend"
            return c
        host_group = _host_group_from_component(component)

        # Track event types and component counts
        if event_name:
            unique_event_types.add(event_name)
        if component:
            if component in component_counts:
                component_counts[component] += 1
            else:
                component_counts.setdefault(component, 0)
                component_counts[component] += 1
            events_per_component[component] = events_per_component.get(component, 0) + 1

        # Validate event_id uniqueness
        eid = ev.get("event_id")
        if eid is None:
            errors.append(f"Event[{idx}] missing required field 'event_id'")
        else:
            if eid in seen_event_ids:
                errors.append(f"Event[{idx}] has duplicate event_id: '{eid}'")
                duplicate_event_ids += 1
            else:
                seen_event_ids.add(eid)

        # Expected seq == idx + 1 after re-sequencing
        if seq != idx + 1:
            errors.append(
                f"Event[{idx}] sequence mismatch: expected seq={idx + 1}, got seq={seq}"
            )
            duplicate_events += 1

        # Monotonic timestamps must never decrease within the same host-group.
        prev_mono = last_mono_ns_by_group.get(host_group, 0)
        if mono_ns < prev_mono:
            errors.append(
                f"Event[{idx}] ({event_name}) monotonic timestamp decreased for host_group '{host_group}': "
                f"{mono_ns} < {prev_mono}"
            )
            ordering_errors += 1
        last_mono_ns_by_group[host_group] = mono_ns

        # Valid event names
        if event_name and event_name not in ALL_VALID_EVENTS:
            errors.append(f"Event[{idx}] has unknown event name: '{event_name}'")

        # Valid component names
        if component and component not in VALID_COMPONENTS:
            errors.append(f"Event[{idx}] ({event_name}) has unknown component: '{component}'")

        # Metadata completeness
        metadata = ev.get("metadata")
        if metadata is None or not isinstance(metadata, dict):
            errors.append(f"Event[{idx}] ({event_name}) metadata is missing or not a dict")
        else:
            required_fields = REQUIRED_METADATA.get(event_name, [])
            for field in required_fields:
                if field not in metadata:
                    errors.append(
                        f"Event[{idx}] ({event_name}) missing required metadata field: '{field}'"
                    )

        # Collect by correlation ID for lifecycle validation (non-empty corr only)
        if correlation_id:
            if correlation_id not in corr_sequences:
                corr_sequences[correlation_id] = []
                corr_events[correlation_id] = []
            corr_sequences[correlation_id].append(event_name)
            corr_events[correlation_id].append(ev)

    # ── 3. Correlation integrity & lifecycle validation ──────────────────────
    total_correlations = len(corr_sequences)
    complete_correlations = 0
    incomplete_correlations = 0
    orphan_events = 0

    incomplete_reasons: Dict[str, List[str]] = {}

    for corr_id, event_names in corr_sequences.items():
        # Orphan: no backend start event present
        has_start = any(
            name in ("MIC_FRAME_RECEIVED", "VAD_DECISION", "AUDIO_SENT_TO_GEMINI")
            for name in event_names
        )
        if not has_start:
            warnings.append(
                f"Correlation '{corr_id}' has no backend start event "
                f"(MIC_FRAME_RECEIVED / VAD_DECISION / AUDIO_SENT_TO_GEMINI); "
                f"possible orphan chain"
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
            # Only check if both events exist in this correlation chain
            if fi != -1 and si != -1 and fi > si:
                errors.append(
                    f"Lifecycle violation in correlation '{corr_id}': "
                    f"'{second_ev}' (pos {si}) occurred before '{first_ev}' (pos {fi})"
                )

        # Completeness: a correlation is complete if it reached scheduling or text render
        is_complete = (
            "AUDIO_PLAYBACK_SCHEDULED" in event_names
            or "REACT_RENDER_COMPLETED" in event_names
        )
        if is_complete:
            complete_correlations += 1
        else:
            incomplete_correlations += 1
            reasons: List[str] = []
            # Check for pipeline errors
            evs = corr_events.get(corr_id, [])
            last_err = None
            for e in evs:
                if e.get("event") == "PIPELINE_ERROR":
                    md = e.get("metadata") or {}
                    last_err = md.get("message") or md.get("exception") or "PIPELINE_ERROR"
            if last_err:
                reasons.append(f"pipeline_error: {last_err}")

            # If audio was published but never received by frontend
            has_published = any(e.get("event") == "AUDIO_PUBLISHED" for e in evs)
            has_packet_received = any(e.get("event") == "AUDIO_PACKET_RECEIVED" for e in evs)
            if has_published and not has_packet_received:
                reasons.append("audio_published_but_not_received_by_frontend")

            # If gemini produced final frames but no playback scheduled
            has_gemini_final = any((e.get("event") in ("GEMINI_WS_FRAME_RECEIVED", "TRANSLATED_AUDIO_RECEIVED")) for e in evs)
            if has_gemini_final and not has_published and not has_packet_received:
                reasons.append("gemini_produced_but_not_published_or_received")

            # Session end may have truncated this correlation
            session_end = trace_data.get("session", {}).get("end_time_epoch_ms")
            last_event_epoch = None
            for e in reversed(evs):
                if e.get("timestamp_epoch_ms"):
                    last_event_epoch = e.get("timestamp_epoch_ms")
                    break
            if session_end and last_event_epoch and session_end <= last_event_epoch + 1:
                reasons.append("session_ended_before_completion")

            if not reasons:
                # If the chain only contains backend send events to Gemini and no downstream
                # frames / publish / frontend receive events, we cannot determine why it
                # didn't complete from available telemetry.
                event_set = set(event_names)
                minimal_set = {"VAD_DECISION", "MIC_FRAME_RECEIVED", "AUDIO_SENT_TO_GEMINI"}
                if event_set and event_set.issubset(minimal_set):
                    reasons.append(
                        "telemetry_insufficient: no downstream frames/publish/receive"
                    )
                else:
                    reasons.append("unknown_incompletion")

            warnings.append(f"Correlation '{corr_id}' is incomplete: reasons={', '.join(reasons)}")
            incomplete_reasons[corr_id] = reasons

    trace_valid = len(errors) == 0
    validation_passed = trace_valid

    try:
        logger.info(f"[Validator] Completed validate_session_trace: trace_valid={trace_valid}, ordering_errors={ordering_errors}, total_correlations={total_correlations}, complete_correlations={complete_correlations}")
    except Exception:
        logger.info("[Validator] Completed validate_session_trace")
    logger.info("[Validator] TRACE_VALIDATION_COMPLETED")

    return {
        "trace_valid": trace_valid,
        "validation_passed": validation_passed,
        "total_events": total_events,
        "total_correlations": total_correlations,
        "complete_correlations": complete_correlations,
        "incomplete_correlations": incomplete_correlations,
        "duplicate_events": duplicate_events,
        "ordering_errors": ordering_errors,
        "orphan_events": orphan_events,
        "duplicate_event_ids": duplicate_event_ids,
        "incomplete_reasons": incomplete_reasons,
        "statistics": {
            "backend_events": component_counts.get("backend", 0),
            "frontend_events": component_counts.get("frontend", 0),
            "gemini_events": component_counts.get("gemini", 0),
            "pcm_events": component_counts.get("pcm", 0),
            "session_events": component_counts.get("session", 0),
            "unique_event_types": len(unique_event_types),
            "unique_components": len(events_per_component),
            "events_per_component": events_per_component,
        },
        "warnings": warnings,
        "errors": errors,
    }
