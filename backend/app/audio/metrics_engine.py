"""
Lightweight Metrics Engine (Phase 3)

Reads a canonical `session_trace.json` and emits `metrics.json` containing
per-correlation latency metrics and session-level statistics.

Usage (from repo root):
  python -m backend.app.audio.metrics_engine --trace path/to/session_trace.json

Design principles:
- Single-file implementation for simplicity and maintainability.
- Never modify the input trace; only read and derive metrics.
- Prefer monotonic timestamps for same-host deltas, fall back to epoch.
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence
import logging


# Metrics engine metadata
METRICS_ENGINE_VERSION = "0.2.0"
METRICS_SCHEMA_VERSION = 1

logger = logging.getLogger("onemeta.metrics_engine")


def load_trace(trace_path: Path) -> Dict[str, Any]:
    with open(trace_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _sort_events(events: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        list(events),
        key=lambda ev: (
            ev.get("timestamp_epoch_ms") or 0,
            ev.get("timestamp_monotonic_ns") or 0,
            ev.get("seq") or 0,
        ),
    )


def _delta_ms(a: Dict[str, Any], b: Dict[str, Any]) -> Optional[float]:
    """Compute delta (b - a) in milliseconds.

    Rules:
    - If both events are from the same component and both have monotonic
      timestamps, use monotonic_ns (high-precision, safe on same host).
    - Else, prefer epoch timestamps (`timestamp_epoch_ms`) when available.
    - Return None if no compatible timestamp pair is available.
    """
    if a is None or b is None:
        return None

    a_mono = a.get("timestamp_monotonic_ns")
    b_mono = b.get("timestamp_monotonic_ns")
    a_epoch = a.get("timestamp_epoch_ms")
    b_epoch = b.get("timestamp_epoch_ms")

    def _host_group(ev: Dict[str, Any]) -> str:
        comp = (ev.get("component") or "").lower()
        if comp in ("frontend", "react", "ui"):
            return "frontend"
        if comp in ("backend", "session", "agent", "processor", "audio", "pipeline"):
            return "backend"
        # treat 3rd-party services as their own group
        return comp

    # Use monotonic timestamps only when both events originate from the same host group
    try:
        same_host = _host_group(a) == _host_group(b)
    except Exception:
        same_host = False

    if same_host and a_mono is not None and b_mono is not None:
        return (float(b_mono) - float(a_mono)) / 1_000_000.0

    # Prefer epoch timestamps across hosts
    if a_epoch is not None and b_epoch is not None:
        return float(b_epoch) - float(a_epoch)

    # Do not attempt to mix monotonic and epoch across hosts — return None when incompatible
    return None


def _first_event(events: Sequence[Dict[str, Any]], names: Sequence[str]) -> Optional[Dict[str, Any]]:
    for n in names:
        for ev in events:
            if str(ev.get("event")) == n:
                return ev
    return None


def _last_event(events: Sequence[Dict[str, Any]], names: Sequence[str]) -> Optional[Dict[str, Any]]:
    for ev in reversed(list(events)):
        if str(ev.get("event")) in names:
            return ev
    return None


def compute_correlation_metrics(events: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute metrics for a single correlation's ordered events.

    Returns a dictionary of computed metric values (milliseconds) or None.
    """
    evs = _sort_events(events)

    # Quick lookups and lists
    first_mic = _first_event(evs, ["MIC_FRAME_RECEIVED"])
    first_text = _first_event(evs, ["TEXT_PACKET_RECEIVED", "TRANSLATED_TEXT_RECEIVED"])
    first_audio_frontend = _first_event(evs, ["AUDIO_PACKET_RECEIVED"])
    first_audio_backend = _first_event(evs, ["TRANSLATED_AUDIO_RECEIVED"])
    audio_published = _first_event(evs, ["AUDIO_PUBLISHED"])

    # Last-occurring completion markers (use last chunk's marker for multi-chunk utterances)
    last_audio_playback_scheduled = _last_event(evs, ["AUDIO_PLAYBACK_SCHEDULED"])
    last_react_render_completed = _last_event(evs, ["REACT_RENDER_COMPLETED"])

    # Build chunk / packet maps for pairing
    chunk_indices = set()
    packet_ids = set()
    for ev in evs:
        md = ev.get("metadata") or {}
        if "chunk_index" in md:
            try:
                chunk_indices.add(int(md.get("chunk_index")))
            except Exception:
                chunk_indices.add(md.get("chunk_index"))
        if "packet_id" in md:
            packet_ids.add(str(md.get("packet_id")))

    metrics: Dict[str, Optional[float]] = {}

    # Time To First Text: split into arrival vs render
    if first_mic and first_text:
        metrics["time_to_first_text_arrival_ms"] = _delta_ms(first_mic, first_text)
    else:
        metrics["time_to_first_text_arrival_ms"] = None

    # Render-visible TTFT: mic -> first render
    first_render = _first_event(evs, ["REACT_RENDER_COMPLETED"])
    if first_mic and first_render:
        metrics["time_to_first_text_render_ms"] = _delta_ms(first_mic, first_render)
    else:
        metrics["time_to_first_text_render_ms"] = None

    # Text render latency (text packet -> react render) if both exist
    text_render_latencies: List[float] = []
    if packet_ids:
        for pid in packet_ids:
            text_ev = next((e for e in evs if e.get("event") in ("TEXT_PACKET_RECEIVED", "TRANSLATED_TEXT_RECEIVED") and str((e.get("metadata") or {}).get("packet_id")) == pid), None)
            render_ev = next((e for e in evs if e.get("event") == "REACT_RENDER_COMPLETED" and str((e.get("metadata") or {}).get("packet_id")) == pid), None)
            if text_ev and render_ev:
                d = _delta_ms(text_ev, render_ev)
                if d is not None:
                    text_render_latencies.append(d)
    # fallback to first pair
    if not text_render_latencies and first_text and first_render:
        d = _delta_ms(first_text, first_render)
        if d is not None:
            text_render_latencies.append(d)

    metrics["text_render_latency_ms"] = float(statistics.median(text_render_latencies)) if text_render_latencies else None

    # Time To First Audio: prefer frontend receive then backend generated
    if first_mic and first_audio_frontend:
        metrics["time_to_first_audio_frontend_ms"] = _delta_ms(first_mic, first_audio_frontend)
    else:
        metrics["time_to_first_audio_frontend_ms"] = None

    if first_mic and first_audio_backend:
        metrics["time_to_first_audio_backend_ms"] = _delta_ms(first_mic, first_audio_backend)
    else:
        metrics["time_to_first_audio_backend_ms"] = None

    # End-to-End: MIC_FRAME_RECEIVED -> last AUDIO_PLAYBACK_SCHEDULED (prefer last)
    end_marker = last_audio_playback_scheduled or last_react_render_completed
    if first_mic and end_marker:
        metrics["end_to_end_ms"] = _delta_ms(first_mic, end_marker)
    else:
        metrics["end_to_end_ms"] = None

    # Gemini processing time: compute per-chunk AUDIO_SENT_TO_GEMINI -> TRANSLATED_AUDIO_RECEIVED
    gemini_latencies: List[float] = []
    if chunk_indices:
        for idx in chunk_indices:
            sent = next((e for e in evs if e.get("event") == "AUDIO_SENT_TO_GEMINI" and (e.get("metadata") or {}).get("chunk_index") == idx), None)
            recv = next((e for e in reversed(evs) if e.get("event") in ("TRANSLATED_AUDIO_RECEIVED", "GEMINI_WS_FRAME_RECEIVED") and (e.get("metadata") or {}).get("chunk_index") == idx), None)
            if sent and recv:
                d = _delta_ms(sent, recv)
                if d is not None:
                    gemini_latencies.append(d)
    # fallback to first recorded pair
    if not gemini_latencies:
        audio_sent_to_gemini = _first_event(evs, ["AUDIO_SENT_TO_GEMINI"])
        gemini_received = _first_event(evs, ["TRANSLATED_AUDIO_RECEIVED", "GEMINI_WS_FRAME_RECEIVED"])
        if audio_sent_to_gemini and gemini_received:
            d = _delta_ms(audio_sent_to_gemini, gemini_received)
            if d is not None:
                gemini_latencies.append(d)

    metrics["gemini_processing_ms"] = float(statistics.median(gemini_latencies)) if gemini_latencies else None

    # PCM decode time: pair sequential START/END events
    pcm_latencies: List[float] = []
    last_pcm_start = None
    for ev in evs:
        if ev.get("event") == "PCM_DECODE_STARTED":
            last_pcm_start = ev
        elif ev.get("event") == "PCM_DECODE_COMPLETED" and last_pcm_start is not None:
            d = _delta_ms(last_pcm_start, ev)
            if d is not None:
                pcm_latencies.append(d)
            last_pcm_start = None

    metrics["pcm_decode_ms"] = float(statistics.median(pcm_latencies)) if pcm_latencies else None

    # Playback scheduling delay: pair AUDIO_PACKET_RECEIVED -> AUDIO_PLAYBACK_SCHEDULED using packet_id or chunk_index
    playback_latencies: List[float] = []
    if packet_ids:
        for pid in packet_ids:
            recv = next((e for e in evs if e.get("event") == "AUDIO_PACKET_RECEIVED" and str((e.get("metadata") or {}).get("packet_id")) == pid), None)
            sched = next((e for e in evs if e.get("event") == "AUDIO_PLAYBACK_SCHEDULED" and str((e.get("metadata") or {}).get("packet_id")) == pid), None)
            if recv and sched:
                d = _delta_ms(recv, sched)
                if d is not None:
                    playback_latencies.append(d)
    # fallback to chunk pairing
    if not playback_latencies and chunk_indices:
        for idx in chunk_indices:
            recv = next((e for e in evs if e.get("event") == "AUDIO_PACKET_RECEIVED" and (e.get("metadata") or {}).get("chunk_index") == idx), None)
            sched = next((e for e in evs if e.get("event") == "AUDIO_PLAYBACK_SCHEDULED" and (e.get("metadata") or {}).get("chunk_index") == idx), None)
            if recv and sched:
                d = _delta_ms(recv, sched)
                if d is not None:
                    playback_latencies.append(d)

    # final fallback: earliest audio frontend -> earliest playback scheduled
    if not playback_latencies:
        audio_playback_scheduled = _first_event(evs, ["AUDIO_PLAYBACK_SCHEDULED"])
        if first_audio_frontend and audio_playback_scheduled:
            d = _delta_ms(first_audio_frontend, audio_playback_scheduled)
            if d is not None:
                playback_latencies.append(d)

    metrics["playback_scheduling_delay_ms"] = float(statistics.median(playback_latencies)) if playback_latencies else None

    # Correlation completion time (first mic -> last completion marker)
    completion_marker = end_marker
    if first_mic and completion_marker:
        metrics["correlation_completion_ms"] = _delta_ms(first_mic, completion_marker)
    else:
        metrics["correlation_completion_ms"] = None

    # Network latency candidate: per-packet AUDIO_PUBLISHED -> AUDIO_PACKET_RECEIVED (median)
    network_latencies: List[float] = []
    if packet_ids:
        for pid in packet_ids:
            pub = next((e for e in evs if e.get("event") == "AUDIO_PUBLISHED" and str((e.get("metadata") or {}).get("packet_id")) == pid), None)
            rec = next((e for e in evs if e.get("event") == "AUDIO_PACKET_RECEIVED" and str((e.get("metadata") or {}).get("packet_id")) == pid), None)
            if pub and rec:
                d = _delta_ms(pub, rec)
                if d is not None:
                    network_latencies.append(d)
    if network_latencies:
        metrics["network_publish_to_receive_ms"] = float(statistics.median(network_latencies))
    else:
        # fallback to coarse estimate if direct pairings unavailable
        if audio_published and first_audio_frontend:
            metrics["network_publish_to_receive_ms"] = _delta_ms(audio_published, first_audio_frontend)
        else:
            metrics["network_publish_to_receive_ms"] = None

    return metrics


def compute_session_metrics(trace: Dict[str, Any]) -> Dict[str, Any]:
    events: List[Dict[str, Any]] = trace.get("events", [])
    try:
        session_id = trace.get("session", {}).get("session_id")
        logger.info(f"[MetricsEngine] compute_session_metrics started for session_id={session_id}, total_events={len(events)} — module_file={__file__}")
    except Exception:
        logger.info("[MetricsEngine] compute_session_metrics started")
    logger.info("[MetricsEngine] METRICS_GENERATION_STARTED")
    sorted_events = _sort_events(events)

    # Group by non-empty correlation_id
    by_corr: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for ev in sorted_events:
        corr = ev.get("correlation_id") or ""
        if corr:
            by_corr[corr].append(ev)

    per_corr_results: Dict[str, Any] = {}
    collected_metrics: Dict[str, List[float]] = defaultdict(list)

    for corr_id, evs in by_corr.items():
        metrics = compute_correlation_metrics(evs)
        per_corr_results[corr_id] = {
            "metrics": metrics,
            "event_count": len(evs),
        }
        # Collect stats for summary (only non-None values)
        for k, v in metrics.items():
            if v is not None:
                collected_metrics[k].append(v)

    # Session summary
    total_correlations = len(by_corr)
    # Completed = those with correlation_completion_ms present
    completed_correlations = sum(1 for v in per_corr_results.values() if v["metrics"].get("correlation_completion_ms") is not None)
    incomplete_correlations = total_correlations - completed_correlations

    def _stats(values: List[float]) -> Optional[Dict[str, float]]:
        if not values:
            return None
        vals = sorted(values)
        return {
            "count": len(vals),
            "avg_ms": float(statistics.mean(vals)),
            "median_ms": float(statistics.median(vals)),
            "min_ms": float(min(vals)),
            "max_ms": float(max(vals)),
            "p95_ms": float(_percentile(vals, 95)),
        }

    summary_stats = {k: _stats(v) for k, v in collected_metrics.items()}

    # Augment each metric summary with coverage information
    for k, stats in summary_stats.items():
        if stats is None:
            continue
        stats["total_correlations"] = total_correlations
        stats["coverage_pct"] = float((stats.get("count", 0) / total_correlations * 100.0) if total_correlations else 0.0)

    session_metrics = {
        "session_id": trace.get("session", {}).get("session_id"),
        "generated_at_epoch_ms": int(time.time() * 1000),
        "metrics_engine_version": METRICS_ENGINE_VERSION,
        "metrics_schema_version": METRICS_SCHEMA_VERSION,
        "total_correlations": total_correlations,
        "completed_correlations": completed_correlations,
        "incomplete_correlations": incomplete_correlations,
        "per_correlation": per_corr_results,
        "metrics_summary": summary_stats,
    }

    try:
        logger.info(f"[MetricsEngine] compute_session_metrics completed: total_correlations={total_correlations}, completed_correlations={completed_correlations}")
    except Exception:
        logger.info("[MetricsEngine] compute_session_metrics completed")
    logger.info("[MetricsEngine] METRICS_GENERATION_COMPLETED")

    return session_metrics


def _percentile(sorted_vals: List[float], pct: float) -> float:
    """Compute percentile (0-100) from pre-sorted list of values."""
    if not sorted_vals:
        return 0.0
    rank = pct / 100.0 * (len(sorted_vals) - 1)
    lo = int(math.floor(rank))
    hi = int(math.ceil(rank))
    if lo == hi:
        return float(sorted_vals[int(rank)])
    weight = rank - lo
    return float(sorted_vals[lo] * (1 - weight) + sorted_vals[hi] * weight)


def export_metrics(metrics: Dict[str, Any], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)


def main() -> int:
    parser = argparse.ArgumentParser(description="Metrics Engine — compute metrics.json from session_trace.json")
    parser.add_argument("--trace", type=str, help="Path to session_trace.json (file)")
    parser.add_argument("--session-dir", type=str, help="Path to session directory containing session_trace.json")
    parser.add_argument("--out", type=str, help="Output path for metrics.json (optional)")

    args = parser.parse_args()

    trace_path: Optional[Path] = None
    if args.trace:
        trace_path = Path(args.trace)
    elif args.session_dir:
        trace_path = Path(args.session_dir) / "session_trace.json"
    else:
        print("Either --trace or --session-dir must be provided")
        return 2

    if not trace_path.exists():
        print(f"Trace file not found: {trace_path}")
        return 2

    trace = load_trace(trace_path)
    metrics = compute_session_metrics(trace)

    out_path = Path(args.out) if args.out else trace_path.parent / "metrics.json"
    export_metrics(metrics, out_path)
    print(f"Metrics written → {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
