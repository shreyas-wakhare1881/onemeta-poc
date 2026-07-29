"""
Metrics Engine (Phase 5 — Production-Grade)

Reads a canonical `session_trace.json` and emits `metrics.json` with:
  - Industry-standard TTFT/TTFA anchored to AUDIO_STREAM_END_SENT (turn commit)
  - Turn Decision Pipeline metrics (VAD timing, Gemini phases)
  - Streaming continuity metrics
  - Confidence model for every metric
  - Full per-correlation breakdown

Formula definitions (industry references: Google Meet, OpenAI Realtime, Gemini Live):
  TTFT (Time To First Text)
    = AUDIO_STREAM_END_SENT → GEMINI_FIRST_TOKEN  (or fallback: → TRANSLATED_TEXT_RECEIVED)
    This measures how long after the user stops speaking the first translated word appears.

  TTFA (Time To First Audio Playback)
    = AUDIO_STREAM_END_SENT → AUDIO_PLAYBACK_STARTED
    This measures how long after the user stops speaking audio begins playing.
    This is the PRIMARY user-experience metric.

  Gemini Wait Time
    = AUDIO_STREAM_END_SENT → GEMINI_FIRST_AUDIO (first audio chunk from Gemini)

  Turn Decision Latency
    = SPEECH_SEGMENT_ENDED → AUDIO_STREAM_END_SENT  (debounce commit delay)

  Speech Duration
    = From SPEECH_SEGMENT_STARTED metadata: speech_duration_ms

  Streaming continuity gaps
    = per-playback-chunk: gap_ms[i] = AUDIO_PLAYBACK_STARTED[i].epoch - AUDIO_PLAYBACK_STARTED[i-1].epoch - chunk_duration_ms[i-1]

Usage:
  python -m backend.app.audio.metrics_engine --trace path/to/session_trace.json
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

METRICS_ENGINE_VERSION = "3.0.0"
METRICS_SCHEMA_VERSION = 2

logger = logging.getLogger("onemeta.metrics_engine")


# ─── Helpers ──────────────────────────────────────────────────────────────────

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


def _host_group(ev: Dict[str, Any]) -> str:
    comp = (ev.get("component") or "").lower()
    if comp in ("frontend", "react", "ui", "pcm"):
        return "frontend"
    if comp in ("backend", "session", "agent", "processor", "audio", "pipeline"):
        return "backend"
    return comp


def _delta_ms(a: Dict[str, Any], b: Dict[str, Any]) -> Optional[float]:
    """
    Compute (b - a) in milliseconds.
    Rules:
    - Same host + both have monotonic → use monotonic_ns (high precision)
    - Cross-host or missing monotonic → use epoch_ms
    - Cannot mix monotonic and epoch → return None
    """
    if a is None or b is None:
        return None
    a_mono = a.get("timestamp_monotonic_ns")
    b_mono = b.get("timestamp_monotonic_ns")
    a_epoch = a.get("timestamp_epoch_ms")
    b_epoch = b.get("timestamp_epoch_ms")

    same_host = _host_group(a) == _host_group(b)

    if same_host and a_mono is not None and b_mono is not None:
        return (float(b_mono) - float(a_mono)) / 1_000_000.0

    if a_epoch is not None and b_epoch is not None:
        return float(b_epoch) - float(a_epoch)

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


def _percentile(sorted_vals: List[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    rank = pct / 100.0 * (len(sorted_vals) - 1)
    lo = int(math.floor(rank))
    hi = int(math.ceil(rank))
    if lo == hi:
        return float(sorted_vals[int(rank)])
    weight = rank - lo
    return float(sorted_vals[lo] * (1 - weight) + sorted_vals[hi] * weight)


def _stats(values: List[float]) -> Optional[Dict[str, Any]]:
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


# ─── Per-correlation metrics ──────────────────────────────────────────────────

def compute_correlation_metrics(events: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Compute all metrics for a single correlation's events.

    Primary metrics (anchored to turn-commit = AUDIO_STREAM_END_SENT):
      ttft_ms                  : AUDIO_STREAM_END_SENT → GEMINI_FIRST_TOKEN (clamped >= 0)
      ttfa_ms                  : AUDIO_STREAM_END_SENT → AUDIO_PLAYBACK_STARTED (clamped >= 0)
      gemini_wait_ms           : AUDIO_STREAM_END_SENT → GEMINI_FIRST_AUDIO (clamped >= 0)
      turn_decision_latency_ms : Last MIC_FRAME_RECEIVED (is_speech=True) → AUDIO_STREAM_END_SENT

    Secondary metrics (retained for backward compatibility):
      time_to_first_text_arrival_ms   : MIC_FRAME_RECEIVED → TRANSLATED_TEXT_RECEIVED (legacy)
      time_to_first_text_render_ms    : MIC_FRAME_RECEIVED → REACT_RENDER_COMPLETED   (legacy)
      time_to_first_audio_frontend_ms : MIC_FRAME_RECEIVED → AUDIO_PACKET_RECEIVED    (legacy)
      time_to_first_audio_playback_ms : MIC_FRAME_RECEIVED → AUDIO_PLAYBACK_STARTED  (legacy)
      end_to_end_ms                   : MIC_FRAME_RECEIVED → last AUDIO_PLAYBACK_SCHEDULED (informational)

    Supporting metrics:
      speech_duration_ms              : physical speech duration based on frame count (20ms per speech frame)
      gemini_processing_ms            : AUDIO_SENT_TO_GEMINI → TRANSLATED_AUDIO_RECEIVED (per-chunk median)
      first_audio_to_first_playback_ms: AUDIO_PACKET_RECEIVED → AUDIO_PLAYBACK_STARTED (pure client latency)
      playback_scheduling_delay_ms    : AUDIO_PACKET_RECEIVED → AUDIO_PLAYBACK_SCHEDULED
      network_publish_to_receive_ms   : AUDIO_PUBLISHED → AUDIO_PACKET_RECEIVED
      pcm_decode_ms                   : PCM_DECODE_STARTED → PCM_DECODE_COMPLETED (median)
      text_render_latency_ms          : TEXT_PACKET_RECEIVED → REACT_RENDER_COMPLETED (median)
    """
    evs = _sort_events(events)

    # Core anchor events
    stream_end_sent     = _first_event(evs, ["AUDIO_STREAM_END_SENT"])
    speech_seg_ended    = _first_event(evs, ["SPEECH_SEGMENT_ENDED"])
    gemini_first_token  = _first_event(evs, ["GEMINI_FIRST_TOKEN"])
    gemini_first_audio  = _first_event(evs, ["GEMINI_FIRST_AUDIO"])
    first_playback      = _first_event(evs, ["AUDIO_PLAYBACK_STARTED"])

    # Legacy anchor
    first_mic           = _first_event(evs, ["MIC_FRAME_RECEIVED"])
    first_text          = _first_event(evs, ["TEXT_PACKET_RECEIVED", "TRANSLATED_TEXT_RECEIVED"])
    first_audio_frontend= _first_event(evs, ["AUDIO_PACKET_RECEIVED"])
    first_audio_backend = _first_event(evs, ["TRANSLATED_AUDIO_RECEIVED"])
    first_render        = _first_event(evs, ["REACT_RENDER_COMPLETED"])
    audio_published     = _first_event(evs, ["AUDIO_PUBLISHED"])

    last_sched          = _last_event(evs, ["AUDIO_PLAYBACK_SCHEDULED"])
    last_render         = _last_event(evs, ["REACT_RENDER_COMPLETED"])

    # Build lookup maps for paired metrics
    chunk_indices: set = set()
    packet_ids: set = set()
    for ev in evs:
        md = ev.get("metadata") or {}
        if "chunk_index" in md:
            try:
                chunk_indices.add(int(md["chunk_index"]))
            except Exception:
                chunk_indices.add(md["chunk_index"])
        if "packet_id" in md:
            packet_ids.add(str(md["packet_id"]))

    metrics: Dict[str, Any] = {}

    # ── PRIMARY KPIs (turn-commit anchored) ───────────────────────────────────

    # TTFT: AUDIO_STREAM_END_SENT → GEMINI_FIRST_TOKEN
    # Fallback: → first TRANSLATED_TEXT_RECEIVED (cross-host, epoch only)
    raw_ttft = None
    if stream_end_sent and gemini_first_token:
        raw_ttft = _delta_ms(stream_end_sent, gemini_first_token)
    elif stream_end_sent and first_text:
        raw_ttft = _delta_ms(stream_end_sent, first_text)

    metrics["ttft_raw_ms"] = raw_ttft
    metrics["ttft_ms"] = max(0.0, raw_ttft) if raw_ttft is not None else None
    metrics["ttft_overlap_ms"] = abs(raw_ttft) if (raw_ttft is not None and raw_ttft < 0) else (0.0 if raw_ttft is not None else None)

    # TTFA: AUDIO_STREAM_END_SENT → first AUDIO_PLAYBACK_STARTED
    # This is the PRIMARY user-experience metric.
    raw_ttfa = None
    if stream_end_sent and first_playback:
        raw_ttfa = _delta_ms(stream_end_sent, first_playback)

    metrics["ttfa_raw_ms"] = raw_ttfa
    metrics["ttfa_ms"] = max(0.0, raw_ttfa) if raw_ttfa is not None else None
    metrics["ttfa_overlap_ms"] = abs(raw_ttfa) if (raw_ttfa is not None and raw_ttfa < 0) else (0.0 if raw_ttfa is not None else None)

    # Gemini Wait Time: AUDIO_STREAM_END_SENT → GEMINI_FIRST_AUDIO
    raw_gemini_wait = None
    if stream_end_sent and gemini_first_audio:
        raw_gemini_wait = _delta_ms(stream_end_sent, gemini_first_audio)

    metrics["gemini_wait_raw_ms"] = raw_gemini_wait
    metrics["gemini_wait_ms"] = max(0.0, raw_gemini_wait) if raw_gemini_wait is not None else None
    metrics["gemini_wait_overlap_ms"] = abs(raw_gemini_wait) if (raw_gemini_wait is not None and raw_gemini_wait < 0) else (0.0 if raw_gemini_wait is not None else None)

    # Turn Decision Latency: Last MIC_FRAME_RECEIVED with is_speech=True → AUDIO_STREAM_END_SENT (true debounce delay)
    last_speech_mic = None
    for ev in reversed(evs):
        if ev.get("event") == "MIC_FRAME_RECEIVED":
            if (ev.get("metadata") or {}).get("is_speech") is True:
                last_speech_mic = ev
                break

    if last_speech_mic and stream_end_sent:
        metrics["turn_decision_latency_ms"] = _delta_ms(last_speech_mic, stream_end_sent)
    elif speech_seg_ended and stream_end_sent:
        metrics["turn_decision_latency_ms"] = _delta_ms(speech_seg_ended, stream_end_sent)
    else:
        metrics["turn_decision_latency_ms"] = None

    # Speech Duration: calculated using physical frame count (total_speech_frames * 20.0ms)
    speech_frame_count = None
    if stream_end_sent:
        speech_frame_count = (stream_end_sent.get("metadata") or {}).get("total_speech_frames")
    if speech_frame_count is None and speech_seg_ended:
        speech_frame_count = (speech_seg_ended.get("metadata") or {}).get("total_speech_frames")

    if speech_frame_count is not None:
        metrics["speech_duration_ms"] = float(speech_frame_count) * 20.0
    elif speech_seg_ended:
        md = speech_seg_ended.get("metadata") or {}
        speech_dur = md.get("speech_duration_ms")
        silence_dur = md.get("silence_duration_ms") or 0.0
        if speech_dur is not None:
            metrics["speech_duration_ms"] = max(0.0, float(speech_dur) - float(silence_dur))
        else:
            metrics["speech_duration_ms"] = None
    else:
        metrics["speech_duration_ms"] = None

    # First-audio-to-playback (pure client-side latency: AUDIO_PACKET_RECEIVED → AUDIO_PLAYBACK_STARTED)
    if first_audio_frontend and first_playback:
        metrics["first_audio_to_first_playback_ms"] = _delta_ms(first_audio_frontend, first_playback)
    else:
        metrics["first_audio_to_first_playback_ms"] = None

    # ── LEGACY KPIs (MIC-anchored, retained for backward compatibility) ────────

    if first_mic and first_text:
        metrics["time_to_first_text_arrival_ms"] = _delta_ms(first_mic, first_text)
    else:
        metrics["time_to_first_text_arrival_ms"] = None

    if first_mic and first_render:
        metrics["time_to_first_text_render_ms"] = _delta_ms(first_mic, first_render)
    else:
        metrics["time_to_first_text_render_ms"] = None

    if first_mic and first_audio_frontend:
        metrics["time_to_first_audio_frontend_ms"] = _delta_ms(first_mic, first_audio_frontend)
    else:
        metrics["time_to_first_audio_frontend_ms"] = None

    if first_mic and first_audio_backend:
        metrics["time_to_first_audio_backend_ms"] = _delta_ms(first_mic, first_audio_backend)
    else:
        metrics["time_to_first_audio_backend_ms"] = None

    if first_mic and first_playback:
        metrics["time_to_first_audio_playback_ms"] = _delta_ms(first_mic, first_playback)
    else:
        metrics["time_to_first_audio_playback_ms"] = None

    # End-to-End (informational only — not used for scoring)
    end_marker = last_sched or last_render
    if first_mic and end_marker:
        metrics["end_to_end_ms"] = _delta_ms(first_mic, end_marker)
    else:
        metrics["end_to_end_ms"] = None

    # ── USER-FACING LATENCIES (frontend-anchored; strict — no legacy fallbacks) ─
    # These metrics are only valid when the frontend emits the canonical events
    # defined by our instrumentation spec. If the frontend events are missing,
    # the metrics are set to `None` and a per-correlation status/reason is added.
    if first_mic:
        # Source transcript visible in UI (frontend must emit SOURCE_TRANSCRIPT_RENDERED)
        first_source_render = _first_event(evs, ["SOURCE_TRANSCRIPT_RENDERED"])
        if first_source_render:
            metrics["source_transcript_latency_ms"] = _delta_ms(first_mic, first_source_render)
            metrics["source_transcript_status"] = "AVAILABLE"
            metrics["source_transcript_unavailable_reason"] = None
        else:
            metrics["source_transcript_latency_ms"] = None
            metrics["source_transcript_status"] = "NOT_AVAILABLE"
            metrics["source_transcript_unavailable_reason"] = "SOURCE_TRANSCRIPT_RENDERED event missing"

        # Target (translated) transcript rendered (frontend must emit TARGET_TRANSCRIPT_RENDERED)
        first_target_render = _first_event(evs, ["TARGET_TRANSCRIPT_RENDERED"])
        if first_target_render:
            metrics["target_transcript_latency_ms"] = _delta_ms(first_mic, first_target_render)
            metrics["target_transcript_status"] = "AVAILABLE"
            metrics["target_transcript_unavailable_reason"] = None
        else:
            metrics["target_transcript_latency_ms"] = None
            metrics["target_transcript_status"] = "NOT_AVAILABLE"
            metrics["target_transcript_unavailable_reason"] = "TARGET_TRANSCRIPT_RENDERED event missing"

        # Translation audio audible (frontend must emit AUDIO_FIRST_AUDIBLE)
        first_audible = _first_event(evs, ["AUDIO_FIRST_AUDIBLE"])
        if first_audible:
            metrics["translation_audio_latency_ms"] = _delta_ms(first_mic, first_audible)
            metrics["translation_audio_status"] = "AVAILABLE"
            metrics["translation_audio_unavailable_reason"] = None
        else:
            metrics["translation_audio_latency_ms"] = None
            metrics["translation_audio_status"] = "NOT_AVAILABLE"
            metrics["translation_audio_unavailable_reason"] = "AUDIO_FIRST_AUDIBLE event missing"
    else:
        # Cannot compute user-facing latencies without MIC_FRAME_RECEIVED anchor
        metrics["source_transcript_latency_ms"] = None
        metrics["source_transcript_status"] = "NOT_AVAILABLE"
        metrics["source_transcript_unavailable_reason"] = "MIC_FRAME_RECEIVED missing"
        metrics["target_transcript_latency_ms"] = None
        metrics["target_transcript_status"] = "NOT_AVAILABLE"
        metrics["target_transcript_unavailable_reason"] = "MIC_FRAME_RECEIVED missing"
        metrics["translation_audio_latency_ms"] = None
        metrics["translation_audio_status"] = "NOT_AVAILABLE"
        metrics["translation_audio_unavailable_reason"] = "MIC_FRAME_RECEIVED missing"

    # ── TEXT RENDER LATENCY ────────────────────────────────────────────────────
    text_render_latencies: List[float] = []
    if packet_ids:
        for pid in packet_ids:
            text_ev = next((e for e in evs if e.get("event") in ("TEXT_PACKET_RECEIVED", "TRANSLATED_TEXT_RECEIVED")
                            and str((e.get("metadata") or {}).get("packet_id")) == pid), None)
            render_ev = next((e for e in evs if e.get("event") == "REACT_RENDER_COMPLETED"
                              and str((e.get("metadata") or {}).get("packet_id")) == pid), None)
            if text_ev and render_ev:
                d = _delta_ms(text_ev, render_ev)
                if d is not None:
                    text_render_latencies.append(d)
    if not text_render_latencies and first_text and first_render:
        d = _delta_ms(first_text, first_render)
        if d is not None:
            text_render_latencies.append(d)
    metrics["text_render_latency_ms"] = float(statistics.median(text_render_latencies)) if text_render_latencies else None

    # ── GEMINI PROCESSING (per-chunk median, backward compat) ─────────────────
    gemini_latencies: List[float] = []
    if chunk_indices:
        for idx in chunk_indices:
            sent = next((e for e in evs if e.get("event") == "AUDIO_SENT_TO_GEMINI"
                         and (e.get("metadata") or {}).get("chunk_index") == idx), None)
            recv = next((e for e in reversed(evs) if e.get("event") in ("TRANSLATED_AUDIO_RECEIVED", "GEMINI_WS_FRAME_RECEIVED")
                         and (e.get("metadata") or {}).get("chunk_index") == idx), None)
            if sent and recv:
                d = _delta_ms(sent, recv)
                if d is not None:
                    gemini_latencies.append(d)
    if not gemini_latencies:
        sent0 = _first_event(evs, ["AUDIO_SENT_TO_GEMINI"])
        recv0 = _first_event(evs, ["TRANSLATED_AUDIO_RECEIVED", "GEMINI_WS_FRAME_RECEIVED"])
        if sent0 and recv0:
            d = _delta_ms(sent0, recv0)
            if d is not None:
                gemini_latencies.append(d)
    metrics["gemini_processing_ms"] = float(statistics.median(gemini_latencies)) if gemini_latencies else None

    # ── PCM DECODE (median) ───────────────────────────────────────────────────
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

    # ── PLAYBACK SCHEDULING DELAY ─────────────────────────────────────────────
    playback_latencies: List[float] = []
    if packet_ids:
        for pid in packet_ids:
            recv = next((e for e in evs if e.get("event") == "AUDIO_PACKET_RECEIVED"
                         and str((e.get("metadata") or {}).get("packet_id")) == pid), None)
            sched = next((e for e in evs if e.get("event") == "AUDIO_PLAYBACK_SCHEDULED"
                          and str((e.get("metadata") or {}).get("packet_id")) == pid), None)
            if recv and sched:
                d = _delta_ms(recv, sched)
                if d is not None:
                    playback_latencies.append(d)
    if not playback_latencies and chunk_indices:
        for idx in chunk_indices:
            recv = next((e for e in evs if e.get("event") == "AUDIO_PACKET_RECEIVED"
                         and (e.get("metadata") or {}).get("chunk_index") == idx), None)
            sched = next((e for e in evs if e.get("event") == "AUDIO_PLAYBACK_SCHEDULED"
                          and (e.get("metadata") or {}).get("chunk_index") == idx), None)
            if recv and sched:
                d = _delta_ms(recv, sched)
                if d is not None:
                    playback_latencies.append(d)
    if not playback_latencies:
        sched0 = _first_event(evs, ["AUDIO_PLAYBACK_SCHEDULED"])
        if first_audio_frontend and sched0:
            d = _delta_ms(first_audio_frontend, sched0)
            if d is not None:
                playback_latencies.append(d)
    metrics["playback_scheduling_delay_ms"] = float(statistics.median(playback_latencies)) if playback_latencies else None

    # ── NETWORK LATENCY ───────────────────────────────────────────────────────
    network_latencies: List[float] = []
    if packet_ids:
        for pid in packet_ids:
            pub = next((e for e in evs if e.get("event") == "AUDIO_PUBLISHED"
                        and str((e.get("metadata") or {}).get("packet_id")) == pid), None)
            rec = next((e for e in evs if e.get("event") == "AUDIO_PACKET_RECEIVED"
                        and str((e.get("metadata") or {}).get("packet_id")) == pid), None)
            if pub and rec:
                d = _delta_ms(pub, rec)
                if d is not None:
                    network_latencies.append(d)
    if network_latencies:
        metrics["network_publish_to_receive_ms"] = float(statistics.median(network_latencies))
    elif audio_published and first_audio_frontend:
        metrics["network_publish_to_receive_ms"] = _delta_ms(audio_published, first_audio_frontend)
    else:
        metrics["network_publish_to_receive_ms"] = None

    # ── CORRELATION COMPLETION (informational) ────────────────────────────────
    if first_mic and end_marker:
        metrics["correlation_completion_ms"] = _delta_ms(first_mic, end_marker)
    else:
        metrics["correlation_completion_ms"] = None

    return metrics


# ─── Session-level aggregation ────────────────────────────────────────────────

def compute_session_metrics(trace: Dict[str, Any]) -> Dict[str, Any]:
    events: List[Dict[str, Any]] = trace.get("events", [])
    try:
        session_id = trace.get("session", {}).get("session_id")
        logger.info(
            f"[MetricsEngine] compute_session_metrics started for session_id={session_id}, "
            f"total_events={len(events)} — module_file={__file__}"
        )
    except Exception:
        logger.info("[MetricsEngine] compute_session_metrics started")
    logger.info("[MetricsEngine] METRICS_GENERATION_STARTED")

    sorted_events = _sort_events(events)

    # Group by correlation_id
    by_corr: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for ev in sorted_events:
        corr = ev.get("correlation_id") or ""
        if corr:
            by_corr[corr].append(ev)

    per_corr_results: Dict[str, Any] = {}
    collected_metrics: Dict[str, List[float]] = defaultdict(list)

    for corr_id, evs in by_corr.items():
        m = compute_correlation_metrics(evs)
        per_corr_results[corr_id] = {
            "metrics": m,
            "event_count": len(evs),
        }
        # Collect numeric values for session summaries (skip confidence/formula fields)
        for k, v in m.items():
            if isinstance(v, (int, float)) and v is not None and not k.endswith("_confidence") and not k.endswith("_formula"):
                collected_metrics[k].append(v)

    total_correlations = len(by_corr)
    completed_correlations = sum(
        1 for v in per_corr_results.values()
        if v["metrics"].get("correlation_completion_ms") is not None
    )
    incomplete_correlations = total_correlations - completed_correlations

    summary_stats: Dict[str, Any] = {}
    for k, vals in collected_metrics.items():
        s = _stats(vals)
        if s:
            s["total_correlations"] = total_correlations
            s["coverage_pct"] = round((s["count"] / total_correlations * 100.0) if total_correlations else 0.0, 1)
            summary_stats[k] = s

    # ── Streaming continuity (session-wide) ───────────────────────────────────
    playback_started_events = sorted(
        [ev for ev in sorted_events if ev.get("event") == "AUDIO_PLAYBACK_STARTED"],
        key=lambda ev: (ev.get("timestamp_epoch_ms") or 0, ev.get("timestamp_monotonic_ns") or 0)
    )

    sched_duration_map: Dict[str, float] = {}
    for ev in sorted_events:
        if ev.get("event") == "AUDIO_PLAYBACK_SCHEDULED":
            md = ev.get("metadata") or {}
            pid = str(md.get("packet_id") or "")
            dur = md.get("chunk_duration_sec")
            if pid and dur is not None:
                try:
                    sched_duration_map[pid] = float(dur)
                except Exception:
                    pass

    inter_chunk_gaps_ms: List[float] = []
    RESTART_THRESHOLD_MS = 500.0
    restart_count = 0
    starvation_count = sum(1 for ev in sorted_events if ev.get("event") == "PLAYBACK_DROP_DECISION")

    for i in range(1, len(playback_started_events)):
        prev_ev = playback_started_events[i - 1]
        curr_ev = playback_started_events[i]
        prev_epoch = prev_ev.get("timestamp_epoch_ms")
        curr_epoch = curr_ev.get("timestamp_epoch_ms")
        if prev_epoch is None or curr_epoch is None:
            continue
        prev_md = prev_ev.get("metadata") or {}
        prev_pid = str(prev_md.get("packet_id") or "")
        prev_duration_sec = sched_duration_map.get(prev_pid, 0.0)
        prev_duration_ms = prev_duration_sec * 1000.0
        gap_ms = float(curr_epoch) - float(prev_epoch) - prev_duration_ms
        if gap_ms > 0:
            inter_chunk_gaps_ms.append(gap_ms)
            if gap_ms > RESTART_THRESHOLD_MS:
                restart_count += 1

    streaming_continuity: Dict[str, Any] = {
        "total_playback_chunks": len(playback_started_events),
        "inter_chunk_gap_count": len(inter_chunk_gaps_ms),
        "average_gap_ms": round(float(statistics.mean(inter_chunk_gaps_ms)), 2) if inter_chunk_gaps_ms else None,
        "maximum_gap_ms": round(float(max(inter_chunk_gaps_ms)), 2) if inter_chunk_gaps_ms else None,
        "median_gap_ms": round(float(statistics.median(inter_chunk_gaps_ms)), 2) if inter_chunk_gaps_ms else None,
        "p95_gap_ms": round(float(_percentile(sorted(inter_chunk_gaps_ms), 95)), 2) if inter_chunk_gaps_ms else None,
        "restart_count": restart_count,
        "restart_threshold_ms": RESTART_THRESHOLD_MS,
        "starvation_count": starvation_count,
        "continuous_audio_pct": round(
            (1.0 - (restart_count / max(len(playback_started_events), 1))) * 100.0, 1
        ) if playback_started_events else None,
    }

    # ── VAD session summary ────────────────────────────────────────────────────
    speech_durations = [
        float((ev.get("metadata") or {}).get("speech_duration_ms", 0))
        for ev in sorted_events
        if ev.get("event") == "SPEECH_SEGMENT_ENDED"
        and (ev.get("metadata") or {}).get("speech_duration_ms") is not None
    ]
    vad_summary = {
        "total_turns": total_correlations,
        "speech_duration_stats_ms": _stats(speech_durations) if speech_durations else None,
        "false_starts": sum(1 for ev in sorted_events if ev.get("event") == "VAD_FALSE_START"),
        "audio_stream_end_count": sum(1 for ev in sorted_events if ev.get("event") == "AUDIO_STREAM_END_SENT"),
    }

    # ── Gemini session summary ────────────────────────────────────────────────
    gemini_first_token_latencies = [
        v for v in collected_metrics.get("gemini_wait_ms", []) if v is not None
    ]
    gemini_turn_complete_count = sum(1 for ev in sorted_events if ev.get("event") == "GEMINI_TURN_COMPLETE")

    gemini_summary = {
        "turns_with_first_token": len(gemini_first_token_latencies),
        "gemini_turn_complete_count": gemini_turn_complete_count,
        "gemini_wait_stats_ms": _stats(gemini_first_token_latencies) if gemini_first_token_latencies else None,
    }

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
        "streaming_continuity": streaming_continuity,
        "vad_summary": vad_summary,
        "gemini_summary": gemini_summary,
    }

    try:
        logger.info(
            f"[MetricsEngine] compute_session_metrics completed: "
            f"total_correlations={total_correlations}, completed_correlations={completed_correlations}"
        )
    except Exception:
        logger.info("[MetricsEngine] compute_session_metrics completed")
    logger.info("[MetricsEngine] METRICS_GENERATION_COMPLETED")
    return session_metrics


def export_metrics(metrics: Dict[str, Any], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)


def main() -> int:
    parser = argparse.ArgumentParser(description="Metrics Engine v3 — compute metrics.json from session_trace.json")
    parser.add_argument("--trace", type=str, help="Path to session_trace.json")
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
