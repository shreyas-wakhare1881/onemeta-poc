"""Benchmark Engine (Objective engineering dashboard)

This module reads the authoritative `metrics.json` and the `trace_validation.json`
produced by the existing pipeline and emits a deterministic, objective
`benchmark.json` that is a machine-friendly engineering dashboard.

Constraints honored:
- Uses only values already computed and present in `metrics.json` and
  `trace_validation.json`.
- No subjective grades or invented statistics.
- No changes to other pipeline components.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

BENCHMARK_ENGINE_VERSION = "6.1.0"
BENCHMARK_SCHEMA_VERSION = 1

logger = logging.getLogger("onemeta.benchmark_engine")


def _get_stat(summary: Dict[str, Any], key: str) -> Dict[str, Optional[float]]:
    """Extract a normalized stat block from metrics_summary for `key`.

    Returns an object with the fields the engineering dashboard expects.
    If a particular value isn't present in the source summary, it is left
    as `None`.
    """
    if not summary or key not in summary:
        return {
            "min": None,
            "max": None,
            "average": None,
            "median": None,
            "p90": None,
            "p95": None,
            "p99": None,
            "sample_count": None,
            "coverage_pct": None,
        }

    s = summary.get(key, {})
    def _get(k: str):
        v = s.get(k)
        return float(v) if v is not None else None

    return {
        "min": _get("min_ms"),
        "max": _get("max_ms"),
        "average": _get("avg_ms"),
        "median": _get("median_ms"),
        "p90": _get("p90_ms"),
        "p95": _get("p95_ms"),
        "p99": _get("p99_ms"),
        "sample_count": int(s.get("count")) if s.get("count") is not None else None,
        "coverage_pct": float(s.get("coverage_pct")) if s.get("coverage_pct") is not None else None,
    }


def _choose_key(summary: Dict[str, Any], candidates: List[str]) -> Optional[str]:
    for k in candidates:
        if k in summary:
            return k
    return None


def _iso_from_epoch_ms(epoch_ms: Optional[int]) -> Optional[str]:
    if epoch_ms is None:
        return None
    try:
        return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


def generate_benchmark(session_dir: Path) -> Dict[str, Any]:
    """Read `metrics.json` and `trace_validation.json` and write `benchmark.json`.

    This function is the single canonical benchmark generator used by the
    pipeline.
    """
    metrics_path = session_dir / "metrics.json"
    validation_path = session_dir / "trace_validation.json"
    out_path = session_dir / "benchmark.json"

    if not metrics_path.exists():
        logger.error("metrics.json not found: %s", metrics_path)
        return {}

    with metrics_path.open("r", encoding="utf-8") as f:
        metrics = json.load(f)

    summary = metrics.get("metrics_summary", {})
    continuity = metrics.get("streaming_continuity", {})

    total_corr = int(metrics.get("total_correlations") or 0)
    completed_corr = int(metrics.get("completed_correlations") or 0)
    incomplete_corr = int(metrics.get("incomplete_correlations") or 0)
    completion_pct = round((completed_corr / total_corr * 100.0), 2) if total_corr > 0 else None

    # Read validation file if present and surface values directly
    val = {}
    if validation_path.exists():
        with validation_path.open("r", encoding="utf-8") as vf:
            try:
                val = json.load(vf)
            except Exception:
                val = {}

    # Measured benchmarks: canonical, rich statistics
    measured = {
        "session_health": {
            "total_correlations": total_corr,
            "completed_correlations": completed_corr,
            "incomplete_correlations": incomplete_corr,
            "success_rate_percent": completion_pct,
        },
        "end_to_end_latency_ms": _get_stat(summary, "end_to_end_ms"),
        "gemini_processing_ms": _get_stat(summary, "gemini_processing_ms"),
        "first_response": {
            "text_latency_ms": _get_stat(summary, _choose_key(summary, ["ttft_ms", "time_to_first_text_render_ms", "time_to_first_text_arrival_ms"]) or "time_to_first_text_render_ms"),
            "audio_latency_ms": _get_stat(summary, _choose_key(summary, ["ttfa_ms", "time_to_first_audio_playback_ms", "time_to_first_audio_frontend_ms"]) or "time_to_first_audio_playback_ms"),
        },
        "playback": _get_stat(summary, _choose_key(summary, ["playback_scheduling_delay_ms", "first_audio_to_first_playback_ms"]) or "playback_scheduling_delay_ms"),
        "network": _get_stat(summary, "network_publish_to_receive_ms"),
        "pcm_decode": _get_stat(summary, "pcm_decode_ms"),
        "frontend_rendering": _get_stat(summary, "text_render_latency_ms"),
        "streaming": {
            "average_gap_ms": float(continuity.get("average_gap_ms")) if continuity.get("average_gap_ms") is not None else None,
            "median_gap_ms": float(continuity.get("median_gap_ms")) if continuity.get("median_gap_ms") is not None else None,
            "p95_gap_ms": float(continuity.get("p95_gap_ms")) if continuity.get("p95_gap_ms") is not None else None,
            "maximum_gap_ms": float(continuity.get("maximum_gap_ms")) if continuity.get("maximum_gap_ms") is not None else None,
            "restart_count": int(continuity.get("restart_count")) if continuity.get("restart_count") is not None else None,
            "starvation_count": int(continuity.get("starvation_count")) if continuity.get("starvation_count") is not None else None,
            "continuous_audio_pct": float(continuity.get("continuous_audio_pct")) if continuity.get("continuous_audio_pct") is not None else None,
            "total_playback_chunks": int(continuity.get("total_playback_chunks")) if continuity.get("total_playback_chunks") is not None else None,
            "restart_threshold_ms": float(continuity.get("restart_threshold_ms")) if continuity.get("restart_threshold_ms") is not None else None,
        },
        "validation": {
            "trace_valid": val.get("trace_valid"),
            "trace_quality": val.get("trace_quality") or val.get("validation_quality"),
            "ordering_errors": val.get("ordering_errors"),
            "duplicate_events": val.get("duplicate_events"),
            "missing_events": val.get("incomplete_correlations") or val.get("incomplete_correlations", 0),
            "clock_issues_count": None,
        }
    }

    # Count obvious clock-issue patterns in validator 'errors' array if present
    clock_count = 0
    for e in val.get("errors", []) or []:
        se = str(e).lower()
        if "monotonic timestamp decreased" in se or "monotonic jitter" in se:
            clock_count += 1
    if clock_count:
        measured["validation"]["clock_issues_count"] = clock_count

    # Bottlenecks and component-level stats
    components_to_rank = [
        ("Gemini", "gemini_processing_ms"),
        ("Network", "network_publish_to_receive_ms"),
        ("Playback", "playback_scheduling_delay_ms"),
        ("Frontend Rendering", "text_render_latency_ms"),
        ("PCM Decode", "pcm_decode_ms"),
    ]

    component_stats: Dict[str, Dict[str, Optional[float]]] = {}
    for name, key in components_to_rank:
        stat = _get_stat(summary, key)
        component_stats[name] = stat

    ranked = []
    for name in component_stats:
        avg = component_stats[name].get("average")
        ranked.append({
            "component": name,
            "average_ms": avg,
            "p95_ms": component_stats[name].get("p95"),
            "sample_count": component_stats[name].get("sample_count"),
        })

    ranked_sorted = [r for r in sorted(ranked, key=lambda x: (-(x["average_ms"] or -1e12)))]
    top_bottlenecks = []
    for i, r in enumerate(ranked_sorted, start=1):
        if r["average_ms"] is None:
            continue
        top_bottlenecks.append({
            "rank": i,
            "component": r["component"],
            "average_ms": float(r["average_ms"]),
            "p95_ms": r.get("p95_ms"),
            "sample_count": r.get("sample_count"),
        })

    # Percent contributions (share of measured latency across ranked components)
    sum_avgs = sum([b["average_ms"] for b in top_bottlenecks]) if top_bottlenecks else 0.0
    for b in top_bottlenecks:
        if sum_avgs > 0:
            b["percent_of_total_ms"] = round((b["average_ms"] / sum_avgs) * 100.0, 1)
        else:
            b["percent_of_total_ms"] = None

    # Deterministic optimization opportunities with simple expected gain estimate
    thresholds = {"Network": 100.0, "Gemini": 200.0, "Playback": 100.0, "Frontend Rendering": 50.0, "PCM Decode": 10.0}
    difficulty_map = {"Gemini": "MEDIUM", "Network": "MEDIUM", "Playback": "LOW", "Frontend Rendering": "LOW", "PCM Decode": "LOW"}

    optimization_opportunities: List[Dict[str, Any]] = []
    recommendations: List[str] = []
    for b in top_bottlenecks:
        comp = b["component"]
        avg = float(b["average_ms"]) if b.get("average_ms") is not None else None
        target = thresholds.get(comp)
        if avg is None:
            continue
        expected_user_gain_ms = round(avg - target, 2) if (target is not None and avg > target) else 0.0
        if expected_user_gain_ms >= 200:
            estimated_impact = "HIGH"
        elif expected_user_gain_ms >= 50:
            estimated_impact = "MEDIUM"
        elif expected_user_gain_ms > 0:
            estimated_impact = "LOW"
        else:
            estimated_impact = "NONE"

        difficulty = difficulty_map.get(comp, "UNKNOWN")

        # Reason: prefer describing whether it's sustained average or tail-latency
        reason = "sustained high average" if (target is not None and avg > target * 1.5) else ("tail latency (p95)" if (b.get("p95_ms") and target is not None and b.get("p95_ms") > target * 1.5) else "elevated average")

        optimization_opportunities.append({
            "component": comp,
            "priority": ("HIGH" if expected_user_gain_ms >= 200 else ("MEDIUM" if expected_user_gain_ms >= 50 else ("LOW" if expected_user_gain_ms > 0 else "NONE"))),
            "expected_user_gain_ms": expected_user_gain_ms,
            "estimated_impact": estimated_impact,
            "difficulty": difficulty,
            "reason": reason,
            "current_value_ms": avg,
            "target_value_ms": float(target) if target is not None else None,
            "percent_of_total_ms": b.get("percent_of_total_ms"),
        })

        if target is None:
            recommendations.append(f"{comp}: no target defined; measured avg {avg} ms")
        else:
            if avg > target:
                recommendations.append(f"{comp} latency exceeds target: avg {avg} ms > {target} ms")
            else:
                recommendations.append(f"{comp} latency within target: avg {avg} ms <= {target} ms")

    # Additional deterministic recommendations from validation and completion
    if measured["validation"].get("ordering_errors"):
        recommendations.append(f"Trace ordering issues detected: {measured['validation']['ordering_errors']}")
    if completion_pct is not None:
        if completion_pct < 90.0:
            recommendations.append(f"Completion rate below threshold: {completion_pct}% < 90%")
        else:
            recommendations.append(f"Completion rate OK: {completion_pct}% >= 90%")

    # Engineering reference blocks: description, formula, meaning, and measurement anchor
    # Choose the first candidate key that has a non-zero average if possible,
    # otherwise fall back to the first available candidate. This avoids
    # preserving a zero-valued anchor when a correct non-zero metric exists.
    def _ref_block(title: str, formula: str, meaning: str, key_candidates: List[str], overlap_key: Optional[str] = None):
        chosen_key = None
        chosen_stat = {"average": None}
        for k in key_candidates:
            if k in summary:
                s = _get_stat(summary, k)
                avg = s.get("average")
                if avg is not None and avg > 0:
                    chosen_key = k
                    chosen_stat = s
                    break
        if chosen_key is None:
            for k in key_candidates:
                if k in summary:
                    chosen_key = k
                    chosen_stat = _get_stat(summary, k)
                    break

        overlap = None
        if overlap_key and overlap_key in summary:
            overlap = float(summary.get(overlap_key, {}).get("avg_ms")) if summary.get(overlap_key) else None
        return {
            "description": title,
            "formula": formula,
            "engineering_meaning": meaning,
            "measurement_anchor": chosen_key,
            "measurement": chosen_stat,
            "turn_overlap_ms": overlap,
        }

    engineering_reference = {
        "TTFT": _ref_block("First text response (TTFT)", "MIC_FRAME_RECEIVED -> first TEXT_PACKET_RECEIVED or REACT_RENDER_COMPLETED", "Time until first translated text token is available to render.", ["ttft_ms", "time_to_first_text_render_ms", "time_to_first_text_arrival_ms"], "ttft_overlap_ms"),
        "TTFA": _ref_block("First audio response (TTFA)", "MIC_FRAME_RECEIVED -> first AUDIO_PLAYBACK_STARTED", "Time until first translated audio chunk is scheduled to play on client.", ["ttfa_ms", "time_to_first_audio_playback_ms", "time_to_first_audio_frontend_ms"], "ttfa_overlap_ms"),
        "Gemini_Wait": _ref_block("Gemini wait", "AUDIO_STREAM_END_SENT -> GEMINI_FIRST_AUDIO", "Time between turn commit and first backend audio packet from Gemini.", ["gemini_wait_ms"], "gemini_wait_overlap_ms"),
        "Turn_Decision": _ref_block("Turn decision latency", "LAST_MIC_FRAME -> AUDIO_STREAM_END_SENT", "VAD debounce and backend decision latency.", ["turn_decision_latency_ms"]),
        "Playback_Scheduling": _ref_block("Playback scheduling delay", "AUDIO_PACKET_RECEIVED -> AUDIO_PLAYBACK_SCHEDULED", "Client scheduling delay between receiving audio packet and scheduling playback.", ["playback_scheduling_delay_ms", "first_audio_to_first_playback_ms"]),
        "Network": _ref_block("Network publish->receive", "PUBLISH -> RECEIVE", "Measured publish to receive latency between publisher and client.", ["network_publish_to_receive_ms"]),
        "PCM_Decode": _ref_block("PCM decode", "PCM_START -> PCM_COMPLETE", "Time spent decoding PCM on client.", ["pcm_decode_ms"]),
        "Frontend_Rendering": _ref_block("Frontend text render", "TEXT_PACKET_RECEIVED -> REACT_RENDER_COMPLETED", "Time for text to be rendered in the client.", ["text_render_latency_ms"]),
    }

    # User-experience: pick the most appropriate anchor for TTFT/TTFA.
    # Prefer a candidate with a positive average; fallback to the first available.
    def _choose_positive_key(candidates: List[str]) -> Optional[str]:
        for k in candidates:
            if k in summary:
                s = _get_stat(summary, k)
                avg = s.get("average")
                if avg is not None and avg > 0:
                    return k
        for k in candidates:
            if k in summary:
                return k
        return None

    ttft_key = _choose_positive_key(["ttft_ms", "time_to_first_text_render_ms", "time_to_first_text_arrival_ms"])
    # Prefer the actual playback anchor (`time_to_first_audio_playback_ms`) over `ttfa_ms`.
    ttfa_key = _choose_positive_key(["time_to_first_audio_playback_ms", "time_to_first_audio_frontend_ms", "ttfa_ms"])

    ttft = {
        "actual_wait": _get_stat(summary, ttft_key) if ttft_key else {"average": None},
        "raw": _get_stat(summary, "ttft_raw_ms") if "ttft_raw_ms" in summary else {"average": None},
    }
    ttfa = {
        "actual_wait": _get_stat(summary, ttfa_key) if ttfa_key else {"average": None},
        "raw": _get_stat(summary, "ttfa_raw_ms") if "ttfa_raw_ms" in summary else {"average": None},
    }

    # End-to-end breakdown: speech vs processing
    end_stat = _get_stat(summary, "end_to_end_ms")
    speech_stat = _get_stat(summary, "speech_duration_ms")
    processing_avg = None
    if end_stat.get("average") is not None and speech_stat.get("average") is not None:
        processing_avg = round(end_stat.get("average") - speech_stat.get("average"), 3)

    end_to_end_breakdown = {
        "speech_duration_ms": speech_stat,
        "pipeline_processing_ms": {"average": processing_avg},
        "total_ms": end_stat,
    }

    # Pipeline flow (ordered for readability)
    pipeline_flow = [
        {"step": "Turn Decision", "average_ms": _get_stat(summary, "turn_decision_latency_ms").get("average")},
        {"step": "Gemini", "average_ms": _get_stat(summary, "gemini_processing_ms").get("average")},
        {"step": "Network", "average_ms": _get_stat(summary, "network_publish_to_receive_ms").get("average")},
        {"step": "Playback", "average_ms": _get_stat(summary, "playback_scheduling_delay_ms").get("average")},
        {"step": "Frontend", "average_ms": _get_stat(summary, "text_render_latency_ms").get("average")},
        {"step": "PCM", "average_ms": _get_stat(summary, "pcm_decode_ms").get("average")},
    ]

    # Pipeline breakdown: show each stage's formula + average + p95 (re-added)
    pipeline_breakdown = {
        "turn_decision": {"description": "VAD debounce and decision", "formula": "LAST_MIC_FRAME -> AUDIO_STREAM_END_SENT", "average_ms": _get_stat(summary, "turn_decision_latency_ms").get("average"), "p95_ms": _get_stat(summary, "turn_decision_latency_ms").get("p95")},
        "gemini_processing": {"description": "Gemini processing", "formula": "AUDIO_SENT_TO_GEMINI -> GEMINI_FIRST_TOKEN/AUDIO", "average_ms": _get_stat(summary, "gemini_processing_ms").get("average"), "p95_ms": _get_stat(summary, "gemini_processing_ms").get("p95")},
        "network": {"description": "Publish -> Receive", "formula": "PUBLISH -> RECEIVE", "average_ms": _get_stat(summary, "network_publish_to_receive_ms").get("average"), "p95_ms": _get_stat(summary, "network_publish_to_receive_ms").get("p95")},
        "playback_scheduling": {"description": "Audio packet -> scheduled playback", "formula": "AUDIO_PACKET_RECEIVED -> AUDIO_PLAYBACK_SCHEDULED", "average_ms": _get_stat(summary, "playback_scheduling_delay_ms").get("average"), "p95_ms": _get_stat(summary, "playback_scheduling_delay_ms").get("p95")},
        "pcm_decode": {"description": "PCM decode on client", "formula": "PCM_START -> PCM_COMPLETE", "average_ms": _get_stat(summary, "pcm_decode_ms").get("average"), "p95_ms": _get_stat(summary, "pcm_decode_ms").get("p95")},
        "frontend": {"description": "Text render latency", "formula": "TEXT_PACKET_RECEIVED -> REACT_RENDER_COMPLETED", "average_ms": _get_stat(summary, "text_render_latency_ms").get("average"), "p95_ms": _get_stat(summary, "text_render_latency_ms").get("p95")},
    }

    # Session analysis: averages, longest/shortest turn, total session duration
    speech_min = speech_stat.get("min")
    speech_max = speech_stat.get("max")
    avg_turns = total_corr
    total_session_duration_ms = None
    per_corr = metrics.get("per_correlation", {}) or {}
    if per_corr:
        total_session_duration_ms = 0.0
        for c in per_corr.values():
            cm = c.get("metrics", {}).get("correlation_completion_ms")
            if cm is not None:
                try:
                    total_session_duration_ms += float(cm)
                except Exception:
                    pass

    # Cold start heuristic: first gemini processing >> median
    first_corr_key = next(iter(per_corr), None)
    cold_start = None
    if first_corr_key and per_corr.get(first_corr_key):
        first_gemini = per_corr.get(first_corr_key, {}).get("metrics", {}).get("gemini_processing_ms")
        median_gemini = component_stats.get("Gemini", {}).get("median")
        if first_gemini is not None and median_gemini is not None:
            cold_start = bool(first_gemini > (median_gemini * 1.5))

    # Cold start label (heuristic) — keep the boolean but clearly mark inference
    cold_label = None
    if cold_start is not None:
        cold_label = "Likely Cold Start (heuristic)" if cold_start else "Not Cold Start (heuristic)"

    session_analysis = {
        "cold_start_heuristic": cold_start,
        "cold_start_label": cold_label,
        "warm_session": (False if cold_start else True) if cold_start is not None else None,
        "average_turns": avg_turns,
        "longest_turn_ms": speech_max,
        "shortest_turn_ms": speech_min,
        "total_session_duration_ms": total_session_duration_ms,
    }

    # Streaming quality summary
    cont_pct = measured["streaming"].get("continuous_audio_pct")
    if cont_pct is not None:
        if cont_pct >= 99.0:
            stream_quality_label = "Excellent"
        elif cont_pct >= 95.0:
            stream_quality_label = "Good"
        elif cont_pct >= 90.0:
            stream_quality_label = "Fair"
        else:
            stream_quality_label = "Poor"
    else:
        stream_quality_label = None

    streaming_quality = {
        "quality_label": stream_quality_label,
        "total_playback_chunks": measured["streaming"].get("total_playback_chunks"),
        "restart_count": measured["streaming"].get("restart_count"),
        "starvation_count": measured["streaming"].get("starvation_count"),
        "continuous_audio_pct": measured["streaming"].get("continuous_audio_pct"),
        "maximum_gap_ms": measured["streaming"].get("maximum_gap_ms"),
    }

    # Streaming overlap: early/overlap stats for key metrics
    streaming_overlap = {
        "ttft_overlap_ms": _get_stat(summary, "ttft_overlap_ms"),
        "ttfa_overlap_ms": _get_stat(summary, "ttfa_overlap_ms"),
        "gemini_wait_overlap_ms": _get_stat(summary, "gemini_wait_overlap_ms"),
    }

    # Component performance: full statistics per component (keep existing shape)
    component_performance = {
        "gemini": component_stats.get("Gemini"),
        "network": component_stats.get("Network"),
        "playback": component_stats.get("Playback"),
        "frontend": component_stats.get("Frontend Rendering"),
        "pcm": component_stats.get("PCM Decode"),
    }

    validation_section = {
        "trace_valid": measured["validation"].get("trace_valid"),
        "trace_quality": measured["validation"].get("trace_quality"),
        "ordering_errors": measured["validation"].get("ordering_errors"),
        "duplicate_events": measured["validation"].get("duplicate_events"),
        "missing_events": measured["validation"].get("missing_events"),
        "clock_issues_count": measured["validation"].get("clock_issues_count"),
        "correlation_completion": {"total": total_corr, "completed": completed_corr, "incomplete": incomplete_corr},
    }

    # Executive summary lightly updated to reference new breakdowns
    executive_summary = {
        "session_health": measured["session_health"],
        "streaming_summary": {
            "continuous_audio_pct": measured["streaming"].get("continuous_audio_pct"),
            "average_gap_ms": measured["streaming"].get("average_gap_ms"),
        },
        "primary_bottleneck": top_bottlenecks[0]["component"] if top_bottlenecks else None,
        "trace_quality": measured["validation"].get("trace_quality"),
        "quick_summary": {
            "completed_turns": measured["session_health"].get("completed_correlations"),
            "avg_end_to_end_ms": measured["end_to_end_latency_ms"].get("average"),
            "avg_source_transcript_ms": _get_stat(summary, "source_transcript_latency_ms").get("average"),
            "avg_target_transcript_ms": _get_stat(summary, "target_transcript_latency_ms").get("average"),
            "avg_translation_audio_ms": _get_stat(summary, "translation_audio_latency_ms").get("average"),
            "continuous_streaming_pct": measured["streaming"].get("continuous_audio_pct"),
        }
    }

    # Helper: build user-facing KPI block with availability and reason when missing
    def _mk_user_block(key: str, per_corr_reason_key: str, event_name: str) -> Dict[str, Optional[float]]:
        stat = _get_stat(summary, key)
        block = dict(stat)
        # If we have sample_count > 0 treat as available
        if block.get("sample_count"):
            block["status"] = "AVAILABLE"
        else:
            # Try to find a human-readable reason from per-correlation metrics
            reason = None
            for c in per_corr.values():
                r = c.get("metrics", {}).get(per_corr_reason_key)
                if r:
                    reason = r
                    break
            if not reason:
                reason = f"{event_name} event missing"
            block["status"] = "NOT_AVAILABLE"
            block["reason"] = reason
        return block

    # User Experience: top-level user-facing KPIs (frontend-anchored)
    user_experience = {
        "source_transcript": _mk_user_block("source_transcript_latency_ms", "source_transcript_unavailable_reason", "SOURCE_TRANSCRIPT_RENDERED"),
        "target_transcript": _mk_user_block("target_transcript_latency_ms", "target_transcript_unavailable_reason", "TARGET_TRANSCRIPT_RENDERED"),
        "translation_audio": _mk_user_block("translation_audio_latency_ms", "translation_audio_unavailable_reason", "AUDIO_FIRST_AUDIBLE"),
        "end_to_end_breakdown": end_to_end_breakdown,
        "continuous_streaming_pct": measured["streaming"].get("continuous_audio_pct"),
    }

    # Clarify detection method for AUDIO_FIRST_AUDIBLE so consumers understand
    # this is an approximate client-side metric and not a hardware audibility timestamp.
    try:
        if "translation_audio" in user_experience and isinstance(user_experience["translation_audio"], dict):
            user_experience["translation_audio"]["detection_method"] = (
                "Client-side audible detection: AnalyserNode RMS threshold "
                "(frontend implementation polls Web Audio AnalyserNode and emits AUDIO_FIRST_AUDIBLE when RMS crosses a low threshold)."
            )
    except Exception:
        pass

    session_section = {
        "session_id": metrics.get("session_id"),
        "generated_at": _iso_from_epoch_ms(metrics.get("generated_at_epoch_ms")),
        "benchmark_version": BENCHMARK_ENGINE_VERSION,
        "metrics_version": metrics.get("metrics_schema_version"),
        "trace_validation_version": val.get("validation_schema_version") or val.get("trace_validation_version"),
        "metrics_engine_version": metrics.get("metrics_engine_version"),
        "benchmark_engine_version": BENCHMARK_ENGINE_VERSION,
    }

    benchmark = {
        "session": session_section,
        "executive_summary": executive_summary,
        "user_experience": user_experience,
        "streaming_overlap": streaming_overlap,
        "pipeline_flow": pipeline_flow,
        "pipeline_breakdown": pipeline_breakdown,
        "streaming_health": measured["streaming"],
        "streaming_quality": streaming_quality,
        "component_performance": component_performance,
        "validation_trace_health": validation_section,
        "bottlenecks": top_bottlenecks,
        "optimization_opportunities": optimization_opportunities,
        "session_analysis": session_analysis,
        "derived_insights": {"recommendations": recommendations, "session_status": {"completion_pct": completion_pct, "total_correlations": total_corr}},
        "engineering_reference": engineering_reference,
    }

    try:
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(benchmark, f, indent=2)
        logger.info("Wrote benchmark.json → %s", out_path)
    except Exception as e:
        logger.exception("Failed to write benchmark.json: %s", e)

    return benchmark


if __name__ == "__main__":
    # Allow manual invocation for verification during development; this
    # does not create a second generator — it simply exercises the
    # canonical benchmark_engine.
    import sys
    from pathlib import Path

    if len(sys.argv) < 2:
        print("Usage: python benchmark_engine.py <session_output_dir>")
        raise SystemExit(2)
    sd = Path(sys.argv[1])
    generate_benchmark(sd)
