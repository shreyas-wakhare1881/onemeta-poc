"""
Benchmark Engine (Phase 4)

Consumes metrics.json and generates session-level performance benchmark (benchmark.json).
"""
from __future__ import annotations

import json
import logging
import math
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("onemeta.benchmark_engine")

BENCHMARK_VERSION = "1.0.0"


def _compute_stats(values: List[float], percentiles: List[int] = []) -> Dict[str, Optional[float]]:
    """Helper to compute min, max, average, median, and arbitrary percentiles of a float list.
    Handles empty lists gracefully by returning None for all statistics.
    """
    if not values:
        return {
            "min": None,
            "max": None,
            "average": None,
            "median": None,
            **{f"p{p}": None for p in percentiles}
        }

    sorted_vals = sorted(values)
    n = len(sorted_vals)

    def _percentile(p: float) -> float:
        if n == 0:
            return 0.0
        rank = (p / 100.0) * (n - 1)
        lo = int(math.floor(rank))
        hi = int(math.ceil(rank))
        if lo == hi:
            return float(sorted_vals[lo])
        weight = rank - lo
        return float(sorted_vals[lo] * (1.0 - weight) + sorted_vals[hi] * weight)

    res = {
        "min": float(sorted_vals[0]),
        "max": float(sorted_vals[-1]),
        "average": float(statistics.mean(sorted_vals)),
        "median": float(statistics.median(sorted_vals))
    }

    for p in percentiles:
        res[f"p{p}"] = float(_percentile(p))

    # Round calculations for consistency and representation
    for k, v in res.items():
        if v is not None:
            res[k] = round(v, 2)

    return res


def _calculate_performance_score(
    success_rate: float,
    e2e_avg: Optional[float],
    gemini_avg: Optional[float],
    playback_avg: Optional[float],
    network_avg: Optional[float]
) -> Dict[str, Any]:
    """Deterministic score calculation (0 to 100) and letter grading.
    Gracefully scales the weights based on available metrics.
    """
    total_weight = 0.0
    earned_points = 0.0

    # 1. Success Rate (weight 30)
    total_weight += 30.0
    earned_points += (success_rate / 100.0) * 30.0

    # 2. End-to-End Latency (weight 30)
    # Target SLA: <= 600ms is perfect (1.0), >= 3000ms is poor (0.0)
    if e2e_avg is not None:
        total_weight += 30.0
        ratio = max(0.0, min(1.0, (3000.0 - e2e_avg) / (3000.0 - 600.0)))
        earned_points += ratio * 30.0

    # 3. Gemini Processing (weight 20)
    # Target SLA: <= 150ms is perfect (1.0), >= 800ms is poor (0.0)
    if gemini_avg is not None:
        total_weight += 20.0
        ratio = max(0.0, min(1.0, (800.0 - gemini_avg) / (800.0 - 150.0)))
        earned_points += ratio * 20.0

    # 4. Playback Delay (weight 10)
    # Target SLA: <= 100ms is perfect (1.0), >= 500ms is poor (0.0)
    if playback_avg is not None:
        total_weight += 10.0
        ratio = max(0.0, min(1.0, (500.0 - playback_avg) / (500.0 - 100.0)))
        earned_points += ratio * 10.0

    # 5. Network Latency (weight 10)
    # Target SLA: <= 20ms is perfect (1.0), >= 200ms is poor (0.0)
    if network_avg is not None:
        total_weight += 10.0
        ratio = max(0.0, min(1.0, (200.0 - network_avg) / (200.0 - 20.0)))
        earned_points += ratio * 10.0

    if total_weight > 0:
        score = int(round((earned_points / total_weight) * 100.0))
    else:
        score = 100

    if score >= 90:
        overall = "A"
    elif score >= 80:
        overall = "B"
    elif score >= 70:
        overall = "C"
    elif score >= 60:
        overall = "D"
    else:
        overall = "F"

    return {
        "overall": overall,
        "score": score
    }


def _compute_top_bottlenecks(
    playback_avg: Optional[float],
    gemini_avg: Optional[float],
    network_avg: Optional[float],
    frontend_avg: Optional[float],
    pcm_avg: Optional[float]
) -> List[Dict[str, Any]]:
    """Rank components by their average latency in descending order."""
    items = []
    if playback_avg is not None:
        items.append(("Playback", playback_avg))
    if gemini_avg is not None:
        items.append(("Gemini", gemini_avg))
    if network_avg is not None:
        items.append(("Network", network_avg))
    if frontend_avg is not None:
        items.append(("Frontend Rendering", frontend_avg))
    if pcm_avg is not None:
        items.append(("PCM Decode", pcm_avg))

    items.sort(key=lambda x: x[1], reverse=True)

    return [
        {
            "rank": idx,
            "component": name,
            "average_ms": round(avg, 2)
        }
        for idx, (name, avg) in enumerate(items, 1)
    ]


def _compute_opportunities(
    playback_avg: Optional[float],
    gemini_avg: Optional[float],
    network_avg: Optional[float],
    frontend_avg: Optional[float],
    pcm_avg: Optional[float]
) -> List[Dict[str, Any]]:
    """Identify optimization opportunities based on SLA targets and potential gains."""
    targets = {
        "Playback": (100.0, playback_avg),
        "Gemini": (150.0, gemini_avg),
        "Network": (20.0, network_avg),
        "Frontend Rendering": (10.0, frontend_avg),
        "PCM Decode": (0.2, pcm_avg)
    }

    opps = []
    for comp, (target, avg) in targets.items():
        if avg is not None and avg > target:
            gain = avg - target
            if gain >= 100.0:
                priority = "HIGH"
            elif gain >= 30.0:
                priority = "MEDIUM"
            else:
                priority = "LOW"

            opps.append({
                "component": comp,
                "priority": priority,
                "expected_gain_ms": round(gain, 2)
            })

    # Sort opportunities by expected gain in descending order
    opps.sort(key=lambda x: x["expected_gain_ms"], reverse=True)
    return opps


def _generate_recommendations(
    success_rate: float,
    playback_avg: Optional[float],
    gemini_avg: Optional[float],
    network_avg: Optional[float],
    frontend_avg: Optional[float],
    pcm_avg: Optional[float]
) -> List[str]:
    """Generate concise data-driven recommendations."""
    recs = []

    if playback_avg is not None:
        if playback_avg > 200.0:
            recs.append(f"Playback scheduling delay is high (average {round(playback_avg, 1)} ms). Optimize client-side queue size or scheduling intervals.")
        else:
            recs.append(f"Playback scheduling latency is healthy (average {round(playback_avg, 1)} ms).")

    if gemini_avg is not None:
        if gemini_avg > 250.0:
            recs.append(f"Gemini latency is elevated (average {round(gemini_avg, 1)} ms). Check region hosting or prompt length.")
        else:
            recs.append(f"Gemini latency is within expected limits (average {round(gemini_avg, 1)} ms).")

    if network_avg is not None:
        if network_avg > 50.0:
            recs.append(f"Network transmission latency is high (average {round(network_avg, 1)} ms). Consider checking network connection or compression.")
        else:
            recs.append(f"Network latency is healthy (average {round(network_avg, 1)} ms).")

    if pcm_avg is not None:
        if pcm_avg > 1.0:
            recs.append(f"PCM decoding is slow (average {round(pcm_avg, 2)} ms). Consider hardware acceleration or optimizing buffer size.")
        else:
            recs.append(f"PCM decoding does not require optimization (average {round(pcm_avg, 2)} ms is very low).")

    if frontend_avg is not None:
        if frontend_avg > 30.0:
            recs.append(f"Frontend rendering delay is high (average {round(frontend_avg, 1)} ms). Profile React component rendering.")
        else:
            recs.append(f"Frontend rendering latency is healthy (average {round(frontend_avg, 1)} ms).")

    if success_rate < 90.0:
        recs.append(f"Session success rate is low ({round(success_rate, 1)}%). Check for connection drops or crash logs.")
    else:
        recs.append(f"Session health is excellent with a success rate of {round(success_rate, 1)}%.")

    return recs


def generate_benchmark(session_dir: Path) -> None:
    """Consumes metrics.json from session_dir and outputs benchmark.json.
    Ensures safe operations and logs lifecycle events.
    """
    import time
    start_time = time.perf_counter()
    logger.info("[BenchmarkEngine] BENCHMARK_GENERATION_STARTED")

    metrics_path = session_dir / "metrics.json"
    benchmark_path = session_dir / "benchmark.json"

    if not metrics_path.exists():
        logger.warning(f"[BenchmarkEngine] metrics.json not found in {session_dir}. Skipping benchmark generation.")
        elapsed_ms = (time.perf_counter() - start_time) * 1000.0
        logger.info(f"[BenchmarkEngine] BENCHMARK_GENERATION_COMPLETED — generation_time_ms={elapsed_ms:.1f}")
        return

    # Load metrics.json
    try:
        with open(metrics_path, "r", encoding="utf-8") as f:
            metrics_json = json.load(f)
        logger.info(f"[BenchmarkEngine] BENCHMARK_METRICS_LOADED — path={metrics_path}")
    except Exception as e:
        logger.error(f"[BenchmarkEngine] Failed to load metrics.json: {e}")
        elapsed_ms = (time.perf_counter() - start_time) * 1000.0
        logger.info(f"[BenchmarkEngine] BENCHMARK_GENERATION_COMPLETED — generation_time_ms={elapsed_ms:.1f}")
        return

    logger.info("[BenchmarkEngine] BENCHMARK_CALCULATION_STARTED")

    # Extract session variables
    session_id = metrics_json.get("session_id")
    generated_at_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Extract correlation metadata
    total_correlations = metrics_json.get("total_correlations", 0)
    completed_correlations = metrics_json.get("completed_correlations", 0)
    incomplete_correlations = metrics_json.get("incomplete_correlations", 0)
    
    success_rate_percent = 0.0
    if total_correlations > 0:
        success_rate_percent = round((completed_correlations / total_correlations) * 100.0, 2)

    # Accumulate metrics across individual correlations
    end_to_end_vals = []
    gemini_vals = []
    playback_vals = []
    network_vals = []
    pcm_decode_vals = []
    text_render_vals = []
    text_first_response_vals = []
    audio_first_response_vals = []

    required_metrics = [
        "time_to_first_text_arrival_ms",
        "time_to_first_text_render_ms",
        "text_render_latency_ms",
        "time_to_first_audio_frontend_ms",
        "time_to_first_audio_backend_ms",
        "end_to_end_ms",
        "gemini_processing_ms",
        "pcm_decode_ms",
        "playback_scheduling_delay_ms",
        "correlation_completion_ms",
        "network_publish_to_receive_ms"
    ]

    per_correlation = metrics_json.get("per_correlation", {})
    missing_fields_warning_logged = False

    for c_id, c_data in per_correlation.items():
        m = c_data.get("metrics") or {}
        
        # Check expected metrics fields exist in the dictionary (not necessarily non-None)
        missing_keys = [k for k in required_metrics if k not in m]
        if missing_keys and not missing_fields_warning_logged:
            logger.warning(
                f"[BenchmarkEngine] Missing expected metrics fields in correlation {c_id}: {missing_keys}"
            )
            missing_fields_warning_logged = True
        
        # End-to-End
        e2e = m.get("end_to_end_ms")
        if e2e is not None:
            end_to_end_vals.append(e2e)
            
        # Gemini
        gemini = m.get("gemini_processing_ms")
        if gemini is not None:
            gemini_vals.append(gemini)
            
        # Playback
        playback = m.get("playback_scheduling_delay_ms")
        if playback is not None:
            playback_vals.append(playback)
            
        # Network
        net = m.get("network_publish_to_receive_ms")
        if net is not None:
            network_vals.append(net)
            
        # PCM Decode
        pcm = m.get("pcm_decode_ms")
        if pcm is not None:
            pcm_decode_vals.append(pcm)
            
        # Frontend Rendering (text_render_latency_ms)
        f_render = m.get("text_render_latency_ms")
        if f_render is not None:
            text_render_vals.append(f_render)

        # First Text Response
        ft = m.get("time_to_first_text_render_ms")
        if ft is None:
            ft = m.get("time_to_first_text_arrival_ms")
        if ft is not None:
            text_first_response_vals.append(ft)

        # First Audio Response
        fa = m.get("time_to_first_audio_frontend_ms")
        if fa is None:
            fa = m.get("time_to_first_audio_backend_ms")
        if fa is not None:
            audio_first_response_vals.append(fa)

    # Check if there is absolutely no valid correlation data or all are empty/None
    all_empty = not (
        end_to_end_vals or
        gemini_vals or
        playback_vals or
        network_vals or
        pcm_decode_vals or
        text_render_vals or
        text_first_response_vals or
        audio_first_response_vals
    )

    # Compute Statistics
    end_to_end_stats = _compute_stats(end_to_end_vals, [90, 95, 99])
    gemini_stats = _compute_stats(gemini_vals, [95])
    playback_stats = _compute_stats(playback_vals)
    network_stats = _compute_stats(network_vals)
    pcm_decode_stats = _compute_stats(pcm_decode_vals)
    frontend_rendering_stats = _compute_stats(text_render_vals)
    text_first_response_stats = _compute_stats(text_first_response_vals)
    audio_first_response_stats = _compute_stats(audio_first_response_vals)

    # Derived Insights
    playback_avg = playback_stats.get("average")
    gemini_avg = gemini_stats.get("average")
    network_avg = network_stats.get("average")
    frontend_rendering_avg = frontend_rendering_stats.get("average")
    pcm_decode_avg = pcm_decode_stats.get("average")
    e2e_avg = end_to_end_stats.get("average")

    if all_empty:
        perf_score = {
            "overall": "N/A",
            "score": 0
        }
        top_bottlenecks = []
        opportunities = []
        recommendations = ["No valid benchmark data available for this session."]
    else:
        perf_score = _calculate_performance_score(
            success_rate_percent,
            e2e_avg,
            gemini_avg,
            playback_avg,
            network_avg
        )

        top_bottlenecks = _compute_top_bottlenecks(
            playback_avg,
            gemini_avg,
            network_avg,
            frontend_rendering_avg,
            pcm_decode_avg
        )

        opportunities = _compute_opportunities(
            playback_avg,
            gemini_avg,
            network_avg,
            frontend_rendering_avg,
            pcm_decode_avg
        )

        recommendations = _generate_recommendations(
            success_rate_percent,
            playback_avg,
            gemini_avg,
            network_avg,
            frontend_rendering_avg,
            pcm_decode_avg
        )

    logger.info("[BenchmarkEngine] BENCHMARK_CALCULATION_COMPLETED")

    # Build final benchmark.json structure
    benchmark_data = {
        "session": {
            "session_id": session_id,
            "generated_at": generated_at_iso,
            "benchmark_version": BENCHMARK_VERSION,
            "metrics_version": str(metrics_json.get("metrics_schema_version", "1.0.0"))
        },
        "measured_benchmarks": {
            "session_health": {
                "total_correlations": total_correlations,
                "completed_correlations": completed_correlations,
                "incomplete_correlations": incomplete_correlations,
                "success_rate_percent": success_rate_percent
            },
            "end_to_end_latency_ms": {
                "min": end_to_end_stats.get("min"),
                "max": end_to_end_stats.get("max"),
                "average": end_to_end_stats.get("average"),
                "median": end_to_end_stats.get("median"),
                "p90": end_to_end_stats.get("p90"),
                "p95": end_to_end_stats.get("p95"),
                "p99": end_to_end_stats.get("p99")
            },
            "gemini_processing_ms": {
                "min": gemini_stats.get("min"),
                "max": gemini_stats.get("max"),
                "average": gemini_stats.get("average"),
                "median": gemini_stats.get("median"),
                "p95": gemini_stats.get("p95")
            },
            "first_response": {
                "text_latency_ms": {
                    "average": text_first_response_stats.get("average"),
                    "median": text_first_response_stats.get("median")
                },
                "audio_latency_ms": {
                    "average": audio_first_response_stats.get("average"),
                    "median": audio_first_response_stats.get("median")
                }
            },
            "playback": {
                "average_delay_ms": playback_stats.get("average"),
                "median_delay_ms": playback_stats.get("median"),
                "max_delay_ms": playback_stats.get("max")
            },
            "network": {
                "average_ms": network_stats.get("average"),
                "median_ms": network_stats.get("median"),
                "max_ms": network_stats.get("max")
            },
            "pcm_decode": {
                "average_ms": pcm_decode_stats.get("average"),
                "max_ms": pcm_decode_stats.get("max")
            },
            "frontend_rendering": {
                "average_ms": frontend_rendering_stats.get("average"),
                "median_ms": frontend_rendering_stats.get("median"),
                "max_ms": frontend_rendering_stats.get("max")
            }
        },
        "derived_insights": {
            "performance_score": perf_score,
            "top_bottlenecks": top_bottlenecks,
            "optimization_opportunities": opportunities,
            "recommendations": recommendations
        }
    }

    try:
        with open(benchmark_path, "w", encoding="utf-8") as f:
            json.dump(benchmark_data, f, indent=2)
        logger.info(f"[BenchmarkEngine] BENCHMARK_JSON_WRITTEN — path={benchmark_path}")
    except Exception as e:
        logger.error(f"[BenchmarkEngine] Failed to write benchmark.json to {benchmark_path}: {e}")

    elapsed_ms = (time.perf_counter() - start_time) * 1000.0
    logger.info(f"[BenchmarkEngine] BENCHMARK_GENERATION_COMPLETED — generation_time_ms={elapsed_ms:.1f}")
