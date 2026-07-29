"""
Analytics and Reporting Engine (Optional Layer)

Computes:
  - Historical comparison runs (supporting both old and new schema variants)
  - Regression detection against previous session
  - Subjective/approximate similarity score against Gemini Live targets
  - Subjective performance grades (A, B, C...)

This reporting/analytics layer is separated from the core reality-measurement
instrumentation layer (metrics_engine, benchmark_engine).
Outputs: analytics_report.json
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("onemeta.analytics_engine")

TARGETS = {
    "ttfa_excellent_ms": 500.0,
    "ttfa_good_ms": 1000.0,
    "ttfa_acceptable_ms": 2000.0,
    "gemini_live_ttfa_target_ms": 200.0,
    "gemini_live_streaming_quality": 95.0,
    "gemini_live_gap_p95_ms": 50.0,
}


def _grade(score: float) -> str:
    if score >= 92: return "A+"
    elif score >= 85: return "A"
    elif score >= 76: return "B"
    elif score >= 67: return "C"
    elif score >= 50: return "D"
    return "F"


def _score_against_thresholds(value: Optional[float], excellent: float, good: float, acceptable: float) -> float:
    if value is None: return 0.0
    # Negative latency represents instant responsiveness
    if value <= 0: return 100.0
    if value <= excellent: return 100.0
    if value <= good:
        return 75.0 + 25.0 * (good - value) / max(good - excellent, 1)
    if value <= acceptable:
        return 50.0 + 25.0 * (acceptable - value) / max(acceptable - good, 1)
    return max(0.0, 50.0 * (acceptable / max(value, 1)))


def _gemini_live_similarity(ttfa_avg: Optional[float], gap_p95: Optional[float], continuous_pct: Optional[float]) -> Dict[str, Any]:
    ref_ttfa = TARGETS["gemini_live_ttfa_target_ms"]
    ref_gap  = TARGETS["gemini_live_gap_p95_ms"]
    ref_cont = TARGETS["gemini_live_streaming_quality"]

    ttfa_sim = None
    if ttfa_avg is not None:
        ttfa_sim = min(100.0, ref_ttfa / max(ttfa_avg, ref_ttfa) * 100.0) if ttfa_avg > 0 else 100.0

    streaming_sim = None
    if gap_p95 is not None:
        gap_sim = min(100.0, ref_gap / max(gap_p95, ref_gap) * 100.0)
        cont_sim = (continuous_pct or 0.0) / ref_cont * 100.0 if continuous_pct is not None else 50.0
        streaming_sim = (gap_sim + min(100.0, cont_sim)) / 2.0

    overall = None
    if ttfa_sim is not None and streaming_sim is not None:
        overall = (ttfa_sim * 0.6 + streaming_sim * 0.4)

    return {
        "ttfa_similarity_pct": round(ttfa_sim, 1) if ttfa_sim is not None else None,
        "streaming_similarity_pct": round(streaming_sim, 1) if streaming_sim is not None else None,
        "overall_similarity_pct": round(overall, 1) if overall is not None else None,
    }


def generate_analytics_report(session_dir: Path) -> Dict[str, Any]:
    benchmark_path = session_dir / "benchmark.json"
    report_path = session_dir / "analytics_report.json"

    if not benchmark_path.exists():
        logger.error(f"[AnalyticsEngine] benchmark.json not found at {benchmark_path}")
        return {}

    try:
        with open(benchmark_path, "r", encoding="utf-8") as f:
            bench = json.load(f)
    except Exception as e:
        logger.exception(f"[AnalyticsEngine] Failed to load benchmark.json: {e}")
        return {}

    es = bench.get("executive_summary") or {}
    mm = bench.get("measured_metrics") or {}
    td = bench.get("turn_decision_pipeline") or {}

    ttfa_val = es.get("ttfa_ms")
    ttft_val = es.get("ttft_ms")
    p95_gap  = es.get("p95_gap_ms")
    restarts = es.get("restart_count")

    # ── 1. Calculate Grades and Scores ────────────────────────────────────────
    ttfa_score = _score_against_thresholds(ttfa_val, 500.0, 1000.0, 2000.0) if ttfa_val is not None else 0.0
    ttfa_grade = _grade(ttfa_score)

    # ── 2. Gemini Live Similarity ─────────────────────────────────────────────
    # Get continuous audio percentage (from metrics.json if not in benchmark.json)
    cont_pct = None
    metrics_path = session_dir / "metrics.json"
    if metrics_path.exists():
        try:
            with open(metrics_path, "r", encoding="utf-8") as mf:
                metrics_data = json.load(mf)
                cont_pct = metrics_data.get("streaming_continuity", {}).get("continuous_audio_pct")
        except Exception:
            pass

    similarity = _gemini_live_similarity(ttfa_val, p95_gap, cont_pct)

    # ── 3. Historical Runs Parsing with fallback ──────────────────────────────
    historical_runs = []
    regression_warnings = []
    try:
        output_dir = session_dir.parent
        other_dirs = []
        if output_dir.exists():
            for p in output_dir.iterdir():
                if p.is_dir() and p.name != session_dir.name and (p / "benchmark.json").exists():
                    other_dirs.append(p)
        other_dirs.sort(key=lambda d: d.name)
        recent_dirs = other_dirs[-3:]
        for r_dir in reversed(recent_dirs):
            try:
                with open(r_dir / "benchmark.json", "r", encoding="utf-8") as rf:
                    old_bench = json.load(rf)
                
                old_es = old_bench.get("executive_summary") or {}
                old_pk = old_bench.get("primary_kpis") or old_bench.get("measured_metrics") or {}
                old_ttfa_fc = old_pk.get("ttfa_from_turn_commit_ms") or old_pk.get("ttfa") or {}
                old_ttft_fc = old_pk.get("ttft_from_turn_commit_ms") or old_pk.get("ttft") or {}
                old_streaming = old_pk.get("streaming_continuity") or old_bench.get("streaming") or {}
                
                # Fetch TTFA average
                old_ttfa = old_ttfa_fc.get("value_ms") or old_ttfa_fc.get("avg_ms") or old_ttfa_fc.get("average_ms") or old_es.get("ttfa_ms")
                if old_ttfa is None:
                    ttfa_obj = old_es.get("ttfa")
                    if isinstance(ttfa_obj, dict):
                        old_ttfa = ttfa_obj.get("average_ms")
                    if old_ttfa is None:
                        old_ttfa = old_bench.get("measured_benchmarks", {}).get("first_response", {}).get("audio_latency_ms", {}).get("average")
                
                # Fetch TTFT average
                old_ttft = old_ttft_fc.get("value_ms") or old_ttft_fc.get("avg_ms") or old_ttft_fc.get("average_ms") or old_es.get("ttft_ms")
                if old_ttft is None:
                    ttft_obj = old_es.get("ttft")
                    if isinstance(ttft_obj, dict):
                        old_ttft = ttft_obj.get("average_ms")
                    if old_ttft is None:
                        old_ttft = old_bench.get("measured_benchmarks", {}).get("first_response", {}).get("text_latency_ms", {}).get("average")

                # Fetch P95 gap
                old_gap = old_streaming.get("p95_gap_ms") or old_es.get("p95_gap_ms")
                if old_gap is None:
                    cont_obj = old_es.get("continuous_audio_streaming")
                    if isinstance(cont_obj, dict):
                        old_gap = cont_obj.get("p95_gap_ms")
                
                # Fetch restart count
                old_restarts = old_streaming.get("restart_count") or old_es.get("restart_count")
                if old_restarts is None:
                    cont_obj = old_es.get("continuous_audio_streaming")
                    if isinstance(cont_obj, dict):
                        old_restarts = cont_obj.get("playback_restart_count")

                old_sess_id = old_bench.get("session_id") or old_bench.get("session", {}).get("session_id") or r_dir.name

                historical_runs.append({
                    "session_folder": r_dir.name,
                    "session_id": old_sess_id,
                    "ttfa_avg_ms": old_ttfa,
                    "ttft_avg_ms": old_ttft,
                    "p95_gap_ms": old_gap,
                    "restart_count": old_restarts
                })
            except Exception as re:
                logger.debug(f"Failed to read historical benchmark from {r_dir}: {re}")

        # Check for regressions against the immediately preceding run
        if historical_runs:
            prev = historical_runs[0]
            prev_ttfa = prev.get("ttfa_avg_ms")
            if prev_ttfa is not None and ttfa_val is not None:
                # 15% deterioration and at least 50ms absolute increase
                if ttfa_val > prev_ttfa * 1.15 and (ttfa_val - prev_ttfa) > 50.0:
                    regression_warnings.append(
                        f"TTFA deteriorated by {((ttfa_val - prev_ttfa)/prev_ttfa)*100.0:.1f}% "
                        f"({ttfa_val:.1f}ms vs previous {prev_ttfa:.1f}ms)"
                    )
            
            prev_gap = prev.get("p95_gap_ms")
            if prev_gap is not None and p95_gap is not None:
                # 25% deterioration and at least 20ms absolute increase
                if p95_gap > prev_gap * 1.25 and (p95_gap - prev_gap) > 20.0:
                    regression_warnings.append(
                        f"Streaming jitter (p95 gap) deteriorated by {((p95_gap - prev_gap)/prev_gap)*100.0:.1f}% "
                        f"({p95_gap:.1f}ms vs previous {prev_gap:.1f}ms)"
                    )
    except Exception as he:
        logger.warning(f"Failed to compile historical runs comparison: {he}")

    # ── 4. Build Report JSON ──────────────────────────────────────────────────
    report = {
        "session_id": bench.get("session_id"),
        "generated_at_epoch_ms": int(time.time() * 1000),
        "performance_assessment": {
            "responsiveness_score": round(ttfa_score, 1),
            "responsiveness_grade": ttfa_grade,
            "basis": f"TTFA avg {ttfa_val:.1f}ms from turn commit" if ttfa_val is not None else "N/A"
        },
        "gemini_live_similarity": similarity,
        "historical_comparison": historical_runs,
        "regression_warnings": regression_warnings
    }

    try:
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        logger.info(f"[AnalyticsEngine] Wrote analytics_report.json → {report_path}")
    except Exception as e:
        logger.exception(f"[AnalyticsEngine] Failed to write analytics_report.json: {e}")

    return report
