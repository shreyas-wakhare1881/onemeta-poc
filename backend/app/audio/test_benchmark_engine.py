"""
Unit tests for the Simplified Benchmark Engine (Phase 6)
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from backend.app.audio import benchmark_engine


def test_generate_benchmark_integration():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        metrics_file = tmp_path / "metrics.json"

        # Mock metrics.json content
        mock_metrics = {
            "session_id": "integration-test-session",
            "metrics_schema_version": 2,
            "total_correlations": 2,
            "completed_correlations": 2,
            "incomplete_correlations": 0,
            "metrics_summary": {
                "ttfa_ms": {
                    "avg_ms": 245.0,
                    "p95_ms": 320.0,
                    "count": 2,
                    "coverage_pct": 100.0
                },
                "ttfa_overlap_ms": {
                    "avg_ms": 10.0,
                    "p95_ms": 15.0,
                    "count": 2,
                    "coverage_pct": 100.0
                },
                "ttft_ms": {
                    "avg_ms": 180.0,
                    "p95_ms": 210.0,
                    "count": 2,
                    "coverage_pct": 100.0
                },
                "ttft_overlap_ms": {
                    "avg_ms": 20.0,
                    "p95_ms": 30.0,
                    "count": 2,
                    "coverage_pct": 100.0
                },
                "gemini_wait_ms": {
                    "avg_ms": 150.0,
                    "p95_ms": 180.0,
                    "count": 2,
                    "coverage_pct": 100.0
                },
                "gemini_wait_overlap_ms": {
                    "avg_ms": 30.0,
                    "p95_ms": 40.0,
                    "count": 2,
                    "coverage_pct": 100.0
                },
                "first_audio_to_first_playback_ms": {
                    "avg_ms": 80.0,
                    "p95_ms": 90.0,
                    "count": 2,
                    "coverage_pct": 100.0
                },
                "speech_duration_ms": {
                    "avg_ms": 1200.0,
                    "p95_ms": 1500.0,
                    "count": 2,
                    "coverage_pct": 100.0
                },
                "turn_decision_latency_ms": {
                    "avg_ms": 600.0,
                    "p95_ms": 600.0,
                    "count": 2,
                    "coverage_pct": 100.0
                }
            },
            "streaming_continuity": {
                "total_playback_chunks": 12,
                "inter_chunk_gap_count": 10,
                "average_gap_ms": 15.0,
                "maximum_gap_ms": 45.0,
                "median_gap_ms": 12.0,
                "p95_gap_ms": 35.0,
                "restart_count": 0,
                "restart_threshold_ms": 500.0,
                "starvation_count": 0,
                "continuous_audio_pct": 100.0
            }
        }

        with open(metrics_file, "w", encoding="utf-8") as f:
            json.dump(mock_metrics, f)

        # Run benchmark engine
        benchmark_engine.generate_benchmark(tmp_path)

        benchmark_file = tmp_path / "benchmark.json"
        assert benchmark_file.exists()

        with open(benchmark_file, "r", encoding="utf-8") as f:
            bench_data = json.load(f)

        assert bench_data["session_id"] == "integration-test-session"
        assert bench_data["session_health"]["total_correlations"] == 2
        assert bench_data["primary_kpis"]["ttfa"]["average_ms"] == 245.0
        assert bench_data["primary_kpis"]["ttfa"]["turn_overlap_ms"] == 10.0
        assert bench_data["primary_kpis"]["ttft"]["average_ms"] == 180.0
        assert bench_data["primary_kpis"]["ttft"]["turn_overlap_ms"] == 20.0
        assert bench_data["primary_kpis"]["gemini_wait"]["average_ms"] == 150.0
        assert bench_data["primary_kpis"]["gemini_wait"]["turn_overlap_ms"] == 30.0
        assert bench_data["primary_kpis"]["ttfa"]["coverage_pct"] == 100.0
        assert bench_data["streaming"]["p95_gap_ms"] == 35.0
        assert bench_data["bottlenecks"][0]["stage"] == "vad_debounce_delay"
        assert bench_data["executive_summary"]["ttfa_ms"] == 245.0


def test_generate_benchmark_missing_metrics():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        # metrics.json is NOT created here

        # Should not raise exception
        try:
            benchmark_engine.generate_benchmark(tmp_path)
        except Exception as e:
            assert False, f"generate_benchmark crashed when metrics.json is missing: {e}"

        # benchmark.json should not exist
        benchmark_file = tmp_path / "benchmark.json"
        assert not benchmark_file.exists()


def test_generate_benchmark_empty_correlations():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        metrics_file = tmp_path / "metrics.json"

        # Mock metrics.json with empty correlations
        mock_metrics = {
            "session_id": "empty-session",
            "metrics_schema_version": 2,
            "total_correlations": 0,
            "completed_correlations": 0,
            "incomplete_correlations": 0,
            "metrics_summary": {},
            "streaming_continuity": {}
        }

        with open(metrics_file, "w", encoding="utf-8") as f:
            json.dump(mock_metrics, f)

        # Should generate without crashing
        try:
            benchmark_engine.generate_benchmark(tmp_path)
        except Exception as e:
            assert False, f"generate_benchmark crashed on empty correlations: {e}"

        benchmark_file = tmp_path / "benchmark.json"
        assert benchmark_file.exists()

        with open(benchmark_file, "r", encoding="utf-8") as f:
            bench_data = json.load(f)

        assert bench_data["session_id"] == "empty-session"
        assert bench_data["primary_kpis"]["ttfa"]["average_ms"] is None
        assert bench_data["primary_kpis"]["ttft"]["average_ms"] is None
