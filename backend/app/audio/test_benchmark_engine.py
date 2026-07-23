"""
Unit tests for the Benchmark Engine (Phase 4)
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from backend.app.audio import benchmark_engine


def test_compute_stats_empty():
    res = benchmark_engine._compute_stats([])
    assert res["min"] is None
    assert res["max"] is None
    assert res["average"] is None
    assert res["median"] is None


def test_compute_stats_normal():
    # Values: [100.0, 200.0, 300.0]
    res = benchmark_engine._compute_stats([100.0, 200.0, 300.0], [90, 95])
    assert res["min"] == 100.0
    assert res["max"] == 300.0
    assert res["average"] == 200.0
    assert res["median"] == 200.0
    assert res["p90"] == 280.0
    assert res["p95"] == 290.0


def test_calculate_performance_score():
    # Perfect score scenario
    score_data = benchmark_engine._calculate_performance_score(
        success_rate=100.0,
        e2e_avg=500.0,
        gemini_avg=120.0,
        playback_avg=80.0,
        network_avg=15.0
    )
    assert score_data["score"] == 100
    assert score_data["overall"] == "A"

    # Lower score scenario
    score_data_low = benchmark_engine._calculate_performance_score(
        success_rate=50.0,
        e2e_avg=2000.0,
        gemini_avg=500.0,
        playback_avg=300.0,
        network_avg=100.0
    )
    assert score_data_low["score"] < 70
    assert score_data_low["overall"] in ("D", "F")


def test_compute_top_bottlenecks():
    bottlenecks = benchmark_engine._compute_top_bottlenecks(
        playback_avg=400.0,
        gemini_avg=150.0,
        network_avg=30.0,
        frontend_avg=12.0,
        pcm_avg=0.5
    )
    assert len(bottlenecks) == 5
    assert bottlenecks[0]["component"] == "Playback"
    assert bottlenecks[0]["average_ms"] == 400.0
    assert bottlenecks[0]["rank"] == 1
    assert bottlenecks[1]["component"] == "Gemini"
    assert bottlenecks[4]["component"] == "PCM Decode"


def test_compute_opportunities():
    opps = benchmark_engine._compute_opportunities(
        playback_avg=400.0,  # target: 100 -> gain: 300 (HIGH)
        gemini_avg=180.0,    # target: 150 -> gain: 30 (MEDIUM)
        network_avg=15.0,    # target: 20 -> gain: 0 (None)
        frontend_avg=12.0,   # target: 10 -> gain: 2 (LOW)
        pcm_avg=0.1          # target: 0.2 -> gain: 0 (None)
    )
    assert len(opps) == 3
    assert opps[0]["component"] == "Playback"
    assert opps[0]["priority"] == "HIGH"
    assert opps[0]["expected_gain_ms"] == 300.0

    assert opps[1]["component"] == "Gemini"
    assert opps[1]["priority"] == "MEDIUM"

    assert opps[2]["component"] == "Frontend Rendering"
    assert opps[2]["priority"] == "LOW"


def test_generate_recommendations():
    recs = benchmark_engine._generate_recommendations(
        success_rate=85.0,
        playback_avg=350.0,
        gemini_avg=120.0,
        network_avg=15.0,
        frontend_avg=8.0,
        pcm_avg=0.1
    )
    assert any("Playback scheduling delay is high" in r for r in recs)
    assert any("Gemini latency is within expected limits" in r for r in recs)
    assert any("Session success rate is low" in r for r in recs)


def test_generate_benchmark_integration():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        metrics_file = tmp_path / "metrics.json"

        # Mock metrics.json content
        mock_metrics = {
            "session_id": "integration-test-session",
            "metrics_schema_version": 1,
            "total_correlations": 2,
            "completed_correlations": 2,
            "incomplete_correlations": 0,
            "per_correlation": {
                "corr-1": {
                    "metrics": {
                        "end_to_end_ms": 500.0,
                        "gemini_processing_ms": 120.0,
                        "playback_scheduling_delay_ms": 80.0,
                        "network_publish_to_receive_ms": 15.0,
                        "pcm_decode_ms": 0.2,
                        "text_render_latency_ms": 8.0,
                        "time_to_first_text_render_ms": 220.0,
                        "time_to_first_audio_frontend_ms": 150.0
                    }
                },
                "corr-2": {
                    "metrics": {
                        "end_to_end_ms": 600.0,
                        "gemini_processing_ms": 130.0,
                        "playback_scheduling_delay_ms": 90.0,
                        "network_publish_to_receive_ms": 20.0,
                        "pcm_decode_ms": 0.3,
                        "text_render_latency_ms": 10.0,
                        "time_to_first_text_render_ms": 240.0,
                        "time_to_first_audio_frontend_ms": 160.0
                    }
                }
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

        assert bench_data["session"]["session_id"] == "integration-test-session"
        assert bench_data["measured_benchmarks"]["session_health"]["success_rate_percent"] == 100.0
        assert bench_data["measured_benchmarks"]["end_to_end_latency_ms"]["average"] == 550.0
        assert bench_data["measured_benchmarks"]["gemini_processing_ms"]["average"] == 125.0
        assert bench_data["derived_insights"]["performance_score"]["score"] == 100
        assert bench_data["derived_insights"]["performance_score"]["overall"] == "A"


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
            "metrics_schema_version": 1,
            "total_correlations": 0,
            "completed_correlations": 0,
            "incomplete_correlations": 0,
            "per_correlation": {}
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

        assert bench_data["session"]["session_id"] == "empty-session"
        assert bench_data["measured_benchmarks"]["session_health"]["success_rate_percent"] == 0.0
        
        # Derived insights for empty/null metrics
        assert bench_data["derived_insights"]["performance_score"]["overall"] == "N/A"
        assert bench_data["derived_insights"]["performance_score"]["score"] == 0
        assert bench_data["derived_insights"]["top_bottlenecks"] == []
        assert bench_data["derived_insights"]["optimization_opportunities"] == []
        assert bench_data["derived_insights"]["recommendations"] == ["No valid benchmark data available for this session."]

