# Benchmark Engine — Walkthrough

Purpose
-------
This document explains the fields emitted by `benchmark_engine.py` and proves
that every numeric value in `benchmark.json` is taken only from `metrics.json`
and `trace_validation.json` (the canonical inputs in the pipeline).

Files of interest
- Metrics input: [onemeta-poc/output/2026-07-28_02-23-00_test11/metrics.json](onemeta-poc/output/2026-07-28_02-23-00_test11/metrics.json)
- Trace validation: [onemeta-poc/output/2026-07-28_02-23-00_test11/trace_validation.json](onemeta-poc/output/2026-07-28_02-23-00_test11/trace_validation.json)
- Generated sample: [onemeta-poc/output/2026-07-28_02-23-00_test11/benchmark.json](onemeta-poc/output/2026-07-28_02-23-00_test11/benchmark.json)

Design constraints
------------------
- The benchmark engine uses only the fields present in `metrics.json` and
  `trace_validation.json`.
- No subjective scoring, letter grades, or invented regression statistics are
  added. Those belong in `analytics_report.json`.
- The file `benchmark_engine.py` is the single canonical generator. No other
  benchmark generators exist in the repository.

Structure of `benchmark.json`
----------------------------
Top-level keys produced:

- `session`: session metadata and engine versions.
- `measured_benchmarks`: rich, numeric metrics (min/max/avg/median/p95 etc.)
- `derived_insights`: objective lists and deterministic recommendations
- `executive_summary`: cheat-sheet entries for key metrics (description,
  formula, and exact stats)

Provenance mapping (measured fields)
------------------------------------
All mappings below point to the key in `metrics.json` (the `metrics_summary`
map) or to the `streaming_continuity` / `vad_summary` sections when appropriate.

- Session health
  - `total_correlations` ← `metrics.json` key `total_correlations`
  - `completed_correlations` ← `metrics.json` key `completed_correlations`
  - `incomplete_correlations` ← `metrics.json` key `incomplete_correlations`
  - `success_rate_percent` ← computed as completed / total (no external data)

- End-to-end latency
  - `measured_benchmarks.end_to_end_latency_ms` ← `metrics_summary.end_to_end_ms`
    (fields: `min_ms`, `max_ms`, `avg_ms`, `median_ms`, `p95_ms`, `count`, `coverage_pct`)

- Gemini processing
  - `measured_benchmarks.gemini_processing_ms` ← `metrics_summary.gemini_processing_ms`

- First response
  - Text latency: prefer `metrics_summary.ttft_ms` else `time_to_first_text_render_ms` / `time_to_first_text_arrival_ms`
  - Audio latency: prefer `metrics_summary.ttfa_ms` else `time_to_first_audio_playback_ms` / `time_to_first_audio_frontend_ms`

- Playback
  - `measured_benchmarks.playback` ← `metrics_summary.playback_scheduling_delay_ms` or `first_audio_to_first_playback_ms`

- Network
  - `measured_benchmarks.network` ← `metrics_summary.network_publish_to_receive_ms`

- PCM decode
  - `measured_benchmarks.pcm_decode` ← `metrics_summary.pcm_decode_ms`

- Frontend rendering
  - `measured_benchmarks.frontend_rendering` ← `metrics_summary.text_render_latency_ms`

- Streaming
  - `measured_benchmarks.streaming.*` ← `streaming_continuity` fields in `metrics.json`:
    - `average_gap_ms`, `median_gap_ms`, `p95_gap_ms`, `maximum_gap_ms`,
      `restart_count`, `starvation_count`, `continuous_audio_pct`, `total_playback_chunks`,
      `restart_threshold_ms`.

- Validation summary
  - `measured_benchmarks.validation.*` ← pass-through values from
    `trace_validation.json`: `trace_valid`, `trace_quality`, `ordering_errors`,
    `duplicate_events`, `incomplete_correlations`. Additionally the engine
    counts clock-related errors by scanning `trace_validation.json`'s
    `errors` array for `monotonic timestamp decreased` / `monotonic jitter`.

Derived insights (objective)
----------------------------
- Top bottlenecks
  - Produced by sorting measured averages for these metrics:
    `network_publish_to_receive_ms`, `gemini_processing_ms`,
    `playback_scheduling_delay_ms`, `text_render_latency_ms`, `pcm_decode_ms`.
  - Only measured averages are used for ranking.

- Optimization opportunities
  - Deterministically computed from a small table of engineering thresholds
    (e.g. Network 100 ms, Gemini 200 ms, Playback 100 ms, Frontend 50 ms,
    PCM 10 ms).
  - Each opportunity contains: `component`, `priority`, `reason`,
    `current_average_ms`, and `target_ms`.
  - `priority` is derived deterministically: `HIGH` if avg >= 1.5x target,
    `MEDIUM` if avg > target, else `LOW`.
  - No `expected_gain_ms` or invented numeric benefit is included.

- Recommendations
  - Deterministic, single-line, objective statements, e.g.
    - `Network latency exceeds target: avg X ms > Y ms`
    - `Trace ordering issues detected: N`
    - `Completion rate below threshold: X% < 90%`

Executive summary
-----------------
Each block is explicit about provenance and formula. Example entries:

- `TTFT` (first translated text)
  - Description: Time from User Speech Start to first translated text available
  - Formula: `MIC_FRAME_RECEIVED -> first TEXT_PACKET_RECEIVED or REACT_RENDER_COMPLETED`
  - Values: taken directly from `metrics_summary.ttft_ms` (or fallback keys)

- `TTFA` (first translated audio)
  - Description: Time from User Speech Start to first translated audio actually playing
  - Formula: `MIC_FRAME_RECEIVED -> first AUDIO_PLAYBACK_STARTED` (explicitly uses
    the playback-start anchor; the engine documents that this is 'timer-based'
    on the client and therefore may represent a scheduled start rather than
    audible confirmation — the raw metric is preserved as-is.)

- `Gemini_Wait`, `Speech_Duration`, `Turn_Decision_Latency`, `Playback_Scheduling`,
  `Network`, `PCM_Decode`, `Frontend_Rendering` — each block includes:
  - `description`, `formula`, and the exact statistic fields pulled from
    `metrics_summary` (average, median, min, max, p95, sample_count, coverage).

Why subjective analysis is excluded
----------------------------------
- `benchmark.json` is an engineering artifact. Its role is to present
  deterministic, verifiable telemetry originating from the tracer and
  metrics engine.
- Subjective scoring (letter grades, historical/regression comparisons,
  composite performance scores) is the responsibility of the analytics
  layer (`analytics_engine.py`), which can combine benchmarks, business rules,
  and historical baselines to produce management-facing outputs.

Verification steps (how to reproduce)
------------------------------------
1. Run the canonical pipeline so `metrics.json` and `trace_validation.json` are produced.
2. From the repository root run:

```powershell
python backend/app/audio/benchmark_engine.py <path/to/session_output_dir>
```

Example:

```powershell
python backend/app/audio/benchmark_engine.py onemeta-poc/output/2026-07-28_02-23-00_test11
```

This writes the deterministic engineering dashboard to the session folder
as `benchmark.json`.

Notes and caveats
-----------------
- The benchmark engine only projects values already computed by the metrics
  engine. If a required metric is missing (coverage 0%) the field is left
  as `null`.
- Negative network or stage averages (from clock skew) are preserved so
  engineers can see skew effects; the validator also surfaces such issues in
  `trace_validation.json`.

Contact
-------
For follow-ups about specific metric definitions or to add additional
transparent fields (still computed only from `metrics.json`), open an issue
or request a small patch to `benchmark_engine.py` that documents the new
mapping.
