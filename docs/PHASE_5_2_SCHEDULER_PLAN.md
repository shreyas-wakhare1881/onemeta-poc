Phase 5.2 — Playback Scheduler Optimization (Design)

Status: Draft

This document captures the Phase 5.2 design, analysis, and implementation plan for optimizing the frontend playback scheduler. It follows the constraints in the repo and uses repository instrumentation and traces as the single source of truth.

1) Scope and goals
- Reduce playback backlog and scheduling delay while preserving audio ordering and natural playback.
- Preserve existing telemetry and tracing.
- Prefer minimal, incremental changes with measurable ROI.

2) Findings summary (Phase 5.1)
- Median playback scheduling delay: ~437 ms (session 7)
- Avg backlog: ~3138 ms; max backlog: ~7681 ms
- Avg queue depth before scheduling: ~13; max: 31
- Root cause: append-only global `nextPlayTime` timeline in `PCMStreamPlayer.playChunk()` leads to unbounded client-side backlog when producer supplies audio-time faster than wall-clock.

3) Relevant files / decision points
- frontend/services/pcmPlayer.service.ts — PCMStreamPlayer.playChunk() (scheduling, nextPlayTime, activeNodesCount, sourceNode.start())
- frontend/hooks/useLiveKit.ts — handleDataReceived() (packet parsing, packet_id, chunk_index injection)
- frontend/app/page.tsx — consumes aiEvents and calls pcmPlayer.playChunk(...)
- backend/app/audio/agent.py — producer-side bounded publish_queue with eviction (backend drop).
- backend/app/audio/config.py — audio framing configuration (sample rate, frame duration, publisher_queue_size)

4) Per-location recommendations (keep/modify/remove)
- PCMStreamPlayer.playChunk() — modify: replace append-only timeline with rate-aware scheduling and limited buffering (see design below). Keep decoding and tracing.
- useLiveKit.handleDataReceived() — keep as-is; continue injecting packet_id + chunk_index (required for pairing). No change.
- app/page.tsx event handler — keep as-is; no pre-buffering. Consider optional drop policy toggle at UI level.
- agent.py publish_queue eviction — keep; backend should remain source-of-truth for upstream backpressure.

5) Proposed scheduler architecture (high level)
- Maintain a small soft buffer window (W) at the client (e.g., 100–300 ms) instead of unlimited append.
- On arrival of a chunk, compute: chunk_duration (D), current_audio_time = audioCtx.currentTime, backlog_ms = max(0, nextPlayTime - current_audio_time) * 1000.
- If backlog_ms < W: append chunk as before (schedule at nextPlayTime).
- If backlog_ms >= W: instead of appending and increasing backlog, adopt one of the following adaptive strategies (ranked by ROI):
  1. Trim-and-append (preferred): If chunk_duration small, append but reduce scheduled gap by compressing silence—effectively schedule chunk at max(current_audio_time, nextPlayTime - trim_amount). Implementation: calculate allowable_shift = min(backlog_ms - W, chunk_duration * 0.5) then schedule earlier by allowable_shift.
  2. Skip/Drop-Low-Importance: If chunk belongs to a correlation marked as low-priority or is a partial update and backlog exceeds high-water mark (H), drop oldest pending partial chunks from same correlation. Preserve final/completed chunks.
  3. Progressive playback (play immediately at current_audio_time): For final chunks where semantic continuity is less critical, schedule at current_audio_time to minimize latency.

Key properties preserved:
- Ordering: default strategy preserves arrival ordering by appending; trim and immediate-play strategies are conservative and apply only when backlog exceeds W.
- Continuous playback: small shifts keep the audio continuous and avoid large gaps.
- Multi-correlation: scheduler remains correlation-agnostic by default but supports per-correlation priority metadata to avoid starving one correlation.

6) Detailed design
- Parameters:
  - W (soft window) = 100–300 ms (tunable)
  - H (high-water mark) = 2000–3000 ms (monitoring alarm)
  - trim_fraction = 0.2–0.5 (how much of chunk can be time-compressed)

- Algorithm (on each chunk arrival):
  1. Decode chunk to determine D (duration).
  2. Read currentTime and nextPlayTime.
  3. Compute backlog_ms = max(0, nextPlayTime - currentTime) * 1000.
  4. If backlog_ms < W: schedule at nextPlayTime (unchanged).
  5. Else if W <= backlog_ms < H: schedule at adjusted_time = nextPlayTime - min((backlog_ms - W)/1000, D * trim_fraction). Record `trim_applied` metadata.
  6. Else if backlog_ms >= H: prefer dropping oldest pending partial chunks (backend should be configured to avoid functionally critical dropping). For UI, log tracing events `PLAYBACK_DROP_DECISION` and `PLAYBACK_TRIM_APPLIED`.

- Implementation notes:
  - Time shifting must use `audioCtx.currentTime` and respect `sourceNode.start()` scheduling precision.
  - Tracing must record original `scheduled_time_sec` and `adjusted_time_sec` together with `trim_applied_ms` and `drop_reason` if applied.
  - Avoid cross-correlation starvation: allow `correlation_priority` in event metadata; when dropping, prefer dropping lower-priority correlation chunks.

7) Required code changes (file-level)
- frontend/services/pcmPlayer.service.ts
  - Add configuration constants for `W`, `H`, `trim_fraction`.
  - Before scheduling, compute backlog and choose: append, trim-and-append, or drop. Implement a `applyTrim` helper that modifies scheduledTime and updates `chunk_duration_sec` accordingly (log `trim_applied_ms`).
  - Emit `PLAYBACK_TRIM_APPLIED` and `PLAYBACK_DROP_DECISION` tracer events with correlation and packet ids.
  - Preserve original `AUDIO_PLAYBACK_SCHEDULED` event fields; add `adjusted_scheduled_time_sec` and `trim_applied_ms` when changed.

- frontend/app/page.tsx
  - No change required. Optional: expose UI toggle to control conservative vs aggressive mode.

- frontend/hooks/useLiveKit.ts
  - No change required.

8) Implementation plan (step-by-step)
1. Add `W/H/trim_fraction` constants in `pcmPlayer.service.ts` and unit tests for trim calculation.
2. Implement `applyTrim` logic and tracing fields on scheduling.
3. Add metrics counters for `trim_applied_count` and `drop_count` and expose in telemetry.
4. Run local session (Session 8) benchmark and compare `metrics.json` vs Session 7.
5. Tweak `W` and `trim_fraction` based on observed `playback_scheduling_delay_ms` and audio artifacts.

9) Estimated impact, complexity, risk, confidence, ROI ranking
- Trim-and-append (Strategy 1):
  - Expected playback scheduling reduction: 30–70% (median may drop from ~437 ms to ~150 ms depending on W).
  - Expected E2E reduction: 200–800 ms in many sessions.
  - Implementation complexity: Low–Medium (changes localized to `pcmPlayer`).
  - Risk: Low audio artifacts if `trim_fraction` small; moderate if aggressive.
  - Confidence: Medium-high.
  - ROI rank: 1

- Immediate play (Strategy 3):
  - Expected scheduling reduction: High but potential audio discontinuities.
  - Complexity: Low
  - Risk: Medium-high (audio jumpiness)
  - Confidence: Medium
  - ROI rank: 2

- Drop partial chunks (Strategy 2):
  - Expected scheduling reduction: High for bursts where many partials are present
  - Complexity: Medium (requires marking partial vs final, per-correlation logic)
  - Risk: Moderate (lost partial content, but final chunks retained)
  - Confidence: Medium
  - ROI rank: 3

10) Observability and metrics
- Preserve `AUDIO_PLAYBACK_SCHEDULED` and `NEXT_PLAYTIME_UPDATED` events.
- Add `PLAYBACK_TRIM_APPLIED` and `PLAYBACK_DROP_DECISION` events with metadata.
- Track `avg_backlog_ms`, `avg_queue_depth_before`, `trim_applied_count`, `drop_count` in `metrics_engine`.

11) Rollout plan
- Phase 5.2B-1: Implement trim-and-append with conservative defaults (`W=300ms`, `trim_fraction=0.2`). Validate locally.
- Phase 5.2B-2: Measure Session 8, adjust `W` and `trim_fraction` to balance latency vs artifact.
- Phase 5.2B-3: Optionally implement drop-policy for extreme `H` exceedance.

12) Risks and mitigation
- Risk: Audio artifacts from time-compression. Mitigate via small `trim_fraction` and A/B listening tests.
- Risk: Unintended ordering changes. Mitigate: only shift earlier by <= half chunk duration and keep finalization semantics.

13) Next steps (immediately)
- Produce exact file edits for `frontend/services/pcmPlayer.service.ts` implementing the conservative trim-and-append strategy. (Implementation only after design sign-off.)

Appendix: Key code references
- frontend/services/pcmPlayer.service.ts — `playChunk()` and scheduling code (`sourceNode.start`)
- frontend/hooks/useLiveKit.ts — `handleDataReceived()` packet propagation
- frontend/app/page.tsx — `pcmPlayer.playChunk(...)` calls

End of draft.
