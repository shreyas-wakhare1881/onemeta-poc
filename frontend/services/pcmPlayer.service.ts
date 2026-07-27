import { tracer } from './trace.service';
import { PipelineEvent } from '../types/trace';

export class PCMStreamPlayer {
  private audioCtx: AudioContext | null = null;
  private nextPlayTime = 0;
  // Track active scheduled nodes so we can apply gentle catch-up speedups when backlog grows
  private activeNodes: Array<{ source: AudioBufferSourceNode; scheduledStart: number; scheduledEnd: number; duration: number }> = [];

  public onPlaybackStart: (() => void) | null = null;
  public onPlaybackEnd: (() => void) | null = null;

  // Experiment instrumentation metrics
  public playChunkCalledCount = 0;
  public playChunkScheduledCount = 0;
  public playbackStartEventCount = 0;
  public playbackEndEventCount = 0;
  public playChunkDroppedCount = 0;

  // Adaptive scheduler tuning (configurable)
  // Target client buffer window — avoid accumulating more than this on the client
  private readonly MAX_CLIENT_BUFFER_MS = 80; // soft window (W), tunable — reduced from 200ms to 80ms for lower scheduling latency
  private readonly MIN_SCHEDULE_AHEAD_SEC = 0.008; // reduced from 50ms to 8ms — minimum Web Audio API scheduling headroom

  constructor() {
    // AudioContext will be initialized on first user interaction (session start)
  }

  private initAudioContext() {
    if (!this.audioCtx) {
      const AudioCtxClass = window.AudioContext || (window as any).webkitAudioContext;
      this.audioCtx = new AudioCtxClass();
      this.nextPlayTime = 0;
    }
    if (this.audioCtx.state === 'suspended') {
      this.audioCtx.resume();
    }
  }

  public playChunk(base64Data: string, correlationId: string = '', packetId?: string, chunkIndex?: number) {
    this.playChunkCalledCount++;
    if (tracer.isEnabled()) {
      tracer.logEvent(PipelineEvent.PCM_DECODE_STARTED, correlationId, { packet_size: base64Data.length, packet_id: packetId || '', chunk_index: chunkIndex ?? 0 });
    }
    try {
      this.initAudioContext();
      if (!this.audioCtx) return;

      // 1. Decode base64
      const binaryString = atob(base64Data);
      const len = binaryString.length;
      const bytes = new Uint8Array(len);
      for (let i = 0; i < len; i++) {
        bytes[i] = binaryString.charCodeAt(i);
      }

      // 2. Convert Little-Endian 16-bit PCM to Float32
      const int16Array = new Int16Array(bytes.buffer, bytes.byteOffset, bytes.byteLength / 2);
      const float32Array = new Float32Array(int16Array.length);
      for (let i = 0; i < int16Array.length; i++) {
        float32Array[i] = int16Array[i] / 32768.0;
      }

      if (float32Array.length === 0) return;

      // 3. Create AudioBuffer (24kHz Mono)
      const audioBuffer = this.audioCtx.createBuffer(1, float32Array.length, 24000);
      audioBuffer.getChannelData(0).set(float32Array);

      if (tracer.isEnabled()) {
        tracer.logEvent(PipelineEvent.PCM_DECODE_COMPLETED, correlationId, {
          sample_rate: 24000,
          channels: 1,
          duration_sec: audioBuffer.duration,
          packet_id: packetId || '',
          chunk_index: chunkIndex ?? 0,
          pcm_samples: float32Array.length
        });
      }

      // 4. Create source node
      const sourceNode = this.audioCtx.createBufferSource();
      sourceNode.buffer = audioBuffer;
      sourceNode.connect(this.audioCtx.destination);

      // Scheduler bookkeeping: capture nextPlayTime lifecycle and queue depth before modification
      const currentTime = this.audioCtx.currentTime;
      const audioCtxState = this.audioCtx.state;
      const nextPlayTimeBefore = this.nextPlayTime;
      let resetReason = '';
      // If nextPlayTime is in the past, schedule immediately (preserve behavior)
      if (this.nextPlayTime < currentTime) {
        resetReason = 'reset_to_current_time';
        this.nextPlayTime = currentTime;
      }

      // Recompute after possible reset
      let nextPlayTimeAdjusted = this.nextPlayTime;
      let backlogMs = Math.max(0, nextPlayTimeAdjusted - currentTime) * 1000;
      let queueDepthBefore = this.activeNodes.length;

      // If backlog exceeds the soft buffer window, avoid growing the client-side queue.
      // Production-grade policy: prefer dropping incoming chunks rather than modifying
      // already-scheduled audio. This prevents unlimited nextPlayTime drift and aligns
      // with low-latency streaming principles (do not build a large client queue).
      const incomingDurationMs = Math.round(audioBuffer.duration * 1000);
      const incomingPcmSamples = float32Array.length;

      if (backlogMs > this.MAX_CLIENT_BUFFER_MS) {
        this.playChunkDroppedCount++;
        if (tracer.isEnabled()) {
          tracer.logEvent(PipelineEvent.PLAYBACK_DROP_DECISION, correlationId, {
            reason: 'backlog_exceeded',
            backlog_ms_before: backlogMs,
            backlog_ms_after: backlogMs, // unchanged because we drop
            max_client_buffer_ms: this.MAX_CLIENT_BUFFER_MS,
            packet_id: packetId || '',
            chunk_index: chunkIndex ?? 0,
            dropped_duration_ms: incomingDurationMs,
            dropped_pcm_samples: incomingPcmSamples,
            queue_depth_before: queueDepthBefore,
            queue_depth_after: this.activeNodes.length
          });
        }
        // Do not schedule or update nextPlayTime — drop the chunk to prevent queue growth
        return;
      }

      const playDelaySec = nextPlayTimeAdjusted - currentTime;
      let scheduledTime = nextPlayTimeAdjusted;

      // Predicted scheduled timestamps (epoch + monotonic) for pairing later
      const currentMonoNs = Math.round(performance.now() * 1_000_000);
      const playMonoNsPred = Math.round(currentMonoNs + playDelaySec * 1_000_000_000);
      const playEpochMsPred = Date.now() + playDelaySec * 1000;

      if (tracer.isEnabled()) {
        tracer.logEvent(PipelineEvent.AUDIO_SCHEDULED, correlationId, {
          scheduled_time_sec: scheduledTime,
          delay_sec: playDelaySec,
          backlog_ms: backlogMs,
          audio_ctx_state: audioCtxState,
          audio_ctx_current_time_sec: currentTime,
          next_play_time_before: nextPlayTimeBefore,
          packet_id: packetId || '',
          chunk_index: chunkIndex ?? 0,
          queue_depth_before: queueDepthBefore
        });

        tracer.logEvent(
          PipelineEvent.AUDIO_PLAYBACK_SCHEDULED,
          correlationId,
          {
            scheduled_time_sec: scheduledTime,
            description: 'Scheduled playback timestamp based on Web Audio scheduling timeline',
            backlog_ms: backlogMs,
            audio_ctx_state: audioCtxState,
            queue_depth_before: queueDepthBefore,
            packet_id: packetId || '',
            chunk_index: chunkIndex ?? 0,
            pcm_samples: float32Array.length,
            sample_rate: 24000,
            chunk_duration_sec: audioBuffer.duration
          },
          playEpochMsPred,
          playMonoNsPred
        );
      }

      // Ensure we don't schedule in the immediate past
      if (scheduledTime < currentTime + this.MIN_SCHEDULE_AHEAD_SEC) {
        scheduledTime = currentTime + this.MIN_SCHEDULE_AHEAD_SEC;
      }

      // Schedule play
      // If we applied a speedup to existing nodes, schedule this new node after the adjusted timeline
      sourceNode.start(scheduledTime);

      // Add node to activeNodes tracking so future arrivals can apply catch-up to it as well
      const nodeEntry = {
        source: sourceNode,
        scheduledStart: scheduledTime,
        scheduledEnd: scheduledTime + audioBuffer.duration,
        duration: audioBuffer.duration
      };
      this.activeNodes.push(nodeEntry);

      // Update nextPlayTime cursor to reflect appended node
      const nextPlayTimeAfter = nodeEntry.scheduledEnd;
      const increment = audioBuffer.duration;
      const nextPlayTimeBeforeUpdate = this.nextPlayTime;
      this.nextPlayTime = nextPlayTimeAfter;
      this.playChunkScheduledCount++;

      // Record nextPlayTime evolution for observability
      if (tracer.isEnabled()) {
        tracer.logEvent(PipelineEvent.NEXT_PLAYTIME_UPDATED, correlationId, {
          next_play_time_before: nextPlayTimeBefore,
          next_play_time_after: nextPlayTimeAfter,
          increment: increment,
          reason: resetReason || 'append',
          packet_id: packetId || '',
          chunk_index: chunkIndex ?? 0
        });
      }

      // 5. Track state changes (queue depth semantics preserved)
      const queueDepthAfter = queueDepthBefore + 1;
      if (this.activeNodes.length === 1) {
        this.playbackStartEventCount++;
        if (this.onPlaybackStart) {
          this.onPlaybackStart();
        }
      }

      // Setup actual-playback start detection (best-effort using timer aligned to scheduledTime)
      let recordedStartEpochMs: number | null = null;
      let recordedStartMonoNs: number | null = null;
      const playDelayMs = Math.max(0, Math.round(playDelaySec * 1000));
      const startTimer = setTimeout(() => {
        recordedStartEpochMs = Date.now();
        recordedStartMonoNs = Math.round(performance.now() * 1_000_000);
        if (tracer.isEnabled()) {
          tracer.logEvent(
            PipelineEvent.AUDIO_PLAYBACK_STARTED,
            correlationId,
            {
              packet_id: packetId || '',
              chunk_index: chunkIndex ?? 0,
              scheduled_time_sec: scheduledTime,
              scheduled_time_pred_epoch_ms: playEpochMsPred,
              scheduled_time_pred_monotonic_ns: playMonoNsPred,
              queue_depth_at_start: this.activeNodes.length,
              backlog_ms_at_start: Math.max(0, this.nextPlayTime - (this.audioCtx ? this.audioCtx.currentTime : 0)) * 1000
            },
            recordedStartEpochMs,
            recordedStartMonoNs
          );
        }
      }, playDelayMs);

      sourceNode.onended = () => {
        // Ensure we clear the start timer if the node ended before the start callback (edge cases)
        try { clearTimeout(startTimer); } catch (e) { }

        // Remove node from activeNodes tracking
        try {
          const idx = this.activeNodes.findIndex((n) => n.source === sourceNode);
          if (idx >= 0) this.activeNodes.splice(idx, 1);
        } catch (e) { }

        if (this.activeNodes.length <= 0) {
          this.activeNodes.length = 0;
          this.playbackEndEventCount++;
          if (this.onPlaybackEnd) {
            this.onPlaybackEnd();
          }
        }

        // Record playback completed event with actual times and durations
        const endEpochMs = Date.now();
        const endMonoNs = Math.round(performance.now() * 1_000_000);
        if (tracer.isEnabled()) {
          tracer.logEvent(PipelineEvent.AUDIO_PLAYBACK_COMPLETED, correlationId, {
            packet_id: packetId || '',
            chunk_index: chunkIndex ?? 0,
            scheduled_time_sec: scheduledTime,
            scheduled_time_pred_epoch_ms: playEpochMsPred,
            scheduled_time_pred_monotonic_ns: playMonoNsPred,
            actual_start_epoch_ms: recordedStartEpochMs,
            actual_start_monotonic_ns: recordedStartMonoNs,
            actual_end_epoch_ms: endEpochMs,
            actual_end_monotonic_ns: endMonoNs,
            chunk_duration_sec: audioBuffer.duration,
            queue_depth_after: this.activeNodes.length
          },
            endEpochMs,
            endMonoNs);
        }
      };

    } catch (e) {
      console.error('PCMStreamPlayer: Failed to play chunk:', e);
    }
  }

  public stop() {
    this.nextPlayTime = 0;
    // Clear active nodes tracking
    try { this.activeNodes.length = 0; } catch (e) { }
    this.playChunkCalledCount = 0;
    this.playChunkScheduledCount = 0;
    this.playbackStartEventCount = 0;
    this.playbackEndEventCount = 0;
    if (this.audioCtx) {
      try {
        this.audioCtx.close();
      } catch (e) { }
      this.audioCtx = null;
    }
  }

  // Expose lightweight runtime stats for telemetry and pacing decisions
  public getStats() {
    try {
      const audioState = this.audioCtx ? this.audioCtx.state : 'closed';
      const backlogMs = this.audioCtx ? Math.max(0, this.nextPlayTime - this.audioCtx.currentTime) * 1000 : 0;
      return {
        backlog_ms: Math.round(backlogMs),
        queue_depth: this.activeNodes.length,
        play_chunk_called_count: this.playChunkCalledCount,
        play_chunk_scheduled_count: this.playChunkScheduledCount,
        play_chunk_dropped_count: this.playChunkDroppedCount,
        playback_start_event_count: this.playbackStartEventCount,
        playback_end_event_count: this.playbackEndEventCount,
        audio_ctx_state: audioState
      };
    } catch (e) {
      return {
        backlog_ms: 0,
        queue_depth: 0,
        play_chunk_called_count: this.playChunkCalledCount,
        play_chunk_scheduled_count: this.playChunkScheduledCount,
        play_chunk_dropped_count: this.playChunkDroppedCount,
        playback_start_event_count: this.playbackStartEventCount,
        playback_end_event_count: this.playbackEndEventCount,
        audio_ctx_state: 'error'
      };
    }
  }
}

export const pcmPlayer = new PCMStreamPlayer();
