import { tracer } from './trace.service';
import { PipelineEvent } from '../types/trace';

export class PCMStreamPlayer {
  private audioCtx: AudioContext | null = null;
  private nextPlayTime = 0;
  private activeNodesCount = 0;
  
  public onPlaybackStart: (() => void) | null = null;
  public onPlaybackEnd: (() => void) | null = null;

  // Experiment instrumentation metrics
  public playChunkCalledCount = 0;
  public playChunkScheduledCount = 0;
  public playbackStartEventCount = 0;
  public playbackEndEventCount = 0;

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

  public playChunk(base64Data: string, correlationId: string = '') {
    this.playChunkCalledCount++;
    if (tracer.isEnabled()) {
      tracer.logEvent(PipelineEvent.PCM_DECODE_STARTED, correlationId, { packet_size: base64Data.length });
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
          duration_sec: audioBuffer.duration
        });
      }

      // 4. Create source node
      const sourceNode = this.audioCtx.createBufferSource();
      sourceNode.buffer = audioBuffer;
      sourceNode.connect(this.audioCtx.destination);

      const currentTime = this.audioCtx.currentTime;
      // If nextPlayTime is in the past, schedule immediately
      if (this.nextPlayTime < currentTime) {
        this.nextPlayTime = currentTime;
      }

      const playDelaySec = this.nextPlayTime - currentTime;
      const scheduledTime = this.nextPlayTime;

      // Schedule play
      sourceNode.start(scheduledTime);

      if (tracer.isEnabled()) {
        const currentMonoNs = performance.now() * 1_000_000;
        const playMonoNs = Math.round(currentMonoNs + playDelaySec * 1_000_000_000);
        const playEpochMs = Date.now() + playDelaySec * 1000;

        tracer.logEvent(PipelineEvent.AUDIO_SCHEDULED, correlationId, {
          scheduled_time_sec: scheduledTime,
          delay_sec: playDelaySec
        });

        // Record the scheduled playback timestamp based on the Web Audio scheduling timeline.
        // Clearly distinguish scheduled playback time from actual callback-based playback detection.
        tracer.logEvent(
          PipelineEvent.AUDIO_PLAYBACK_SCHEDULED,
          correlationId,
          {
            scheduled_time_sec: scheduledTime,
            description: "Scheduled playback timestamp based on Web Audio scheduling timeline"
          },
          playEpochMs,
          playMonoNs
        );
      }

      this.nextPlayTime += audioBuffer.duration;
      this.playChunkScheduledCount++;

      // 5. Track state changes
      this.activeNodesCount++;
      if (this.activeNodesCount === 1) {
        this.playbackStartEventCount++;
        if (this.onPlaybackStart) {
          this.onPlaybackStart();
        }
      }

      sourceNode.onended = () => {
        this.activeNodesCount--;
        if (this.activeNodesCount <= 0) {
          this.activeNodesCount = 0;
          this.playbackEndEventCount++;
          if (this.onPlaybackEnd) {
            this.onPlaybackEnd();
          }
        }
      };

    } catch (e) {
      console.error('PCMStreamPlayer: Failed to play chunk:', e);
    }
  }

  public stop() {
    this.nextPlayTime = 0;
    this.activeNodesCount = 0;
    this.playChunkCalledCount = 0;
    this.playChunkScheduledCount = 0;
    this.playbackStartEventCount = 0;
    this.playbackEndEventCount = 0;
    if (this.audioCtx) {
      try {
        this.audioCtx.close();
      } catch (e) {}
      this.audioCtx = null;
    }
  }
}

export const pcmPlayer = new PCMStreamPlayer();
