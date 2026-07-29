export enum PipelineEvent {
  // ── Session lifecycle ──────────────────────────────────────────────────────
  SESSION_STARTED = 'SESSION_STARTED',
  SESSION_ENDED   = 'SESSION_ENDED',

  // ── Frontend data events ───────────────────────────────────────────────────
  TEXT_PACKET_RECEIVED   = 'TEXT_PACKET_RECEIVED',
  AUDIO_PACKET_RECEIVED  = 'AUDIO_PACKET_RECEIVED',
  REACT_RENDER_COMPLETED = 'REACT_RENDER_COMPLETED',
  // Canonical user-visible render/audio anchors (frontend must emit)
  SOURCE_TRANSCRIPT_RENDERED = 'SOURCE_TRANSCRIPT_RENDERED',
  TARGET_TRANSCRIPT_RENDERED = 'TARGET_TRANSCRIPT_RENDERED',
  AUDIO_FIRST_AUDIBLE = 'AUDIO_FIRST_AUDIBLE',

  // ── PCM decode ────────────────────────────────────────────────────────────
  PCM_DECODE_STARTED   = 'PCM_DECODE_STARTED',
  PCM_DECODE_COMPLETED = 'PCM_DECODE_COMPLETED',

  // ── Playback scheduling ───────────────────────────────────────────────────
  AUDIO_SCHEDULED          = 'AUDIO_SCHEDULED',
  AUDIO_PLAYBACK_SCHEDULED = 'AUDIO_PLAYBACK_SCHEDULED',
  AUDIO_PLAYBACK_STARTED   = 'AUDIO_PLAYBACK_STARTED',
  AUDIO_PLAYBACK_COMPLETED = 'AUDIO_PLAYBACK_COMPLETED',

  // ── Playback control ──────────────────────────────────────────────────────
  AUDIO_CONTEXT_STATE    = 'AUDIO_CONTEXT_STATE',
  NEXT_PLAYTIME_UPDATED  = 'NEXT_PLAYTIME_UPDATED',
  PLAYBACK_TRIM_APPLIED  = 'PLAYBACK_TRIM_APPLIED',
  PLAYBACK_DROP_DECISION = 'PLAYBACK_DROP_DECISION',

  // ── VAD control-plane (mirrored from backend for correlation) ─────────────
  // NOTE: These are logged by the backend. Frontend mirrors them in the enum
  // so the merged trace validator recognises them as valid event names.
  SPEECH_SEGMENT_STARTED = 'SPEECH_SEGMENT_STARTED',
  SPEECH_SEGMENT_ENDED   = 'SPEECH_SEGMENT_ENDED',
  AUDIO_STREAM_END_SENT  = 'AUDIO_STREAM_END_SENT',
  VAD_FALSE_START        = 'VAD_FALSE_START',

  // ── Gemini runtime phases (mirrored for trace validation) ─────────────────
  GEMINI_FIRST_TOKEN   = 'GEMINI_FIRST_TOKEN',
  GEMINI_FIRST_AUDIO   = 'GEMINI_FIRST_AUDIO',
  GEMINI_TURN_COMPLETE = 'GEMINI_TURN_COMPLETE',
}

export interface TraceEvent {
  seq: number;
  event: PipelineEvent | string; // string to tolerate backend-only events in merged trace
  component: string;
  correlation_id: string;
  event_id: string;
  timestamp_epoch_ms: number;
  timestamp_monotonic_ns: number;
  metadata?: Record<string, any>;
}

export interface TraceSession {
  session_id: string;
  start_time_epoch_ms: number;
  end_time_epoch_ms: number;
}

export interface PipelineTrace {
  trace_version: number;
  session: TraceSession;
  events: TraceEvent[];
}
