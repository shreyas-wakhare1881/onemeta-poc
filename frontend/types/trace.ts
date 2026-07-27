export enum PipelineEvent {
  SESSION_STARTED = 'SESSION_STARTED',
  SESSION_ENDED = 'SESSION_ENDED',
  TEXT_PACKET_RECEIVED = 'TEXT_PACKET_RECEIVED',
  AUDIO_PACKET_RECEIVED = 'AUDIO_PACKET_RECEIVED',
  REACT_RENDER_COMPLETED = 'REACT_RENDER_COMPLETED',
  PCM_DECODE_STARTED = 'PCM_DECODE_STARTED',
  PCM_DECODE_COMPLETED = 'PCM_DECODE_COMPLETED',
  AUDIO_SCHEDULED = 'AUDIO_SCHEDULED',
  AUDIO_PLAYBACK_SCHEDULED = 'AUDIO_PLAYBACK_SCHEDULED'
  ,
  AUDIO_PLAYBACK_STARTED = 'AUDIO_PLAYBACK_STARTED',
  AUDIO_PLAYBACK_COMPLETED = 'AUDIO_PLAYBACK_COMPLETED',
  AUDIO_CONTEXT_STATE = 'AUDIO_CONTEXT_STATE',
  NEXT_PLAYTIME_UPDATED = 'NEXT_PLAYTIME_UPDATED'
  ,
  PLAYBACK_TRIM_APPLIED = 'PLAYBACK_TRIM_APPLIED',
  PLAYBACK_DROP_DECISION = 'PLAYBACK_DROP_DECISION'
}

export interface TraceEvent {
  seq: number;
  event: PipelineEvent;
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
