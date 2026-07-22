import type { RemoteVideoTrack, RemoteAudioTrack } from 'livekit-client';

export type RoomConnectionState = 'Disconnected' | 'Connecting' | 'Connected' | 'Disconnecting' | 'Failed';

export interface ParticipantInfo {
  identity: string;
  sid: string;
  videoTrack: RemoteVideoTrack | null;
  audioTrack: RemoteAudioTrack | null;
}

export interface MediaDeviceState {
  isCameraEnabled: boolean;
  isMicrophoneEnabled: boolean;
  cameraError: string | null;
  microphoneError: string | null;
}

// Immutable Backend EventMetrics mirror definition
export interface EventMetrics {
  start_timestamp: number;
  end_timestamp: number;
  queue_wait_ms: number;
  processing_time_ms: number;
  chunk_duration_ms: number;
  ttft_ms: number;
  gemma_latency_ms: number;
  total_ai_latency_ms: number;
  audio_duration_ms: number;
}

// Bounded protocol packet structures
export interface BasePacket {
  id: string;
  version: number;
  type: string;
  timestamp: number;
}

export interface BaseAIEventPacket extends BasePacket {
  metrics: EventMetrics;
}

export interface AIStartedPacket extends BaseAIEventPacket {
  type: 'AIStartedEvent';
  payload: {
    chunk_id: string;
    sequence_number: number;
  };
}

export interface AIPartialPacket extends BaseAIEventPacket {
  type: 'AIPartialEvent';
  payload: {
    chunk_id: string;
    sequence_number: number;
    text_delta: string;
    cumulative_text: string;
  };
}

export interface AICompletedPacket extends BaseAIEventPacket {
  type: 'AICompletedEvent';
  payload: {
    chunk_id: string;
    sequence_number: number;
    full_text: string;
    duration_ms: number;
  };
}

export interface TranslationFailedPacket extends BaseAIEventPacket {
  type: 'TranslationFailedEvent' | 'AIErrorEvent';
  payload: {
    chunk_id: string;
    sequence_number: number;
    error_message: string;
  };
}

export interface AudioTelemetryReport {
  elapsed_seconds: number;
  frames_received: number;
  frames_processed: number;
  dropped_frames: number;
  drop_rate_pct: number;
  processed_fps: number;
  throughput_bytes_per_sec: number;
  queue_depth: number;
  queue_utilization_pct: number;
  worker_busy_pct: number;
  avg_queue_wait_ms_est: number;
  max_queue_wait_ms_est: number;
  avg_processing_time_ms_est: number;
  max_processing_time_ms_est: number;
  avg_frame_age_ms_est: number;
  max_frame_age_ms_est: number;
}

export interface AITelemetryReport {
  elapsed_seconds: number;
  successful_requests: number;
  failed_requests: number;
  dropped_chunks: number;
  total_requests: number;
  inference_queue_depth: number;
  avg_queue_wait_ms: number;
  max_queue_wait_ms: number;
  avg_first_token_latency_ms: number;
  max_first_token_latency_ms: number;
  avg_gemma_latency_ms: number;
  max_gemma_latency_ms: number;
  avg_total_ai_latency_ms: number;
  max_total_ai_latency_ms: number;
  total_tokens: number;
  tokens_per_second: number;
}

export interface PublisherTelemetryReport {
  published_packets: number;
  retried_packets: number;
  dropped_packets: number;
  queue_evictions: number;
  queue_depth: number;
}

export interface TelemetryUpdatePayload {
  audio: AudioTelemetryReport;
  ai: AITelemetryReport;
  publisher: PublisherTelemetryReport;
  timestamp: number;
}

export interface TelemetryUpdatePacket extends BasePacket {
  type: 'TelemetryUpdate';
  payload: TelemetryUpdatePayload;
}

export interface StreamingPartialTranslationPacket extends BasePacket {
  type: 'StreamingPartialTranslationEvent';
  payload: {
    session_id: string;
    event_seq: number;
    text_delta: string;
    cumulative_text: string;
    correlation_id: string;
  };
}

export interface StreamingTranslationAudioPacket extends BasePacket {
  type: 'StreamingTranslationAudioEvent';
  payload: {
    session_id: string;
    event_seq: number;
    audio_data: string;
    mime_type: string;
    correlation_id: string;
    participant_identity?: string;
  };
}

export interface StreamingTranslationCompletedPacket extends BasePacket {
  type: 'StreamingTranslationCompletedEvent';
  payload: {
    session_id: string;
    event_seq: number;
    full_text: string;
    correlation_id: string;
  };
}

export interface StreamingRuntimeErrorPacket extends BasePacket {
  type: 'StreamingRuntimeErrorEvent';
  payload: {
    session_id: string;
    event_seq: number;
    error_message: string;
    correlation_id: string;
  };
}

export interface StreamingInputTranscriptionPacket extends BasePacket {
  type: 'StreamingInputTranscriptionEvent';
  payload: {
    session_id: string;
    event_seq: number;
    text_delta: string;
    cumulative_text: string;
    correlation_id: string;
  };
}

export interface StreamingInputTranscriptionCompletedPacket extends BasePacket {
  type: 'StreamingInputTranscriptionCompletedEvent';
  payload: {
    session_id: string;
    event_seq: number;
    full_text: string;
    correlation_id: string;
  };
}

export type LiveKitAIEventPacket = 
  | AIStartedPacket 
  | AIPartialPacket 
  | AICompletedPacket 
  | TranslationFailedPacket
  | StreamingPartialTranslationPacket
  | StreamingTranslationAudioPacket
  | StreamingTranslationCompletedPacket
  | StreamingRuntimeErrorPacket
  | StreamingInputTranscriptionPacket
  | StreamingInputTranscriptionCompletedPacket;


export type LiveKitPacket = 
  | LiveKitAIEventPacket
  | TelemetryUpdatePacket;
