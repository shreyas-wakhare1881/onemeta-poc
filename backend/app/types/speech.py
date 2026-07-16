from dataclasses import dataclass
from enum import Enum

class FlushReason(Enum):
    """
    Indicates the reason why a speech chunk was flushed from the builder.
    """
    SILENCE = "silence"            # Silence timeout detected
    MAX_DURATION = "max_duration"  # Max chunk length limit reached
    END_OF_STREAM = "end_of_stream" # Stream ended or explicitly flushed

@dataclass(frozen=True)
class SpeechChunkMetadata:
    """
    Strongly typed metadata tracking processing delays and chunk stats.
    """
    queue_wait_ms: float           # Estimated wait time in async queue (ms)
    processing_time_ms: float      # Estimated worker processing duration (ms)
    end_to_end_age_ms: float       # Estimated capture-to-sink latency (ms)
    flush_reason: FlushReason      # Trigger reason for chunk flush
    average_rms: float             # Average RMS energy of speech frames in chunk
    peak_rms: float                # Peak RMS energy of speech frames in chunk
    speech_ratio: float            # Ratio of speech frames to total frames (speech + silence timeout frames)

@dataclass(frozen=True)
class SpeechChunk:
    """
    Represents a continuous segment of active speech flushed from the pipeline.
    
    Serves as the immutable output contract for downstream speech processors.
    """
    chunk_id: str                  # Participant-session relative unique chunk ID
    sequence_number: int           # Chunk sequence counter
    participant_identity: str      # Speaking participant identity
    participant_session_id: str    # Participant connection session ID
    room_name: str                 # Target room name
    start_timestamp: float         # Utterance capture start time (seconds)
    end_timestamp: float           # Utterance capture end time (seconds)
    duration_ms: float             # Utterance segment duration (ms)
    frame_count: int               # Number of frames contained
    pcm_data: bytes                # Combined raw PCM bytes (references raw pcm)
    is_final: bool                 # Denotes end of speech utterance
    metadata: SpeechChunkMetadata  # Processing telemetry indicators
