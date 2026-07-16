from dataclasses import dataclass

@dataclass(frozen=True)
class AudioFrame:
    """
    Standardized, immutable data structure representing a 20ms slice of raw PCM audio.
    
    Includes nanosecond-precision relative monotonic timestamps to profile delays.
    Guarantees thread safety and prevents downstream BufferErrors by exposing immutable
    bytes at the public boundary while leveraging zero-copy memoryviews internally in the builder.
    """
    frame_id: str                 # ID format: {participant_session_id}-{sequence_number}
    sequence_number: int          # Monotonically increasing sequence number per track
    participant_identity: str     # Identity of the participant
    participant_session_id: str    # Unique connection session ID of the participant
    capture_timestamp_ns: int     # Relative monotonic capture start timestamp (ns)
    queue_timestamp_ns: int = 0   # Relative monotonic queue entry timestamp (ns)
    processing_timestamp_ns: int = 0  # Relative monotonic worker pop timestamp (ns)
    sample_rate: int = 16000      # Audio sample rate in Hz
    channels: int = 1             # Audio channels count
    frame_duration: float = 0.02  # Duration of the frame in seconds
    pcm_data: bytes = b""         # Raw PCM bytes (zero-copy internally in frame builder, immutable bytes at public boundary)
