from dataclasses import dataclass

@dataclass(frozen=True)
class AudioFrame:
    """
    Standardized, immutable 20ms slice of raw PCM audio — Stage 1 internal type.

    This type is used exclusively within the audio pipeline (frame_builder,
    worker, processor). It does NOT cross the Stage 1 / Stage 2 boundary.
    The Stage 1 → Stage 2 contract is StreamingAudioPacket in transport/packet.py.
    """
    frame_id: str
    sequence_number: int
    participant_identity: str
    participant_session_id: str
    capture_timestamp_ns: int
    queue_timestamp_ns: int = 0
    processing_timestamp_ns: int = 0
    sample_rate: int = 16000
    channels: int = 1
    frame_duration: float = 0.02
    pcm_data: bytes = b""
