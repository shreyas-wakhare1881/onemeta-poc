from dataclasses import dataclass

@dataclass(frozen=True)
class StreamingPacketMetadata:
    """
    Strongly-typed metadata for a StreamingAudioPacket.
    Carries participant and correlation tracking data.
    """
    frame_id: str
    participant_identity: str
    participant_session_id: str
    rms: float
    correlation_id: str = ""


@dataclass(frozen=True)
class StreamingAudioPacket:
    """
    Transport-neutral packet representing a raw audio frame slice.

    This is the Stage 1 → Stage 2 contract. It is provider-agnostic:
    any StreamingRuntime implementation receives this packet and handles
    encoding/transmission details internally.

    Uses memoryview for zero-copy reference from the audio pipeline.
    """
    pcm_data: memoryview
    sample_rate: int
    channels: int
    capture_timestamp_ns: int
    sequence_number: int
    is_speech: bool
    metadata: StreamingPacketMetadata
