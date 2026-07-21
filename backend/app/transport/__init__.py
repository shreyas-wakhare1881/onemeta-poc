"""
transport — Stage 1 ↔ Stage 2 contract layer.

Defines transport-neutral packet types that carry audio data between the
Stage 1 audio pipeline and Stage 2 AI runtime. Neither audio/ nor ai/
owns these — they are the shared contract between them.
"""
from .packet import StreamingAudioPacket, StreamingPacketMetadata

__all__ = ["StreamingAudioPacket", "StreamingPacketMetadata"]
