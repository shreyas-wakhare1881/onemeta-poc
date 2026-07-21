import asyncio
import logging
from typing import Optional

from ..transport.packet import StreamingAudioPacket
from .config import AIConfig
from .pipeline_streaming import StreamingInferencePipeline
from .streaming import BaseStreamingRuntime, StreamingSession

logger = logging.getLogger("onemeta.ai.engine")

class AIEngine:
    """
    Thin facade over the streaming inference pipeline.
    Provider-agnostic: accepts any BaseStreamingRuntime implementation.
    AIEngine has no knowledge of GeminiLiveRuntime or any other provider directly.
    """
    def __init__(self, config: AIConfig):
        self.config = config
        self._streaming_pipeline = StreamingInferencePipeline()

    async def start(self) -> None:
        """
        Launches the streaming pipeline.
        """
        logger.info("Starting AIEngine (streaming-only)...")
        await self._streaming_pipeline.start()
        logger.info("AIEngine started successfully.")

    async def shutdown(self) -> None:
        """
        Gracefully terminates streaming sessions and releases resources.
        """
        logger.info("Shutting down AIEngine...")
        await self._streaming_pipeline.shutdown()
        logger.info("AIEngine shutdown complete.")

    async def start_streaming_session(
        self,
        session_id: str,
        runtime: BaseStreamingRuntime,
        source_lang: str,
        target_lang: str,
        queue_maxsize: int = 100,
        metadata: dict = None
    ) -> StreamingSession:
        """
        Creates and starts a persistent streaming session backed by the given runtime.
        """
        return await self._streaming_pipeline.start_streaming_session(
            session_id=session_id,
            runtime=runtime,
            source_lang=source_lang,
            target_lang=target_lang,
            queue_maxsize=queue_maxsize,
            metadata=metadata
        )

    async def stop_streaming_session(self, session_id: str) -> None:
        """
        Closes a streaming session by ID.
        """
        await self._streaming_pipeline.stop_streaming_session(session_id)

    async def get_streaming_session(self, session_id: str) -> Optional[StreamingSession]:
        """
        Retrieves an active session by ID.
        """
        return await self._streaming_pipeline.get_streaming_session(session_id)

    async def process_audio_packet(self, session_id: str, packet: StreamingAudioPacket) -> None:
        """
        Routes a transport-neutral packet to its targeted streaming session.
        """
        await self._streaming_pipeline.process_audio_packet(session_id, packet)
