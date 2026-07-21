import asyncio
import logging
from typing import Optional, List

from ..transport.packet import StreamingAudioPacket
from .streaming import StreamingSessionManager, BaseStreamingRuntime, StreamingSession

logger = logging.getLogger("onemeta.ai.pipeline_streaming")

class StreamingInferencePipeline:
    """
    Handles persistent isolated streaming sessions (Audio Frames -> Streaming Session -> Streaming Runtime).
    Decoupled from chunk inference.
    """
    def __init__(self):
        self.streaming_manager = StreamingSessionManager()

    async def start(self) -> None:
        pass

    async def shutdown(self) -> None:
        """
        Closes all active sessions on shutdown.
        """
        session_ids = await self.streaming_manager.list_sessions()
        for sid in session_ids:
            try:
                await self.streaming_manager.close_session(sid)
            except Exception as e:
                logger.error(f"Error closing streaming session {sid} on pipeline shutdown: {e}")

    async def start_streaming_session(
        self,
        session_id: str,
        runtime: BaseStreamingRuntime,
        source_lang: str,
        target_lang: str,
        queue_maxsize: int = 100,
        metadata: dict = None
    ) -> StreamingSession:
        return await self.streaming_manager.create_session(
            session_id=session_id,
            runtime=runtime,
            source_language=source_lang,
            target_language=target_lang,
            queue_maxsize=queue_maxsize,
            metadata=metadata
        )

    async def stop_streaming_session(self, session_id: str) -> None:
        await self.streaming_manager.close_session(session_id)

    async def get_streaming_session(self, session_id: str) -> Optional[StreamingSession]:
        return await self.streaming_manager.get_session(session_id)

    async def process_audio_packet(self, session_id: str, packet: StreamingAudioPacket) -> None:
        session = await self.streaming_manager.get_session(session_id)
        if session:
            await session.send_audio(packet)
        else:
            logger.warning(f"Failed to route audio packet: Session {session_id} not found.")
