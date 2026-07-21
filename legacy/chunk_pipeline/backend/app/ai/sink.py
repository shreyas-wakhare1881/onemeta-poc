import logging
from ..audio.sink import BaseSpeechChunkSink
from ..types.speech import SpeechChunk
from .engine import AIEngine

logger = logging.getLogger("onemeta.ai.sink")

class InferenceSink(BaseSpeechChunkSink):
    """
    Integrates the audio pipeline with the AI pipeline by acting as a SpeechChunk sink.
    Forwards SpeechChunk objects immediately to the AIEngine queue without blocking.
    """
    def __init__(self, engine: AIEngine):
        self.engine = engine

    async def start(self) -> None:
        """
        Triggers runtime initialization and worker startup in the AIEngine.
        """
        logger.info("InferenceSink starting: initializing AIEngine lifecycle...")
        await self.engine.start()
        logger.info("InferenceSink successfully started.")

    async def shutdown(self) -> None:
        """
        Triggers graceful draining and resource releases in the AIEngine.
        """
        logger.info("InferenceSink shutting down: terminating AIEngine...")
        await self.engine.shutdown()
        logger.info("InferenceSink shutdown complete.")

    async def write_chunk(self, chunk: SpeechChunk) -> None:
        """
        Receives SpeechChunks from the audio pipeline and enqueues them into the AIEngine.
        This call is completely non-blocking, ensuring the audio worker is never delayed.
        """
        # Non-blocking enqueue to isolate the audio worker
        self.engine.enqueue_chunk(chunk)
