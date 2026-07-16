from abc import ABC, abstractmethod
import asyncio
import logging
from typing import List
from ..types.speech import SpeechChunk

logger = logging.getLogger("onemeta.sink")

class BaseSpeechChunkSink(ABC):
    """
    Abstract contract defining the destination for finalized speech chunks.
    """
    @abstractmethod
    async def write_chunk(self, chunk: SpeechChunk) -> None:
        """
        Processes a finalized SpeechChunk.
        """
        pass


class MultiChunkSink(BaseSpeechChunkSink):
    """
    Broadcasts SpeechChunks to multiple downstream sinks concurrently.
    
    Guarantees strict error isolation and pipeline self-healing:
    - Sinks are executed concurrently using asyncio.gather(..., return_exceptions=True).
    - Downstream writes are wrapped in a configurable timeout boundary.
    - Exceptions raised by a child sink do not impact other sinks.
    """
    def __init__(self, sinks: List[BaseSpeechChunkSink], timeout_sec: float = 0.5):
        self.sinks = sinks
        self.timeout_sec = timeout_sec

    async def start(self):
        """
        Recursively triggers startup routines on downstream child sinks.
        """
        for sink in self.sinks:
            if hasattr(sink, "start"):
                await sink.start()

    async def shutdown(self):
        """
        Recursively triggers shutdown routines on downstream child sinks.
        """
        for sink in self.sinks:
            if hasattr(sink, "shutdown"):
                await sink.shutdown()

    async def write_chunk(self, chunk: SpeechChunk) -> None:
        if not self.sinks:
            return

        async def _safe_write(target_sink: BaseSpeechChunkSink):
            try:
                # Wrap each child sink write in a configurable timeout to isolate slow sinks
                await asyncio.wait_for(target_sink.write_chunk(chunk), timeout=self.timeout_sec)
            except asyncio.TimeoutError:
                logger.error(
                    f"Sink timeout: '{target_sink.__class__.__name__}' took longer than {self.timeout_sec}s to process chunk {chunk.chunk_id}."
                )
            except Exception as exc:
                logger.error(
                    f"Sink failure: '{target_sink.__class__.__name__}' failed to write chunk {chunk.chunk_id}: {exc}", 
                    exc_info=True
                )

        # Dispatch concurrently to all sinks, capturing/preventing exception propagation
        await asyncio.gather(*(_safe_write(sink) for sink in self.sinks), return_exceptions=True)


class QueuedChunkSink(BaseSpeechChunkSink):
    """
    An async pipeline sink wrapper utilizing a bounded Queue.
    
    Enforces a DROP_OLDEST policy on overflow. Downstream processing writes 
    are protected by a configurable timeout wrapper.
    """
    def __init__(self, target_sink: BaseSpeechChunkSink, maxsize: int = 100, timeout_sec: float = 0.5):
        self.target_sink = target_sink
        self.maxsize = maxsize
        self.timeout_sec = timeout_sec
        self.queue = asyncio.Queue(maxsize=maxsize)
        self._task: asyncio.Task | None = None
        self._running = False
        self._overflow_count = 0

    async def start(self):
        if self._running:
            return

        if hasattr(self.target_sink, "start"):
            await self.target_sink.start()

        self._running = True
        self._task = asyncio.create_task(self._process_queue())
        logger.info(f"QueuedChunkSink consumer active (maxsize={self.maxsize}, timeout={self.timeout_sec}s).")

    async def shutdown(self):
        if not self._running:
            return
        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        if hasattr(self.target_sink, "shutdown"):
            await self.target_sink.shutdown()

        logger.info(f"QueuedChunkSink consumer stopped. Drop count: {self._overflow_count}")

    async def write_chunk(self, chunk: SpeechChunk) -> None:
        if not self._running:
            logger.warning("QueuedChunkSink ignoring write: consumer is not running.")
            return

        if self.queue.full():
            try:
                # Evict oldest element (DROP_OLDEST policy)
                self.queue.get_nowait()
                self.queue.task_done()
                self._overflow_count += 1
                logger.warning(
                    f"QueuedChunkSink full. Evicted oldest chunk (DROP_OLDEST). "
                    f"Total overflows: {self._overflow_count}"
                )
            except asyncio.QueueEmpty:
                pass

        try:
            self.queue.put_nowait(chunk)
        except asyncio.QueueFull:
            logger.error(f"Failed to queue chunk {chunk.chunk_id} in QueuedChunkSink even after eviction.")

    async def _process_queue(self):
        while self._running:
            try:
                chunk = await self.queue.get()
                try:
                    # Enforce timeout protection inside the background consumer as well
                    await asyncio.wait_for(self.target_sink.write_chunk(chunk), timeout=self.timeout_sec)
                except asyncio.TimeoutError:
                    logger.error(
                        f"QueuedChunkSink worker: target sink write timed out for chunk {chunk.chunk_id} "
                        f"({self.timeout_sec}s threshold)."
                    )
                except Exception as exc:
                    logger.error(f"Downstream sink write failed inside QueuedChunkSink: {exc}", exc_info=True)
                finally:
                    self.queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(f"Exception in QueuedChunkSink processing loop: {exc}", exc_info=True)
                await asyncio.sleep(0.01)


class LoggingChunkSink(BaseSpeechChunkSink):
    """
    Synchronous logging sink to trace processing latency on speech chunk finalizations.
    """
    async def write_chunk(self, chunk: SpeechChunk) -> None:
        logger.info(
            f"[SpeechChunk Sink] Finalized: {chunk.chunk_id} | "
            f"Seq: {chunk.sequence_number} | "
            f"Duration: {chunk.duration_ms:.1f}ms | "
            f"Frames: {chunk.frame_count} | "
            f"Final: {chunk.is_final} | "
            f"Reason: {chunk.metadata.flush_reason.value} | "
            f"Queue Delay: {chunk.metadata.queue_wait_ms:.2f}ms (Estimated) | "
            f"Total Age: {chunk.metadata.end_to_end_age_ms:.2f}ms (Estimated) | "
            f"Avg RMS: {chunk.metadata.average_rms:.1f} | "
            f"Peak RMS: {chunk.metadata.peak_rms:.1f} | "
            f"Speech Ratio: {chunk.metadata.speech_ratio * 100.0:.1f}%"
        )
