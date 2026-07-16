import asyncio
import logging
import time
from dataclasses import replace
from .queue import AsyncAudioQueue
from .processor import BaseAudioProcessor
from .telemetry import AudioTelemetry
from ..types.audio import AudioFrame

logger = logging.getLogger("onemeta.audio_worker")

class AudioProcessingWorker:
    """
    An asynchronous processing engine that pops frames from AsyncAudioQueue 
    and processes them using an injected BaseAudioProcessor.
    """
    def __init__(self, queue: AsyncAudioQueue, processor: BaseAudioProcessor, telemetry: AudioTelemetry):
        self.queue = queue
        self.processor = processor
        self.telemetry = telemetry
        self._task: asyncio.Task | None = None
        self._running = False

    def is_running(self) -> bool:
        """
        Returns True if the worker processing task is active.
        """
        return self._running and self._task is not None and not self._task.done()

    async def start(self):
        """
        Initializes the processor and starts the async run loop.
        """
        if self._running:
            return

        logger.info("Initializing processor for worker start...")
        try:
            await self.processor.initialize()
        except Exception as e:
            logger.error(f"Failed to initialize processor during worker startup: {e}")
            raise

        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("AudioProcessingWorker run loop started successfully.")

    async def shutdown(self):
        """
        Stops the worker loop, flushes/shuts down the processor, and cancels the task.
        """
        if not self._running:
            return

        logger.info("Stopping AudioProcessingWorker...")
        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        try:
            await self.processor.flush()
            await self.processor.shutdown()
        except Exception as e:
            logger.error(f"Failed to flush/shutdown audio processor cleanly: {e}")

        logger.info("AudioProcessingWorker shutdown complete.")

    async def cleanup(self):
        """
        Drains task structures and ensures complete worker shutdown.
        """
        await self.shutdown()

    async def _run_loop(self):
        while self._running:
            try:
                frame = await self.queue.get()
                try:
                    await self.on_frame(frame)
                except Exception as e:
                    await self.on_error(e)
                finally:
                    self.queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self.on_error(e)

    async def on_frame(self, frame: AudioFrame):
        """
        Executes frame processing using the injected processor, updating latency timestamps in nanoseconds.
        """
        t_start_ns = time.perf_counter_ns()
        
        # Create copy of frame with processing timestamp (immutability)
        updated_frame = replace(frame, processing_timestamp_ns=t_start_ns)
        
        # Delegate process execution
        await self.processor.process_frame(updated_frame)
        
        t_end_ns = time.perf_counter_ns()
        
        # Record processing metrics
        self.telemetry.record_processed(updated_frame, t_start_ns, t_end_ns)

    async def on_error(self, error: Exception):
        """
        Handles worker loop exceptions gracefully without crashing the loop thread.
        """
        logger.error(f"Audio processing worker loop encountered an error: {error}")
        # Pause briefly to prevent high-frequency tight loop crashes on failure
        await asyncio.sleep(0.01)
