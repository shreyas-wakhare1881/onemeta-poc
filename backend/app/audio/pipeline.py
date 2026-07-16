import logging
import time
from dataclasses import replace
from .config import AudioConfig
from .queue import AsyncAudioQueue
from .worker import AudioProcessingWorker
from .telemetry import AudioTelemetry
from .frame_builder import AudioFrameBuilder
from .processor import BaseAudioProcessor

logger = logging.getLogger("onemeta.pipeline")

class AudioPipelineManager:
    """
    Coordinates raw audio ingestion, frame building, capacity queuing, 
    and worker thread dispatching. Fully decoupled from LiveKit SDK.
    """
    def __init__(
        self,
        room_name: str,
        config: AudioConfig,
        queue: AsyncAudioQueue,
        worker: AudioProcessingWorker,
        frame_builder: AudioFrameBuilder,
        telemetry: AudioTelemetry,
        processor: BaseAudioProcessor
    ):
        self.room_name = room_name
        self.config = config
        self.queue = queue
        self.worker = worker
        self.frame_builder = frame_builder
        self.telemetry = telemetry
        self.processor = processor
        self._started = False

    async def start(self):
        """
        Starts the pipeline and initiates the background worker process.
        """
        if self._started:
            return

        logger.info(f"Starting AudioPipelineManager for room: {self.room_name}")
        await self.worker.start()
        self._started = True

    def ingest_pcm(
        self, 
        pcm_bytes: bytes, 
        timestamp: float, 
        participant_identity: str, 
        participant_session_id: str
    ):
        """
        Ingests raw PCM audio data, standardizes it into 20ms frames, 
        and schedules frames into the bounded processing queue.
        """
        if not self._started:
            logger.warning(f"PCM ingestion ignored: Pipeline for room {self.room_name} not started.")
            return

        self.telemetry.record_received()

        # Capture relative monotonic timestamp in nanoseconds
        now_ns = time.perf_counter_ns()

        # Chunk byte buffer into standardized 20ms frames using zero-copy slicing
        frames = self.frame_builder.append(
            pcm_bytes, 
            now_ns, 
            participant_identity, 
            participant_session_id
        )

        for frame in frames:
            t_queued_ns = time.perf_counter_ns()
            # Preserve immutability by copying with the queue insertion timestamp in nanoseconds
            queued_frame = replace(frame, queue_timestamp_ns=t_queued_ns)

            # Non-blocking push to the bounded queue
            success = self.queue.put_nowait(queued_frame)
            if success:
                self.telemetry.record_queued(now_ns)
            else:
                self.telemetry.record_dropped()

    async def cleanup(self):
        """
        Shuts down worker, drains queue metrics, and cleans up frame builder state.
        """
        logger.info(f"Tearing down AudioPipelineManager for room: {self.room_name}")
        if self._started:
            await self.worker.cleanup()
            self._started = False

        self.queue.clear()
        self.frame_builder.clear()
        self.telemetry.reset()
        logger.info(f"Pipeline cleanup completed for room: {self.room_name}")
