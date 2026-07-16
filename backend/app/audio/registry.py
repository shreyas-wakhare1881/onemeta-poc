import asyncio
import logging
from typing import Dict, Optional
from .config import AudioConfig
from .processor import BaseAudioProcessor
from .telemetry import AudioTelemetry
from .pipeline import AudioPipelineManager
from .queue import AsyncAudioQueue
from .worker import AudioProcessingWorker
from .frame_builder import AudioFrameBuilder

logger = logging.getLogger("onemeta.registry")

class PipelineRegistry:
    """
    Manages active AudioPipelineManager instances per room, facilitating 
    multi-room orchestration and dependency injection.
    
    Fully thread-safe and async-safe using asyncio.Lock.
    """
    def __init__(self):
        self._pipelines: Dict[str, AudioPipelineManager] = {}
        self._lock = asyncio.Lock()

    async def create(
        self, 
        room_name: str, 
        config: AudioConfig, 
        processor: BaseAudioProcessor, 
        telemetry: AudioTelemetry
    ) -> AudioPipelineManager:
        """
        Creates and registers a new pipeline manager with injected dependencies.
        """
        async with self._lock:
            if room_name in self._pipelines:
                logger.info(f"Pipeline already registered for room: {room_name}")
                return self._pipelines[room_name]

            # Ingest dependency injections
            queue = AsyncAudioQueue(maxsize=config.queue_maxsize)
            worker = AudioProcessingWorker(queue, processor, telemetry)
            frame_builder = AudioFrameBuilder(
                target_samples=config.samples_per_frame,
                sample_rate=config.sample_rate,
                channels=config.channels
            )

            pipeline = AudioPipelineManager(
                room_name=room_name,
                config=config,
                queue=queue,
                worker=worker,
                frame_builder=frame_builder,
                telemetry=telemetry,
                processor=processor
            )

            self._pipelines[room_name] = pipeline
            logger.info(f"Successfully created and registered pipeline for room: {room_name}")
            return pipeline

    async def get(self, room_name: str) -> Optional[AudioPipelineManager]:
        """
        Retrieves the pipeline manager for a given room.
        """
        async with self._lock:
            return self._pipelines.get(room_name)

    async def exists(self, room_name: str) -> bool:
        """
        Checks if a pipeline exists for a given room.
        """
        async with self._lock:
            return room_name in self._pipelines

    async def remove(self, room_name: str) -> Optional[AudioPipelineManager]:
        """
        Deregisters a pipeline manager without tearing it down.
        """
        async with self._lock:
            pipeline = self._pipelines.pop(room_name, None)
            if pipeline:
                logger.info(f"Deregistered pipeline for room: {room_name}")
            return pipeline

    async def shutdown_all(self) -> None:
        """
        Performs clean termination of all active room pipeline tasks and workers.
        """
        async with self._lock:
            logger.info(f"Shutting down PipelineRegistry (Active: {len(self._pipelines)})...")
            rooms = list(self._pipelines.keys())
            for room_name in rooms:
                pipeline = self._pipelines.pop(room_name, None)
                if pipeline:
                    try:
                        await pipeline.cleanup()
                    except Exception as e:
                        logger.error(f"Failed to clean up pipeline for room {room_name} during shutdown: {e}")
            logger.info("PipelineRegistry shutdown complete.")

# Shared global instance
pipeline_registry = PipelineRegistry()
