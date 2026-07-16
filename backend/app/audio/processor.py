from abc import ABC, abstractmethod
import logging
import time
from typing import List
from ..types.audio import AudioFrame
from ..types.speech import FlushReason
from .config import AudioConfig
from .vad import StreamingVADProcessor
from .chunk_builder import AdaptiveChunkBuilder
from .context_manager import StreamingContextManager
from .sink import BaseSpeechChunkSink
from .telemetry import AudioTelemetry

logger = logging.getLogger("onemeta.processor")

class BaseAudioProcessor(ABC):
    """
    Abstract contract defining setup and execution boundaries for pipeline processing stages.
    """
    @abstractmethod
    async def initialize(self) -> None:
        pass

    @abstractmethod
    async def process_frame(self, frame: AudioFrame) -> None:
        pass

    @abstractmethod
    async def flush(self) -> None:
        pass

    @abstractmethod
    async def shutdown(self) -> None:
        pass


class DefaultAudioProcessor(BaseAudioProcessor):
    """
    Default passthrough processor for testing raw pipeline ingestion flows.
    """
    def __init__(self):
        self._initialized = False

    async def initialize(self) -> None:
        self._initialized = True
        logger.info("DefaultAudioProcessor initialized.")

    async def process_frame(self, frame: AudioFrame) -> None:
        if not self._initialized:
            raise RuntimeError("DefaultAudioProcessor must be initialized.")
        pass

    async def flush(self) -> None:
        pass

    async def shutdown(self) -> None:
        self._initialized = False
        logger.info("DefaultAudioProcessor shut down successfully.")


class StreamingSpeechProcessor(BaseAudioProcessor):
    """
    Orchestrates the core DSP pipeline chain:
    AudioFrame -> VAD -> ChunkBuilder -> ContextManager -> SpeechChunkSink.
    
    Dynamically tracks frame RMS values processed during the active chunk
    to compute chunk telemetry statistics without double-calculating arrays.
    """
    def __init__(self, config: AudioConfig, room_name: str, sink: BaseSpeechChunkSink, telemetry: AudioTelemetry):
        self.config = config
        self.room_name = room_name
        self.sink = sink
        self.telemetry = telemetry
        
        self.vad = StreamingVADProcessor(config)
        self.builder = AdaptiveChunkBuilder(config)
        self.context_manager = StreamingContextManager(room_name, config)
        self._rms_values: List[float] = []
        self._initialized = False

    async def initialize(self) -> None:
        self._initialized = True
        
        # Propagate start signal to downstream sinks (e.g. QueuedChunkSink consumer tasks)
        if hasattr(self.sink, "start"):
            await self.sink.start()
            
        logger.info(f"StreamingSpeechProcessor initialized for room: {self.room_name}")

    async def process_frame(self, frame: AudioFrame) -> None:
        if not self._initialized:
            raise RuntimeError("StreamingSpeechProcessor must be initialized before processing.")

        t_start_ns = time.perf_counter_ns()

        # 1. Voice Activity Detection (float32 RMS energy calculation with dual-threshold hysteresis VAD)
        is_speech, rms = self.vad.is_speech(frame)

        # Track RMS for active speech frames to compile chunk stats on flush
        if is_speech:
            self._rms_values.append(rms)

        # 2. Accumulate in AdaptiveChunkBuilder (deque buffers, silence frames isolated)
        flush_result = self.builder.add_frame(frame, is_speech)

        # 3. If a flush occurred, compile chunk and deliver to MultiSink
        if flush_result:
            frames, reason, silence_count = flush_result
            
            # Calculate RMS metrics
            average_rms = sum(self._rms_values) / len(self._rms_values) if self._rms_values else 0.0
            peak_rms = max(self._rms_values) if self._rms_values else 0.0
            self._rms_values.clear()

            # Telemetry generates strongly-typed metadata (Separation of Concerns)
            metadata = self.telemetry.create_chunk_metadata(
                frames_iterable=frames, 
                reason=reason, 
                t_start_ns=t_start_ns,
                silence_count=silence_count,
                average_rms=average_rms,
                peak_rms=peak_rms
            )
            # Pure context manager transforms frames list into SpeechChunk
            chunk = self.context_manager.build_chunk(frames, metadata)
            await self.sink.write_chunk(chunk)

    async def flush(self) -> None:
        """
        Forces flushing of any buffered speech frames.
        """
        if not self._initialized:
            return

        t_start_ns = time.perf_counter_ns()
        flush_result = self.builder.flush(FlushReason.END_OF_STREAM)
        if flush_result:
            frames, reason, silence_count = flush_result
            
            # Calculate RMS metrics
            average_rms = sum(self._rms_values) / len(self._rms_values) if self._rms_values else 0.0
            peak_rms = max(self._rms_values) if self._rms_values else 0.0
            self._rms_values.clear()

            metadata = self.telemetry.create_chunk_metadata(
                frames_iterable=frames, 
                reason=reason, 
                t_start_ns=t_start_ns,
                silence_count=silence_count,
                average_rms=average_rms,
                peak_rms=peak_rms
            )
            chunk = self.context_manager.build_chunk(frames, metadata)
            await self.sink.write_chunk(chunk)

    async def shutdown(self) -> None:
        """
        Flushes buffers and safely shuts down downstream sink processes.
        """
        await self.flush()
        
        if hasattr(self.sink, "shutdown"):
            await self.sink.shutdown()
            
        self._initialized = False
        logger.info(f"StreamingSpeechProcessor shut down cleanly for room: {self.room_name}")
