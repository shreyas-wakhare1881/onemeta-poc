from abc import ABC, abstractmethod
import asyncio
import logging
import time
import uuid
from typing import List, Callable, Any

from ..types.audio import AudioFrame
from ..transport.packet import StreamingAudioPacket, StreamingPacketMetadata
from .config import AudioConfig
from .vad import StreamingVADProcessor
from .telemetry import AudioTelemetry
from ..ai.events import StreamingSpeechStartedEvent, StreamingSpeechEndedEvent

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

    async def flush(self) -> None:
        pass

    async def shutdown(self) -> None:
        self._initialized = False
        logger.info("DefaultAudioProcessor shut down successfully.")


class StreamingSpeechProcessor(BaseAudioProcessor):
    """
    Orchestrates the streaming audio pipeline:
      AudioFrame → VAD → StreamingAudioPacket → registered packet listeners

    Emits VAD control-plane events (StreamingSpeechStartedEvent,
    StreamingSpeechEndedEvent) to registered event listeners so the
    streaming session can signal turn completion to the runtime.

    Chunk assembly has been removed in Phase 4C. See legacy/chunk_pipeline/ for historical reference.
    """
    def __init__(self, config: AudioConfig, room_name: str, telemetry: AudioTelemetry):
        self.config = config
        self.room_name = room_name
        self.telemetry = telemetry

        self.vad = StreamingVADProcessor(config)
        self._initialized = False

        # Packet listeners receive each StreamingAudioPacket (Stage 1 → Stage 2)
        self._packet_listeners: List[Callable[[StreamingAudioPacket], Any]] = []

        # VAD control-plane tracking
        self._speech_active = False
        self._current_correlation_id = ""
        self._event_listeners: List[Callable[[Any], Any]] = []

    # ------------------------------------------------------------------
    # Listener registration
    # ------------------------------------------------------------------

    def register_packet_listener(self, listener: Callable[[StreamingAudioPacket], Any]) -> None:
        if listener not in self._packet_listeners:
            self._packet_listeners.append(listener)

    def unregister_packet_listener(self, listener: Callable[[StreamingAudioPacket], Any]) -> None:
        if listener in self._packet_listeners:
            self._packet_listeners.remove(listener)

    def register_listener(self, listener: Callable[[Any], Any]) -> None:
        if listener not in self._event_listeners:
            self._event_listeners.append(listener)

    def unregister_listener(self, listener: Callable[[Any], Any]) -> None:
        if listener in self._event_listeners:
            self._event_listeners.remove(listener)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        self._initialized = True
        logger.info(f"StreamingSpeechProcessor initialized for room: {self.room_name}")

    async def process_frame(self, frame: AudioFrame) -> None:
        if not self._initialized:
            raise RuntimeError("StreamingSpeechProcessor must be initialized before processing.")

        # 1. Voice Activity Detection
        is_speech, rms = self.vad.is_speech(frame)

        # 2. VAD control-plane: emit speech start / end events
        if is_speech and not self._speech_active:
            self._speech_active = True
            self._current_correlation_id = f"corr-{uuid.uuid4().hex[:8]}"
            ev = StreamingSpeechStartedEvent(
                session_id=self.room_name,
                event_seq=0,
                wall_timestamp=time.time(),
                session_time_ms=0.0,
                correlation_id=self._current_correlation_id
            )
            await self._emit_event(ev)

        elif not is_speech and self._speech_active:
            self._speech_active = False
            ev = StreamingSpeechEndedEvent(
                session_id=self.room_name,
                event_seq=0,
                wall_timestamp=time.time(),
                session_time_ms=0.0,
                correlation_id=self._current_correlation_id
            )
            await self._emit_event(ev)

        # 3. Build and broadcast StreamingAudioPacket to all listeners (Stage 1 → Stage 2)
        if self._packet_listeners:
            packet_metadata = StreamingPacketMetadata(
                frame_id=frame.frame_id,
                participant_identity=frame.participant_identity,
                participant_session_id=frame.participant_session_id,
                rms=rms,
                correlation_id=self._current_correlation_id if self._speech_active else ""
            )
            packet = StreamingAudioPacket(
                pcm_data=memoryview(frame.pcm_data),
                sample_rate=frame.sample_rate,
                channels=frame.channels,
                capture_timestamp_ns=frame.capture_timestamp_ns,
                sequence_number=frame.sequence_number,
                is_speech=is_speech,
                metadata=packet_metadata
            )
            for listener in self._packet_listeners:
                try:
                    if asyncio.iscoroutinefunction(listener):
                        await listener(packet)
                    else:
                        listener(packet)
                except Exception as e:
                    logger.error(f"Error in streaming packet listener: {e}", exc_info=True)

    async def flush(self) -> None:
        """No-op in streaming mode — no buffered chunk state to flush."""
        pass

    async def shutdown(self) -> None:
        """
        Gracefully shuts down the processor.
        """
        self._initialized = False
        logger.info(f"StreamingSpeechProcessor shut down cleanly for room: {self.room_name}")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _emit_event(self, event: Any) -> None:
        for listener in self._event_listeners:
            try:
                if asyncio.iscoroutinefunction(listener):
                    asyncio.create_task(listener(event))
                else:
                    listener(event)
            except Exception as e:
                logger.error(f"Error in VAD event listener: {e}")
