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
    def __init__(self, config: AudioConfig, room_name: str, telemetry: AudioTelemetry, tracer: Any = None):
        self.config = config
        self.room_name = room_name
        self.telemetry = telemetry
        self.tracer = tracer

        self.vad = StreamingVADProcessor(config)
        self._initialized = False

        # Packet listeners receive each StreamingAudioPacket (Stage 1 → Stage 2)
        self._packet_listeners: List[Callable[[StreamingAudioPacket], Any]] = []

        # VAD control-plane tracking
        self._speech_active = False
        self._current_correlation_id = ""
        self._event_listeners: List[Callable[[Any], Any]] = []

        # Silence debounce counter:
        # Counts consecutive silent frames while speech is active.
        # end_user_turn() is only called after max_silence_frames consecutive
        # silent frames — preventing premature turn-end on natural inter-word pauses.
        self._silence_frame_count = 0

        # VAD observability — per-segment timing
        self._speech_start_epoch_ms: float = 0.0
        self._speech_start_mono_ns: int = 0
        self._speech_frame_count: int = 0  # speech frames in current segment

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

        try:
            # 1. Voice Activity Detection
            is_speech, rms = self.vad.is_speech(frame)

            # 2. VAD control-plane with silence debouncing.
            #
            # PREVIOUS BEHAVIOUR (BROKEN):
            #   A single 20ms silent frame fired SPEECH_ENDED immediately,
            #   calling end_user_turn() on every inter-word pause.
            #   This fragmented "Hi, how are you?" into 3-4 separate Gemini
            #   turns, each too short to produce audio output.
            #
            # NEW BEHAVIOUR:
            #   Speech STARTS on the first speech frame (unchanged — fast start).
            #   Speech ENDS only after max_silence_frames consecutive silent frames
            #   (default: 0.6s / 20ms = 30 frames = 600ms of real silence).
            #   Natural inter-word pauses (50-400ms) are absorbed silently,
            #   keeping Gemini in a single continuous turn with the full sentence.
            vad_triggered = False

            if is_speech:
                # Any speech frame resets the silence counter immediately.
                self._silence_frame_count = 0
                if not self._speech_active:
                    # First speech frame of a new segment.
                    self._speech_active = True
                    self._current_correlation_id = f"corr-{uuid.uuid4().hex[:8]}"
                    self._speech_start_epoch_ms = time.time() * 1000.0
                    self._speech_start_mono_ns = time.perf_counter_ns()
                    self._speech_frame_count = 0
                    vad_triggered = True
                    ev = StreamingSpeechStartedEvent(
                        session_id=self.room_name,
                        event_seq=0,
                        wall_timestamp=time.time(),
                        session_time_ms=0.0,
                        correlation_id=self._current_correlation_id
                    )
                    await self._emit_event(ev)
                    # Emit SPEECH_SEGMENT_STARTED for instrumentation
                    if self.tracer and self.tracer.enabled:
                        from .tracing_events import PipelineEvent
                        self.tracer.log_event(
                            PipelineEvent.SPEECH_SEGMENT_STARTED,
                            correlation_id=self._current_correlation_id,
                            metadata={
                                "speech_start_epoch_ms": self._speech_start_epoch_ms,
                                "correlation_id": self._current_correlation_id,
                                "rms_at_start": rms
                            }
                        )
                self._speech_frame_count += 1

            elif self._speech_active:
                # Silent frame while speech is active — apply debounce counter.
                self._silence_frame_count += 1
                if self._silence_frame_count >= self.config.max_silence_frames:
                    # Sustained silence confirmed: end the speech segment.
                    speech_end_epoch_ms = time.time() * 1000.0
                    speech_duration_ms = speech_end_epoch_ms - self._speech_start_epoch_ms
                    silence_duration_ms = self._silence_frame_count * (self.config.frame_duration_sec * 1000.0)

                    self._speech_active = False
                    self._silence_frame_count = 0
                    vad_triggered = True
                    ev = StreamingSpeechEndedEvent(
                        session_id=self.room_name,
                        event_seq=0,
                        wall_timestamp=time.time(),
                        session_time_ms=0.0,
                        correlation_id=self._current_correlation_id
                    )
                    await self._emit_event(ev)

                    # Emit SPEECH_SEGMENT_ENDED for VAD observability
                    if self.tracer and self.tracer.enabled:
                        from .tracing_events import PipelineEvent
                        self.tracer.log_event(
                            PipelineEvent.SPEECH_SEGMENT_ENDED,
                            correlation_id=self._current_correlation_id,
                            metadata={
                                "speech_duration_ms": round(speech_duration_ms, 2),
                                "silence_frames_elapsed": self.config.max_silence_frames,
                                "silence_duration_ms": round(silence_duration_ms, 2),
                                "total_speech_frames": self._speech_frame_count,
                                "correlation_id": self._current_correlation_id
                            }
                        )
                        # AUDIO_STREAM_END_SENT is the TURN-COMMIT ANCHOR.
                        # This is the event used as t=0 for TTFT and TTFA calculations.
                        # It fires at the same moment end_user_turn() is called on the transport.
                        self.tracer.log_event(
                            PipelineEvent.AUDIO_STREAM_END_SENT,
                            correlation_id=self._current_correlation_id,
                            metadata={
                                "speech_duration_ms": round(speech_duration_ms, 2),
                                "debounce_frames": self.config.max_silence_frames,
                                "total_speech_frames": self._speech_frame_count,
                                "correlation_id": self._current_correlation_id
                            }
                        )
                # else: transient silence — remain in speech state.
                #       Do NOT call end_user_turn(). Gemini keeps streaming.

            # Log VAD_DECISION event on transition
            if vad_triggered and self.tracer and self.tracer.enabled:
                from .tracing_events import PipelineEvent
                self.tracer.log_event(
                    PipelineEvent.VAD_DECISION,
                    correlation_id=self._current_correlation_id,
                    metadata={
                        "is_speech": self._speech_active,
                        "frame_id": frame.frame_id,
                        "rms": rms
                    }
                )

            # Log MIC_FRAME_RECEIVED event correlating the 20ms block to speech segment
            if self.tracer and self.tracer.enabled:
                from .tracing_events import PipelineEvent
                corr_id = self._current_correlation_id if self._speech_active else ""
                self.tracer.log_event(
                    PipelineEvent.MIC_FRAME_RECEIVED,
                    correlation_id=corr_id,
                    metadata={
                        "frame_id": frame.frame_id,
                        "packet_size": len(frame.pcm_data),
                        "sample_rate": frame.sample_rate,
                        "channels": frame.channels,
                        "is_speech": is_speech
                    }
                )

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
                    except Exception as le:
                        logger.error(f"Error in streaming packet listener: {le}", exc_info=True)
                        if self.tracer and self.tracer.enabled:
                            from .tracing_events import PipelineEvent
                            self.tracer.log_event(
                                PipelineEvent.PIPELINE_ERROR,
                                correlation_id=self._current_correlation_id if hasattr(self, "_current_correlation_id") else "",
                                metadata={
                                    "stage": "vad_packet_broadcast",
                                    "exception": le.__class__.__name__,
                                    "message": str(le)
                                }
                            )
        except Exception as e:
            logger.error(f"Exception during speech processor process_frame: {e}", exc_info=True)
            if self.tracer and self.tracer.enabled:
                from .tracing_events import PipelineEvent
                self.tracer.log_event(
                    PipelineEvent.PIPELINE_ERROR,
                    correlation_id=self._current_correlation_id if hasattr(self, "_current_correlation_id") else "",
                    metadata={
                        "stage": "vad_processing",
                        "exception": e.__class__.__name__,
                        "message": str(e)
                    }
                )
            raise e

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
