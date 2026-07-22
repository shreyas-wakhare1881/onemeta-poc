import asyncio
import logging
import time
import collections
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List, Callable, Any, Optional, Union
from dataclasses import replace

from ..transport.packet import StreamingAudioPacket
from .events import (
    StreamingSessionStartedEvent,
    StreamingAudioFrameReceivedEvent,
    StreamingSpeechStartedEvent,
    StreamingSpeechEndedEvent,
    StreamingPartialTranslationEvent,
    StreamingTranslationCompletedEvent,
    StreamingSessionClosedEvent,
    StreamingBackpressureEvent,
    StreamingStateChangedEvent,
    StreamingRuntimeErrorEvent,
)

logger = logging.getLogger("onemeta.ai.streaming")

class StreamingSessionState(Enum):
    CREATED = "CREATED"
    INITIALIZING = "INITIALIZING"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    READY = "READY"
    STREAMING = "STREAMING"
    RECONNECTING = "RECONNECTING"
    FLUSHING = "FLUSHING"
    FINISHED = "FINISHED"
    CLOSED = "CLOSED"
    FAILED = "FAILED"

class BaseStreamingTransport(ABC):
    """
    Abstract Base Class for provider-specific low-level streaming transport connections.
    Decoupled from high-level orchestrator and session management.
    """
    @abstractmethod
    async def send_packet(self, packet: StreamingAudioPacket) -> None:
        """
        Sends a single transport-neutral audio packet to the provider streaming endpoint.
        """
        pass

    @abstractmethod
    async def receive_event(self) -> Any:
        """
        Pulls the next event from the transport stream.
        Should return None or raise StopAsyncIteration when connection closes.
        """
        pass

    @abstractmethod
    async def cancel_generation(self) -> None:
        """
        Cancels active generation/inference on the model backend (useful for barge-in).
        """
        pass

    @abstractmethod
    async def end_user_turn(self) -> None:
        """
        Signals to the provider that the current user speech segment has ended.
        """
        pass

    @abstractmethod
    async def close(self) -> None:
        """
        Closes/terminates the provider connection.
        """
        pass

class BaseStreamingRuntime(ABC):
    """
    Abstract Base Class representing a persistent streaming runtime engine provider.
    """
    @abstractmethod
    async def initialize(self) -> None:
        """
        Initializes runtime backend assets/verifications.
        """
        pass

    @abstractmethod
    async def is_ready(self) -> bool:
        """
        Checks operational readiness.
        """
        pass

    @abstractmethod
    async def connect(
        self,
        session_id: str,
        source_language: str,
        target_language: str,
        on_event: Callable[[Any], Any],
        metadata: dict = None
    ) -> BaseStreamingTransport:
        """
        Establishes a persistent transport connection for a session.
        Accepts on_event callback for backward compatibility or push-based runtime connections.
        """
        pass

    @abstractmethod
    async def shutdown(self) -> None:
        """
        Teardown routine for cleanup.
        """
        pass

from dataclasses import dataclass

@dataclass
class StreamingSessionMetrics:
    """
    Strongly-typed metadata tracking performance metrics for a streaming session.
    """
    session_duration_sec: float = 0.0
    received_frames: int = 0
    processed_frames: int = 0
    dropped_frames: int = 0
    avg_frame_delay_ms: float = 0.0
    queue_wait_ms: float = 0.0
    runtime_processing_delay_ms: float = 0.0
    first_token_latency_ms: float = 0.0
    final_response_latency_ms: float = 0.0
    total_session_latency_ms: float = 0.0

class SessionStateMachine:
    """
    Manages session state transitions and validation constraints.
    """
    def __init__(self, session_id: str, on_transition: Callable[[StreamingSessionState, StreamingSessionState], Any]):
        self.session_id = session_id
        self.state = StreamingSessionState.CREATED
        self._on_transition = on_transition

    async def transition_to(self, new_state: StreamingSessionState) -> None:
        old_state = self.state
        if old_state == new_state:
            return

        if old_state in (StreamingSessionState.CLOSED, StreamingSessionState.FAILED) and new_state not in (StreamingSessionState.CLOSED, StreamingSessionState.FAILED):
            logger.warning(f"Session {self.session_id}: Cannot transition from terminal state {old_state.name} to {new_state.name}")
            return

        self.state = new_state
        await self._on_transition(old_state, new_state)

class SessionMetricsCollector:
    """
    Tracks and compiles latency telemetry and packet statistics.
    """
    def __init__(self, start_time_ns: int):
        self._start_time_ns = start_time_ns
        self.received_frames = 0
        self.processed_frames = 0
        self.dropped_frames = 0
        
        self.total_frame_delay_ms = 0.0
        self.total_queue_wait_ms = 0.0
        self.total_runtime_processing_delay_ms = 0.0
        
        self.first_token_latency_ms: Optional[float] = None
        self.final_response_latency_ms: Optional[float] = None
        self.speech_start_time: Optional[float] = None

    def record_received(self, frame_delay_ms: float) -> None:
        self.received_frames += 1
        self.total_frame_delay_ms += frame_delay_ms

    def record_processed(self, runtime_delay_ms: float, queue_wait_ms: float) -> None:
        self.processed_frames += 1
        self.total_runtime_processing_delay_ms += runtime_delay_ms
        self.total_queue_wait_ms += queue_wait_ms

    def record_dropped(self) -> None:
        self.dropped_frames += 1

    def record_speech_start(self) -> None:
        self.speech_start_time = time.perf_counter()

    def record_partial(self) -> None:
        if self.first_token_latency_ms is None and self.speech_start_time is not None:
            self.first_token_latency_ms = (time.perf_counter() - self.speech_start_time) * 1000.0

    def record_completed(self) -> None:
        if self.speech_start_time is not None:
            self.final_response_latency_ms = (time.perf_counter() - self.speech_start_time) * 1000.0

    def get_metrics(self) -> StreamingSessionMetrics:
        duration_sec = (time.perf_counter_ns() - self._start_time_ns) / 1_000_000_000.0
        avg_frame_delay = self.total_frame_delay_ms / self.received_frames if self.received_frames > 0 else 0.0
        avg_queue_wait = self.total_queue_wait_ms / self.processed_frames if self.processed_frames > 0 else 0.0
        avg_processing_delay = self.total_runtime_processing_delay_ms / self.processed_frames if self.processed_frames > 0 else 0.0

        return StreamingSessionMetrics(
            session_duration_sec=duration_sec,
            received_frames=self.received_frames,
            processed_frames=self.processed_frames,
            dropped_frames=self.dropped_frames,
            avg_frame_delay_ms=avg_frame_delay,
            queue_wait_ms=avg_queue_wait,
            runtime_processing_delay_ms=avg_processing_delay,
            first_token_latency_ms=self.first_token_latency_ms or 0.0,
            final_response_latency_ms=self.final_response_latency_ms or 0.0,
            total_session_latency_ms=duration_sec * 1000.0
        )

@dataclass(frozen=True)
class SessionQueueItem:
    packet: StreamingAudioPacket
    enqueue_ns: int

class SessionPacketQueue:
    """
    Custom bounded packet queue with oldest non-speech eviction.
    Protects speech frames from deletion to preserve speech segment boundaries.
    """
    def __init__(self, maxsize: int):
        self.maxsize = maxsize
        self._queue = collections.deque()
        self._cond = asyncio.Condition()

    async def put(self, item: SessionQueueItem) -> Optional[SessionQueueItem]:
        """
        Puts an item in the queue.
        If queue is full, attempts to evict the oldest non-speech frame.
        If all frames are speech, returns the incoming frame to denote a drop.
        """
        async with self._cond:
            if len(self._queue) >= self.maxsize:
                evicted_idx = -1
                for idx, q_item in enumerate(self._queue):
                    if not q_item.packet.is_speech:
                        evicted_idx = idx
                        break

                if evicted_idx != -1:
                    evicted = self._queue[evicted_idx]
                    del self._queue[evicted_idx]
                    self._queue.append(item)
                    self._cond.notify_all()
                    return evicted
                else:
                    return item

            self._queue.append(item)
            self._cond.notify_all()
            return None

    async def get(self) -> SessionQueueItem:
        async with self._cond:
            while not self._queue:
                await self._cond.wait()
            item = self._queue.popleft()
            self._cond.notify_all()
            return item

    def qsize(self) -> int:
        return len(self._queue)

    def clear(self) -> List[SessionQueueItem]:
        items = list(self._queue)
        self._queue.clear()
        return items

class SessionWorker:
    """
    Background worker loop pulling packets and forwarding to transport.
    """
    def __init__(
        self,
        session_id: str,
        queue: SessionPacketQueue,
        metrics_collector: SessionMetricsCollector,
        state_machine: SessionStateMachine,
        get_transport: Callable[[], Optional[BaseStreamingTransport]],
        on_packet_processed: Callable[[], Any],
        on_error: Callable[[Exception], Any]
    ):
        self.session_id = session_id
        self.queue = queue
        self.metrics_collector = metrics_collector
        self.state_machine = state_machine
        self.get_transport = get_transport
        self.on_packet_processed = on_packet_processed
        self.on_error = on_error
        self.worker_task: Optional[asyncio.Task] = None

    def start(self) -> None:
        self.worker_task = asyncio.create_task(self._worker_loop())

    def stop(self) -> None:
        if self.worker_task:
            self.worker_task.cancel()
            self.worker_task = None

    async def _worker_loop(self) -> None:
        while self.state_machine.state in (
            StreamingSessionState.READY,
            StreamingSessionState.STREAMING,
            StreamingSessionState.CONNECTED
        ):
            try:
                item = await self.queue.get()
                
                if self.state_machine.state == StreamingSessionState.READY:
                    await self.state_machine.transition_to(StreamingSessionState.STREAMING)

                t_start = time.perf_counter()
                transport = self.get_transport()
                if transport:
                    await transport.send_packet(item.packet)
                duration_ms = (time.perf_counter() - t_start) * 1000.0

                queue_wait_ms = (time.perf_counter_ns() - item.enqueue_ns) / 1_000_000.0
                self.metrics_collector.record_processed(duration_ms, queue_wait_ms)
                self.on_packet_processed()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in SessionWorker {self.session_id}: {e}", exc_info=True)
                self.on_error(e)
                break

class StreamingSession:
    """
    Provider-agnostic high-level streaming session coordinator.
    Aggregates metrics, workers, and transports. Exposes state machine boundaries.
    """
    def __init__(
        self,
        session_id: str,
        runtime: BaseStreamingRuntime,
        source_language: str,
        target_language: str,
        queue_maxsize: int = 100,
        metadata: dict = None
    ):
        self.session_id = session_id
        self.runtime = runtime
        self.source_language = source_language
        self.target_language = target_language
        self.metadata = metadata or {}
        
        self._start_time_ns = time.perf_counter_ns()
        self.transport: Optional[BaseStreamingTransport] = None
        
        # State machine
        self._state_machine = SessionStateMachine(session_id, self._on_state_transition)
        
        # Metrics collector
        self._metrics_collector = SessionMetricsCollector(self._start_time_ns)
        
        # Bounded custom queue
        self._queue = SessionPacketQueue(queue_maxsize)
        
        # Event Queuing & Single Dispatcher Thread Task
        self._event_queue = asyncio.Queue()
        self._dispatcher_task: Optional[asyncio.Task] = None
        self._reader_task: Optional[asyncio.Task] = None
        
        # Worker helper
        self._worker = SessionWorker(
            session_id=session_id,
            queue=self._queue,
            metrics_collector=self._metrics_collector,
            state_machine=self._state_machine,
            get_transport=lambda: self.transport,
            on_packet_processed=lambda: None,
            on_error=self._handle_worker_error
        )
        
        self._event_seq_counter = 0
        self._listeners: List[Callable[[Any], Any]] = []
        self._cumulative_text = ""
        self._correlation_to_participant = {}
        self._correlation_to_capture_start_ns = {}

    @property
    def state(self) -> StreamingSessionState:
        return self._state_machine.state

    def get_session_time_ms(self) -> float:
        return (time.perf_counter_ns() - self._start_time_ns) / 1_000_000.0

    def _next_event_seq(self) -> int:
        self._event_seq_counter += 1
        return self._event_seq_counter

    def register_listener(self, listener: Callable[[Any], Any]) -> None:
        if listener not in self._listeners:
            self._listeners.append(listener)

    def unregister_listener(self, listener: Callable[[Any], Any]) -> None:
        if listener in self._listeners:
            self._listeners.remove(listener)

    async def _emit(self, event: Any) -> None:
        for listener in self._listeners:
            try:
                if asyncio.iscoroutinefunction(listener):
                    await listener(event)
                else:
                    listener(event)
            except Exception as e:
                logger.error(f"Error executing streaming listener callback: {e}", exc_info=True)

    async def start(self) -> None:
        """
        Initializes connection state.
        """
        if self.state != StreamingSessionState.CREATED:
            logger.warning(f"Cannot start session {self.session_id} in state {self.state.name}")
            return

        await self._state_machine.transition_to(StreamingSessionState.INITIALIZING)
        
        # Start dispatcher task immediately
        self._dispatcher_task = asyncio.create_task(self._event_dispatcher_loop())
        
        try:
            await self._state_machine.transition_to(StreamingSessionState.CONNECTING)
            self.transport = await self.runtime.connect(
                session_id=self.session_id,
                source_language=self.source_language,
                target_language=self.target_language,
                on_event=self._handle_callback_event,
                metadata=self.metadata
            )
            await self._state_machine.transition_to(StreamingSessionState.CONNECTED)
            await self._state_machine.transition_to(StreamingSessionState.READY)
            
            # Start queue worker and reader loops
            self._worker.start()
            self._reader_task = asyncio.create_task(self._event_reader_loop())
            
            logger.info(f"StreamingSession {self.session_id} started successfully.")
        except Exception as e:
            import traceback
            logger.error(
                f"Failed to start session {self.session_id}, transitioning to FAILED.\n"
                f"Exception: {type(e).__name__}: {e}\n"
                f"Traceback:\n{''.join(traceback.format_exception(type(e), e, e.__traceback__))}",
                exc_info=True
            )
            await self._state_machine.transition_to(StreamingSessionState.FAILED)
            raise

    async def reconnect(self) -> None:
        """
        Triggers transport level reconnection.
        """
        if self.state == StreamingSessionState.CLOSED:
            return

        logger.info(f"Reconnecting session {self.session_id}...")
        await self._state_machine.transition_to(StreamingSessionState.RECONNECTING)
        
        # Suspend worker and reader tasks
        self._worker.stop()
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
            self._reader_task = None

        # Clean/drain packets queue during reconnect to avoid playing old/stale audio
        drained = self._queue.clear()
        for _ in drained:
            self._metrics_collector.record_dropped()

        # Clean transport
        if self.transport:
            try:
                await self.transport.close()
            except Exception:
                pass
            self.transport = None

        try:
            await self._state_machine.transition_to(StreamingSessionState.CONNECTING)
            self.transport = await self.runtime.connect(
                session_id=self.session_id,
                source_language=self.source_language,
                target_language=self.target_language,
                on_event=self._handle_callback_event,
                metadata=self.metadata
            )
            await self._state_machine.transition_to(StreamingSessionState.CONNECTED)
            await self._state_machine.transition_to(StreamingSessionState.READY)
            
            # Resume worker and reader tasks
            self._worker.start()
            self._reader_task = asyncio.create_task(self._event_reader_loop())
            logger.info(f"Session {self.session_id} reconnected successfully.")
        except Exception as e:
            import traceback
            logger.error(
                f"Reconnection failed for session {self.session_id}, transitioning to FAILED.\n"
                f"Exception: {type(e).__name__}: {e}\n"
                f"Traceback:\n{''.join(traceback.format_exception(type(e), e, e.__traceback__))}",
                exc_info=True
            )
            await self._state_machine.transition_to(StreamingSessionState.FAILED)

    async def cancel_generation(self) -> None:
        """
        Instructs the active transport to cancel current generation outputs.
        """
        self._cumulative_text = ""
        if self.transport:
            await self.transport.cancel_generation()

    async def send_audio(self, packet: StreamingAudioPacket) -> None:
        """
        Appends a transport-neutral packet to the isolated packet queue.
        Calculates delay metrics and applies speech-aware backpressure.
        """
        if self.state not in (StreamingSessionState.READY, StreamingSessionState.STREAMING):
            logger.warning(f"Session {self.session_id} ignored packet: state={self.state.name}")
            return

        now_ns = time.perf_counter_ns()
        frame_delay_ms = (now_ns - packet.capture_timestamp_ns) / 1_000_000.0
        self._metrics_collector.record_received(frame_delay_ms)

        # Record chunk timing (Suggestion 5)
        if not hasattr(self, "_last_packet_time"):
            self._last_packet_time = time.perf_counter()
        now = time.perf_counter()
        gap_ms = (now - self._last_packet_time) * 1000.0
        self._last_packet_time = now
        logger.info(f"[Audio Ingest Timing] Packet seq={packet.sequence_number} size={len(packet.pcm_data)} bytes gap={gap_ms:.1f} ms")

        # Track correlation ID to participant identity and capture start time
        corr_id = packet.metadata.correlation_id
        if corr_id:
            self._correlation_to_participant[corr_id] = packet.metadata.participant_identity
            if corr_id not in self._correlation_to_capture_start_ns:
                self._correlation_to_capture_start_ns[corr_id] = packet.capture_timestamp_ns

        # Emit frame received event
        await self._emit(StreamingAudioFrameReceivedEvent(
            session_id=self.session_id,
            event_seq=self._next_event_seq(),
            wall_timestamp=time.time(),
            session_time_ms=self.get_session_time_ms(),
            sequence_number=packet.sequence_number,
            is_speech=packet.is_speech,
            frame_delay_ms=frame_delay_ms
        ))

        # Backpressure eviction check
        queue_item = SessionQueueItem(packet=packet, enqueue_ns=now_ns)
        dropped_item = await self._queue.put(queue_item)

        if dropped_item is not None:
            self._metrics_collector.record_dropped()
            policy = "DROP_OLDEST" if dropped_item is not queue_item else "DROP_NEWEST"
            
            logger.warning(
                f"Backpressure: Session {self.session_id} queue full. "
                f"Evicted packet seq={dropped_item.packet.sequence_number} (is_speech={dropped_item.packet.is_speech}) under {policy} policy."
            )
            await self._emit(StreamingBackpressureEvent(
                session_id=self.session_id,
                event_seq=self._next_event_seq(),
                wall_timestamp=time.time(),
                session_time_ms=self.get_session_time_ms(),
                queue_depth=self._queue.qsize(),
                policy=policy
            ))

    def _handle_callback_event(self, event: Any) -> None:
        """
        Receives events from push-based/callback runtime connections and routes to the queue.
        """
        self._event_queue.put_nowait(event)

    def record_speech_start(self) -> None:
        """
        API Boundary called by Stage 1 Session Controller to note that speaking has begun.
        """
        self._metrics_collector.record_speech_start()

    def record_speech_end(self) -> None:
        """
        API Boundary called by Stage 1 Session Controller to note that speaking has ended.
        Signals the end of the user's turn to the transport to trigger generation.
        """
        if self.transport and hasattr(self.transport, "end_user_turn"):
            asyncio.create_task(self.transport.end_user_turn())

    async def _on_state_transition(self, old_state: StreamingSessionState, new_state: StreamingSessionState) -> None:
        await self._emit(StreamingStateChangedEvent(
            session_id=self.session_id,
            event_seq=self._next_event_seq(),
            wall_timestamp=time.time(),
            session_time_ms=self.get_session_time_ms(),
            old_state=old_state.name,
            new_state=new_state.name
        ))

    def _handle_worker_error(self, exc: Exception) -> None:
        import traceback
        logger.error(
            f"Session {self.session_id} worker encountered fatal error, transitioning to FAILED.\n"
            f"Exception: {type(exc).__name__}: {exc}\n"
            f"Traceback:\n{''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))}"
        )
        asyncio.create_task(self._state_machine.transition_to(StreamingSessionState.FAILED))
        self._event_queue.put_nowait(StreamingRuntimeErrorEvent(
            session_id=self.session_id,
            event_seq=self._next_event_seq(),
            wall_timestamp=time.time(),
            session_time_ms=self.get_session_time_ms(),
            error_message=str(exc)
        ))

    async def _event_reader_loop(self) -> None:
        """
        Task loop that pulls events from bidirectional transport.
        When the transport signals exhaustion (returns None), attempt reconnection
        unless the session is intentionally closing.
        """
        while self.state in (
            StreamingSessionState.CONNECTED,
            StreamingSessionState.STREAMING,
            StreamingSessionState.READY
        ):
            try:
                transport = self.transport
                if not transport:
                    await asyncio.sleep(0.01)
                    continue

                event = await transport.receive_event()
                if event is None:
                    # Transport closed (GoAway, session expiry, or clean close).
                    # Only reconnect if the session is still meant to be active.
                    if self.state in (StreamingSessionState.READY, StreamingSessionState.STREAMING, StreamingSessionState.CONNECTED):
                        logger.info(
                            f"Session {self.session_id}: Transport exhausted while session is active. "
                            "Scheduling reconnect..."
                        )
                        asyncio.create_task(self.reconnect())
                    break

                self._event_queue.put_nowait(event)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in Session event reader {self.session_id}: {e}", exc_info=True)
                break

    async def _event_dispatcher_loop(self) -> None:
        """
        Single dispatcher loop popping events sequentially from internal queue.
        Enriches transport events with canonical sequence numbers and clock timestamps.
        """
        while True:
            try:
                event = await self._event_queue.get()
                
                # Assign sequence numbers and clocks dynamically if event has sequence fields
                if event is not None and hasattr(event, "event_seq"):
                    event = replace(
                        event,
                        event_seq=self._next_event_seq(),
                        wall_timestamp=time.time(),
                        session_time_ms=self.get_session_time_ms()
                    )

                # Intercept metrics & accumulate cumulative text
                if isinstance(event, StreamingPartialTranslationEvent):
                    self._cumulative_text += event.text_delta
                    event = replace(event, cumulative_text=self._cumulative_text)
                    self._metrics_collector.record_partial()
                elif isinstance(event, StreamingTranslationCompletedEvent):
                    # Final response carries the entire cumulative chunk translation
                    if not event.full_text or event.full_text == "[Interrupted]":
                        full_txt = self._cumulative_text
                        if event.full_text == "[Interrupted]":
                            full_txt += " [Interrupted]"
                        event = replace(event, full_text=full_txt)
                    self._cumulative_text = ""  # Reset cumulative buffer on boundary
                    self._metrics_collector.record_completed()

                # Enrich participant identity and calculate latency if applicable
                if event is not None and hasattr(event, "correlation_id"):
                    corr_id = event.correlation_id
                    participant = self._correlation_to_participant.get(corr_id, "")
                    if participant and hasattr(event, "participant_identity"):
                        event = replace(event, participant_identity=participant)

                    capture_start_ns = self._correlation_to_capture_start_ns.get(corr_id)
                    if capture_start_ns:
                        latency_ms = (time.perf_counter_ns() - capture_start_ns) / 1_000_000.0
                        logger.info(
                            f"[Latency Instrumentation] Event {event.__class__.__name__} "
                            f"for correlation {corr_id} | End-to-End Latency: {latency_ms:.2f} ms"
                        )

                # Log event dispatching for debug traceability
                try:
                    logger.info(f"[Event Dispatcher] Emitting event {event.__class__.__name__} | corr={getattr(event, 'correlation_id', '')} | seq={getattr(event, 'event_seq', None)} | payload_preview={str(event)[:200]}")
                except Exception:
                    logger.info(f"[Event Dispatcher] Emitting event {event.__class__.__name__}")
                await self._emit(event)
                logger.info(f"[Event Dispatcher] Emitted event {event.__class__.__name__}")
                self._event_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in Event Dispatcher loop: {e}", exc_info=True)

    async def close(self) -> None:
        if self.state in (StreamingSessionState.CLOSED, StreamingSessionState.FINISHED):
            return

        await self._state_machine.transition_to(StreamingSessionState.FLUSHING)
        
        self._worker.stop()
        
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
            self._reader_task = None

        if self.transport:
            try:
                await self.transport.close()
            except Exception as e:
                logger.error(f"Error closing transport: {e}")
            self.transport = None

        # Clean queue
        drained = self._queue.clear()
        for _ in drained:
            self._metrics_collector.record_dropped()

        # Stop event dispatcher
        if self._dispatcher_task:
            self._dispatcher_task.cancel()
            try:
                await self._dispatcher_task
            except asyncio.CancelledError:
                pass
            self._dispatcher_task = None

        await self._state_machine.transition_to(StreamingSessionState.CLOSED)
        
        metrics_dict = self.get_metrics().__dict__
        await self._emit(StreamingSessionClosedEvent(
            session_id=self.session_id,
            event_seq=self._next_event_seq(),
            wall_timestamp=time.time(),
            session_time_ms=self.get_session_time_ms(),
            metrics=metrics_dict
        ))

    def get_metrics(self) -> StreamingSessionMetrics:
        return self._metrics_collector.get_metrics()

class StreamingSessionManager:
    """
    Manages creation, lookup, and deletion of active isolated sessions.
    """
    def __init__(self):
        self._sessions: Dict[str, StreamingSession] = {}
        self._lock = asyncio.Lock()

    async def create_session(
        self,
        session_id: str,
        runtime: BaseStreamingRuntime,
        source_language: str,
        target_language: str,
        queue_maxsize: int = 100,
        metadata: dict = None
    ) -> StreamingSession:
        async with self._lock:
            if session_id in self._sessions:
                raise ValueError(f"Session with ID {session_id} already exists.")
            
            session = StreamingSession(
                session_id=session_id,
                runtime=runtime,
                source_language=source_language,
                target_language=target_language,
                queue_maxsize=queue_maxsize,
                metadata=metadata
            )
            
            # Trigger startup transition sequence
            await session.start()
            
            self._sessions[session_id] = session
            
            await session._emit(StreamingSessionStartedEvent(
                session_id=session_id,
                event_seq=session._next_event_seq(),
                wall_timestamp=time.time(),
                session_time_ms=session.get_session_time_ms(),
                metadata=metadata or {}
            ))
            
            return session

    async def get_session(self, session_id: str) -> Optional[StreamingSession]:
        async with self._lock:
            return self._sessions.get(session_id)

    async def close_session(self, session_id: str) -> None:
        async with self._lock:
            session = self._sessions.pop(session_id, None)
            if session:
                await session.close()

    async def list_sessions(self) -> List[str]:
        async with self._lock:
            return list(self._sessions.keys())
