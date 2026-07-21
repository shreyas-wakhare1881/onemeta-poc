import asyncio
import time
from typing import Callable, Any, Optional

from ..streaming import BaseStreamingTransport, BaseStreamingRuntime
from ..events import StreamingPartialTranslationEvent, StreamingTranslationCompletedEvent
from ....transport.packet import StreamingAudioPacket

class MockStreamingTransport(BaseStreamingTransport):
    """
    Mock streaming transport connection complying with bidirectional events & cancellation.
    """
    def __init__(self, session_id: str, source_language: str, target_language: str):
        self.session_id = session_id
        self.source_language = source_language
        self.target_language = target_language
        self.closed = False
        self._speech_active = False
        self._event_queue = asyncio.Queue()
        self._generation_task: Optional[asyncio.Task] = None

    async def send_packet(self, packet: StreamingAudioPacket) -> None:
        if self.closed:
            return

        # Minimal processing delay (< 1ms overhead)
        await asyncio.sleep(0.0001)

        # Carry the correlation ID from the packet metadata through to the response events
        correlation_id = packet.metadata.correlation_id if packet.metadata else ""

        if packet.is_speech and not self._speech_active:
            self._speech_active = True
            if self._generation_task:
                self._generation_task.cancel()
            self._generation_task = asyncio.create_task(
                self._simulate_translation(correlation_id)
            )
        elif not packet.is_speech and self._speech_active:
            self._speech_active = False

    async def receive_event(self) -> Any:
        """
        Pulls events from the transport queue.
        """
        if self.closed:
            return None
        try:
            return await self._event_queue.get()
        except asyncio.CancelledError:
            return None

    def _push_event(self, event: Any) -> None:
        self._event_queue.put_nowait(event)

    async def _simulate_translation(self, correlation_id: str):
        try:
            # Simulate TTFT (50ms delay)
            await asyncio.sleep(0.05)
            if self.closed or not self._speech_active:
                return

            partial_event = StreamingPartialTranslationEvent(
                session_id=self.session_id,
                event_seq=0,  # Placeholder, overridden canonically by the Session dispatcher
                wall_timestamp=0.0,
                session_time_ms=0.0,
                text_delta="Hola",
                cumulative_text="Hola",
                correlation_id=correlation_id
            )
            self._push_event(partial_event)

            # Simulate completion (another 50ms delay)
            await asyncio.sleep(0.05)
            if self.closed or not self._speech_active:
                return

            completed_event = StreamingTranslationCompletedEvent(
                session_id=self.session_id,
                event_seq=0,
                wall_timestamp=0.0,
                session_time_ms=0.0,
                full_text="Hola",
                correlation_id=correlation_id,
                metrics={"tokens": 1}
            )
            self._push_event(completed_event)
        except asyncio.CancelledError:
            pass

    async def cancel_generation(self) -> None:
        """
        Cancels active generation tasks (simulates barge-in).
        """
        if self._generation_task:
            self._generation_task.cancel()
            self._generation_task = None
        self._speech_active = False

    async def close(self) -> None:
        self.closed = True
        if self._generation_task:
            self._generation_task.cancel()
            self._generation_task = None
        self._event_queue.put_nowait(None)

class MockStreamingRuntime(BaseStreamingRuntime):
    """
    Mock streaming runtime provider.
    """
    def __init__(self, config=None):
        self.config = config
        self._initialized = False

    async def initialize(self) -> None:
        self._initialized = True

    async def is_ready(self) -> bool:
        return self._initialized

    async def connect(
        self,
        session_id: str,
        source_language: str,
        target_language: str,
        on_event: Optional[Callable[[Any], Any]] = None,
        metadata: dict = None
    ) -> BaseStreamingTransport:
        if not self._initialized:
            await self.initialize()
        transport = MockStreamingTransport(session_id, source_language, target_language)
        return transport

    async def shutdown(self) -> None:
        self._initialized = False
