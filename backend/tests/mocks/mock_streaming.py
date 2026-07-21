import asyncio
from typing import Callable, Any, Optional

from backend.app.ai.streaming import BaseStreamingTransport, BaseStreamingRuntime
from backend.app.ai.events import StreamingPartialTranslationEvent, StreamingTranslationCompletedEvent
from backend.app.transport.packet import StreamingAudioPacket


class MockStreamingTransport(BaseStreamingTransport):
    """
    Mock streaming transport connection complying with bidirectional events & cancellation.
    Used exclusively in unit tests — do NOT use in production code.
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

        await asyncio.sleep(0.0001)

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
            await asyncio.sleep(0.05)
            if self.closed or not self._speech_active:
                return

            self._push_event(StreamingPartialTranslationEvent(
                session_id=self.session_id,
                event_seq=0,
                wall_timestamp=0.0,
                session_time_ms=0.0,
                text_delta="Hola",
                cumulative_text="Hola",
                correlation_id=correlation_id
            ))

            await asyncio.sleep(0.05)
            if self.closed or not self._speech_active:
                return

            self._push_event(StreamingTranslationCompletedEvent(
                session_id=self.session_id,
                event_seq=0,
                wall_timestamp=0.0,
                session_time_ms=0.0,
                full_text="Hola",
                correlation_id=correlation_id,
                metrics={"tokens": 1}
            ))
        except asyncio.CancelledError:
            pass

    async def cancel_generation(self) -> None:
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

    async def end_user_turn(self) -> None:
        pass


class MockStreamingRuntime(BaseStreamingRuntime):
    """
    Mock streaming runtime provider. Used exclusively in unit tests.
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
        return MockStreamingTransport(session_id, source_language, target_language)

    async def shutdown(self) -> None:
        self._initialized = False
