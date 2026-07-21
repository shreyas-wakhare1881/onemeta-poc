import unittest
import asyncio
from typing import Optional, Any
from unittest.mock import AsyncMock, MagicMock, patch

from backend.app.ai.config import AIConfig
from backend.app.transport.packet import StreamingAudioPacket, StreamingPacketMetadata
from backend.app.ai.events import (
    StreamingPartialTranslationEvent,
    StreamingTranslationCompletedEvent,
    StreamingRuntimeErrorEvent
)
from backend.app.ai.runtimes.gemini_live_runtime import GeminiLiveRuntime, GeminiLiveTransport
from backend.app.ai.streaming import StreamingSession, StreamingSessionState

# Mock classes to mimic google.genai.types response structure
class MockAudioTranscription:
    def __init__(self, text: str):
        self.text = text

class MockPart:
    def __init__(self, text: Optional[str] = None, inline_data: Optional[Any] = None):
        self.text = text
        self.inline_data = inline_data

class MockModelTurn:
    def __init__(self, parts: list):
        self.parts = parts

class MockServerContent:
    def __init__(
        self,
        model_turn: Optional[MockModelTurn] = None,
        output_transcription: Optional[MockAudioTranscription] = None,
        turn_complete: bool = False,
        interrupted: bool = False
    ):
        self.model_turn = model_turn
        self.output_transcription = output_transcription
        self.turn_complete = turn_complete
        self.interrupted = interrupted

class MockLiveServerMessage:
    def __init__(self, server_content: Optional[MockServerContent] = None):
        self.server_content = server_content

class MockSDKSession:
    def __init__(self):
        self.sent_realtime_inputs = []
        self.closed = False
        self.receive_queue = asyncio.Queue()
        self._ws = MagicMock()
        self._ws.send = AsyncMock()

    async def send_realtime_input(self, *args, **kwargs) -> None:
        self.sent_realtime_inputs.append((args, kwargs))

    async def receive(self):
        while True:
            val = await self.receive_queue.get()
            if val is None:
                break
            if isinstance(val, StopAsyncIteration):
                return
            if isinstance(val, BaseException):
                raise val
            yield val

    async def close(self) -> None:
        self.closed = True

class TestGeminiLiveRuntime(unittest.IsolatedAsyncioTestCase):
    """
    Unit tests validating the Gemini Live runtime SDK integration and event parsing.
    """
    async def test_runtime_initialization_and_connect(self):
        config = AIConfig(
            gemini_live_api_key="fake-api-key",
            gemini_live_model="models/gemini-2.5-flash-native-audio-latest",
            gemini_live_modalities="AUDIO",
            gemini_live_voice_name="Aoede"
        )
        runtime = GeminiLiveRuntime(config)
        
        # Mock the GenAI Client creation
        mock_client = MagicMock()
        mock_sdk_session = MockSDKSession()
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_sdk_session)
        mock_ctx.__aexit__ = AsyncMock()
        mock_client.aio.live.connect = MagicMock(return_value=mock_ctx)
        
        with patch("google.genai.Client", return_value=mock_client):
            await runtime.initialize()
            self.assertTrue(await runtime.is_ready())
            
            transport = await runtime.connect(
                session_id="test-session",
                source_language="English",
                target_language="Spanish"
            )
            
            self.assertIsInstance(transport, GeminiLiveTransport)
            mock_client.aio.live.connect.assert_called_once()
            
            # Verify packet send delegates to SDK send_realtime_input
            packet = StreamingAudioPacket(
                pcm_data=memoryview(b"\x00" * 320),
                sample_rate=16000,
                channels=1,
                capture_timestamp_ns=1000,
                sequence_number=1,
                is_speech=True,
                metadata=StreamingPacketMetadata("f-1", "user", "sid", 90.0, "corr-test-123")
            )
            await transport.send_packet(packet)
            
            self.assertEqual(len(mock_sdk_session.sent_realtime_inputs), 1)
            args, kwargs = mock_sdk_session.sent_realtime_inputs[0]
            blob = kwargs.get("media")
            self.assertEqual(blob.mime_type, "audio/pcm;rate=16000")
            self.assertEqual(blob.data, b"\x00" * 320)
            
            # Verify receiving output transcription text delta
            tx_msg = MockLiveServerMessage(
                server_content=MockServerContent(
                    output_transcription=MockAudioTranscription(text="Hola")
                )
            )
            await mock_sdk_session.receive_queue.put(tx_msg)
            
            ev1 = await transport.receive_event()
            self.assertIsInstance(ev1, StreamingPartialTranslationEvent)
            self.assertEqual(ev1.text_delta, "Hola")
            self.assertEqual(ev1.correlation_id, "corr-test-123")
            
            # Verify cancel generation sends interruption clientContent directly to socket
            await transport.cancel_generation()
            mock_sdk_session._ws.send.assert_called_once()
            
            # Verify close closes SDK session
            await transport.close()
            mock_ctx.__aexit__.assert_called_once()
            
        await runtime.shutdown()
        self.assertFalse(await runtime.is_ready())

    async def test_session_integration_cumulative_aggregation(self):
        """
        Integration test verifying that StreamingSession correctly computes cumulative text
        and handles turnComplete resets over the Gemini Live transport layer.
        """
        config = AIConfig(
            gemini_live_api_key="fake-api-key"
        )
        runtime = GeminiLiveRuntime(config)
        
        mock_client = MagicMock()
        mock_sdk_session = MockSDKSession()
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_sdk_session)
        mock_ctx.__aexit__ = AsyncMock()
        mock_client.aio.live.connect = MagicMock(return_value=mock_ctx)
        
        with patch("google.genai.Client", return_value=mock_client):
            await runtime.initialize()
            
            session = StreamingSession(
                session_id="test-session-cumulative",
                runtime=runtime,
                source_language="English",
                target_language="Spanish"
            )
            
            await session.start()
            self.assertEqual(session.state, StreamingSessionState.READY)
            
            events = []
            session.register_listener(events.append)
            
            # Send packet to set active correlation ID
            packet = StreamingAudioPacket(
                pcm_data=memoryview(b"\x00" * 320),
                sample_rate=16000,
                channels=1,
                capture_timestamp_ns=1000,
                sequence_number=1,
                is_speech=True,
                metadata=StreamingPacketMetadata("f-1", "user", "sid", 90.0, "corr-test-abc")
            )
            await session.send_audio(packet)
            
            # Queue mock messages
            msg1 = MockLiveServerMessage(
                server_content=MockServerContent(
                    output_transcription=MockAudioTranscription(text="Ho")
                )
            )
            msg2 = MockLiveServerMessage(
                server_content=MockServerContent(
                    output_transcription=MockAudioTranscription(text="la")
                )
            )
            msg3 = MockLiveServerMessage(
                server_content=MockServerContent(
                    turn_complete=True
                )
            )
            
            await mock_sdk_session.receive_queue.put(msg1)
            await mock_sdk_session.receive_queue.put(msg2)
            await mock_sdk_session.receive_queue.put(msg3)
            
            await asyncio.sleep(0.15)
            
            partials = [e for e in events if isinstance(e, StreamingPartialTranslationEvent)]
            completes = [e for e in events if isinstance(e, StreamingTranslationCompletedEvent)]
            
            self.assertEqual(len(partials), 2)
            self.assertEqual(len(completes), 1)
            
            self.assertEqual(partials[0].text_delta, "Ho")
            self.assertEqual(partials[0].cumulative_text, "Ho")
            self.assertEqual(partials[0].correlation_id, "corr-test-abc")
            
            self.assertEqual(partials[1].text_delta, "la")
            self.assertEqual(partials[1].cumulative_text, "Hola")
            self.assertEqual(partials[1].correlation_id, "corr-test-abc")
            
            self.assertEqual(completes[0].full_text, "Hola")
            self.assertEqual(completes[0].correlation_id, "corr-test-abc")
            
            await session.close()
        await runtime.shutdown()

    async def test_reconnection_failures_mapped_to_events(self):
        config = AIConfig(
            gemini_live_api_key="fake-api-key"
        )
        runtime = GeminiLiveRuntime(config)
        
        mock_client = MagicMock()
        mock_sdk_session = MockSDKSession()
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_sdk_session)
        mock_ctx.__aexit__ = AsyncMock()
        mock_client.aio.live.connect = MagicMock(return_value=mock_ctx)
        
        with patch("google.genai.Client", return_value=mock_client):
            await runtime.initialize()
            
            transport = await runtime.connect(
                session_id="test-session",
                source_language="English",
                target_language="Spanish"
            )
            
            # Put a random Exception to test error mapping
            await mock_sdk_session.receive_queue.put(RuntimeError("Unexpected connection error"))
            ev = await transport.receive_event()
            self.assertIsInstance(ev, StreamingRuntimeErrorEvent)
            self.assertIn("Unexpected connection error", ev.error_message)
            
            # Put exception inside queue to simulate connection failure during iteration
            await mock_sdk_session.receive_queue.put(StopAsyncIteration())
            ev2 = await transport.receive_event()
            # StopAsyncIteration returns None safely (iterator ended)
            self.assertIsNone(ev2)
            
            await transport.close()
        await runtime.shutdown()
