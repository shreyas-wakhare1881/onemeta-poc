import unittest
import asyncio
from typing import Optional, Any
from unittest.mock import AsyncMock, MagicMock, patch

from backend.app.ai.config import AIConfig
from backend.app.transport.packet import StreamingAudioPacket, StreamingPacketMetadata
from backend.app.ai.events import (
    StreamingPartialTranslationEvent,
    StreamingTranslationCompletedEvent,
    StreamingRuntimeErrorEvent,
    StreamingTranslationAudioEvent
)
from backend.app.ai.runtimes.gemini_live_translate_runtime import GeminiLiveTranslateRuntime, GeminiLiveTranslateTransport
from backend.app.ai.streaming import StreamingSession, StreamingSessionState

# Mock classes to mimic google.genai.types response structure
class MockAudioTranscription:
    def __init__(self, text: str):
        self.text = text

class MockPart:
    def __init__(self, text: Optional[str] = None, inline_data: Optional[Any] = None):
        self.text = text
        self.inline_data = inline_data

class MockInlineData:
    def __init__(self, data: bytes, mime_type: str = "audio/pcm"):
        self.data = data
        self.mime_type = mime_type

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

class TestGeminiLiveTranslateRuntime(unittest.IsolatedAsyncioTestCase):
    """
    Unit tests validating the Gemini Live Translation runtime integration and event parsing.
    """
    async def test_runtime_initialization_and_connect(self):
        config = AIConfig(
            google_api_key="fake-api-key",
            gemini_live_translate_model="models/gemini-3.5-live-translate-preview",
            target_language="es"
        )
        runtime = GeminiLiveTranslateRuntime(config)
        
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
                session_id="test-session-translate",
                source_language="English",
                target_language="Spanish"
            )
            
            self.assertIsInstance(transport, GeminiLiveTranslateTransport)
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

            # Verify receiving output audio data delta
            audio_msg = MockLiveServerMessage(
                server_content=MockServerContent(
                    model_turn=MockModelTurn(parts=[
                        MockPart(inline_data=MockInlineData(data=b"\x01\x02\x03"))
                    ])
                )
            )
            await mock_sdk_session.receive_queue.put(audio_msg)
            
            ev2 = await transport.receive_event()
            self.assertIsInstance(ev2, StreamingTranslationAudioEvent)
            self.assertEqual(ev2.audio_data, b"\x01\x02\x03")
            self.assertEqual(ev2.mime_type, "audio/pcm")
            self.assertEqual(ev2.correlation_id, "corr-test-123")
            
            # Verify close closes SDK session
            await transport.close()
            mock_ctx.__aexit__.assert_called_once()
            
        await runtime.shutdown()
        self.assertFalse(await runtime.is_ready())

    async def test_event_buffering_and_deduplication(self):
        config = AIConfig(
            google_api_key="fake-api-key",
            gemini_live_translate_model="models/gemini-3.5-live-translate-preview",
            target_language="es"
        )
        runtime = GeminiLiveTranslateRuntime(config)
        
        mock_client = MagicMock()
        mock_sdk_session = MockSDKSession()
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_sdk_session)
        mock_ctx.__aexit__ = AsyncMock()
        mock_client.aio.live.connect = MagicMock(return_value=mock_ctx)
        
        with patch("google.genai.Client", return_value=mock_client):
            await runtime.initialize()
            transport = await runtime.connect(
                session_id="test-session-buf",
                source_language="English",
                target_language="Spanish"
            )
            
            # Send packet to set active correlation ID
            packet = StreamingAudioPacket(
                pcm_data=memoryview(b"\x00" * 320),
                sample_rate=16000,
                channels=1,
                capture_timestamp_ns=1000,
                sequence_number=1,
                is_speech=True,
                metadata=StreamingPacketMetadata("f-1", "user", "sid", 90.0, "corr-test-buf")
            )
            await transport.send_packet(packet)
            
            # 1. Verify multiple events in a single message are preserved and not dropped
            mixed_msg = MockLiveServerMessage(
                server_content=MockServerContent(
                    output_transcription=MockAudioTranscription(text="Hola"),
                    model_turn=MockModelTurn(parts=[
                        MockPart(inline_data=MockInlineData(data=b"\xaa\xbb"))
                    ])
                )
            )
            await mock_sdk_session.receive_queue.put(mixed_msg)
            
            ev1 = await transport.receive_event()
            ev2 = await transport.receive_event()
            
            # Ensure both the transcription event and audio event are returned sequentially
            self.assertIsInstance(ev1, StreamingPartialTranslationEvent)
            self.assertEqual(ev1.text_delta, "Hola")
            self.assertIsInstance(ev2, StreamingTranslationAudioEvent)
            self.assertEqual(ev2.audio_data, b"\xaa\xbb")
            
            # 2. Verify that output transcription text is forwarded directly as delta
            delta_msg = MockLiveServerMessage(
                server_content=MockServerContent(
                    output_transcription=MockAudioTranscription(text=", amigo")
                )
            )
            await mock_sdk_session.receive_queue.put(delta_msg)
            ev3 = await transport.receive_event()
            self.assertIsInstance(ev3, StreamingPartialTranslationEvent)
            self.assertEqual(ev3.text_delta, ", amigo")
            
            # 3. Verify end_user_turn sends audio_stream_end
            await transport.end_user_turn()
            self.assertEqual(len(mock_sdk_session.sent_realtime_inputs), 2) # 1 packet + 1 end_user_turn
            args, kwargs = mock_sdk_session.sent_realtime_inputs[1]
            self.assertTrue(kwargs.get("audio_stream_end"))
            
            await transport.close()
        await runtime.shutdown()
