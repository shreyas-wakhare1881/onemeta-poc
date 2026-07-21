import unittest
import asyncio
import time
from typing import List

from backend.app.types.audio import AudioFrame
from backend.app.transport.packet import StreamingAudioPacket, StreamingPacketMetadata
from backend.app.ai.streaming import (
    StreamingSessionState,
    StreamingSession,
    StreamingSessionManager,
    SessionPacketQueue,
    SessionQueueItem,
)
from backend.tests.mocks.mock_streaming import MockStreamingRuntime, MockStreamingTransport
from backend.app.ai.events import (
    StreamingSessionStartedEvent,
    StreamingAudioFrameReceivedEvent,
    StreamingSpeechStartedEvent,
    StreamingSpeechEndedEvent,
    StreamingPartialTranslationEvent,
    StreamingTranslationCompletedEvent,
    StreamingSessionClosedEvent,
    StreamingBackpressureEvent,
    StreamingStateChangedEvent,
)
from backend.app.audio.processor import StreamingSpeechProcessor
from backend.app.audio.config import AudioConfig
from backend.app.audio.telemetry import AudioTelemetry

class TestStreamingFoundation(unittest.IsolatedAsyncioTestCase):
    """
    Unit and integration tests for the Phase 4A Streaming Runtime Foundation.
    """
    async def test_mock_runtime_bidirectional_flow(self):
        runtime = MockStreamingRuntime()
        await runtime.initialize()
        
        events = []
        transport = await runtime.connect(
            session_id="test-session-1",
            source_language="English",
            target_language="Spanish",
            on_event=events.append
        )
        self.assertIsInstance(transport, MockStreamingTransport)
        
        # Verify pull receive_event interface
        packet = StreamingAudioPacket(
            pcm_data=memoryview(b"\x00" * 640),
            sample_rate=16000,
            channels=1,
            capture_timestamp_ns=time.perf_counter_ns(),
            sequence_number=1,
            is_speech=True,
            metadata=StreamingPacketMetadata("f-1", "u-1", "s-1", 100.0)
        )
        await transport.send_packet(packet)
        
        # Read from pull-based receive_event
        ev1 = await transport.receive_event()
        self.assertIsInstance(ev1, StreamingPartialTranslationEvent)
        self.assertEqual(ev1.text_delta, "Hola")
        
        ev2 = await transport.receive_event()
        self.assertIsInstance(ev2, StreamingTranslationCompletedEvent)
        self.assertEqual(ev2.full_text, "Hola")
        
        # Test cancellation
        await transport.cancel_generation()
        self.assertFalse(transport._speech_active)
        
        await transport.close()
        self.assertTrue(transport.closed)
        await runtime.shutdown()

    async def test_session_state_machine_lifecycle(self):
        runtime = MockStreamingRuntime()
        session = StreamingSession(
            session_id="session-state-1",
            runtime=runtime,
            source_language="English",
            target_language="Spanish"
        )
        self.assertEqual(session.state, StreamingSessionState.CREATED)
        
        events = []
        session.register_listener(events.append)
        await session.start()
        self.assertEqual(session.state, StreamingSessionState.READY)
        
        # Send non-speech frame
        packet = StreamingAudioPacket(
            pcm_data=memoryview(b"\x00" * 640),
            sample_rate=16000,
            channels=1,
            capture_timestamp_ns=time.perf_counter_ns(),
            sequence_number=1,
            is_speech=False,
            metadata=StreamingPacketMetadata("f-1", "user", "sid", 50.0)
        )
        await session.send_audio(packet)
        await asyncio.sleep(0.02)
        
        # State transitions to STREAMING when worker consumes first packet
        self.assertEqual(session.state, StreamingSessionState.STREAMING)
        
        await session.close()
        self.assertEqual(session.state, StreamingSessionState.CLOSED)
        
        state_changes = [ev for ev in events if isinstance(ev, StreamingStateChangedEvent)]
        self.assertTrue(any(sc.new_state == "INITIALIZING" for sc in state_changes))
        self.assertTrue(any(sc.new_state == "CONNECTING" for sc in state_changes))
        self.assertTrue(any(sc.new_state == "CONNECTED" for sc in state_changes))
        self.assertTrue(any(sc.new_state == "READY" for sc in state_changes))
        self.assertTrue(any(sc.new_state == "STREAMING" for sc in state_changes))
        self.assertTrue(any(sc.new_state == "CLOSED" for sc in state_changes))

    async def test_session_reconnection_flow(self):
        runtime = MockStreamingRuntime()
        session = StreamingSession(
            session_id="session-recon-1",
            runtime=runtime,
            source_language="English",
            target_language="Spanish"
        )
        await session.start()
        self.assertEqual(session.state, StreamingSessionState.READY)
        
        events = []
        session.register_listener(events.append)
        
        await session.reconnect()
        self.assertEqual(session.state, StreamingSessionState.READY)
        
        state_changes = [ev.new_state for ev in events if isinstance(ev, StreamingStateChangedEvent)]
        self.assertIn("RECONNECTING", state_changes)
        self.assertIn("CONNECTING", state_changes)
        self.assertIn("CONNECTED", state_changes)
        self.assertIn("READY", state_changes)
        
        await session.close()

    async def test_session_monotonic_clock_metrics(self):
        runtime = MockStreamingRuntime()
        session = StreamingSession(
            session_id="session-clock-1",
            runtime=runtime,
            source_language="English",
            target_language="Spanish"
        )
        await session.start()
        
        # Ingest frames with queue delay simulation
        t_capture = time.perf_counter_ns() - 50_000_000 # 50ms in past
        packet = StreamingAudioPacket(
            pcm_data=memoryview(b"\x00" * 640),
            sample_rate=16000,
            channels=1,
            capture_timestamp_ns=t_capture,
            sequence_number=1,
            is_speech=True,
            metadata=StreamingPacketMetadata("f-1", "user", "sid", 80.0)
        )
        
        # Simulate processor telling session that speech started
        session.record_speech_start()
        
        events = []
        session.register_listener(events.append)
        
        await session.send_audio(packet)
        await asyncio.sleep(0.2) # wait for mock tokens
        
        metrics = session.get_metrics()
        self.assertEqual(metrics.received_frames, 1)
        self.assertGreaterEqual(metrics.avg_frame_delay_ms, 50.0)
        self.assertGreater(metrics.first_token_latency_ms, 0.0)
        self.assertGreater(metrics.final_response_latency_ms, 0.0)
        self.assertGreater(metrics.queue_wait_ms, 0.0) # wait time should be >0
        
        await session.close()

    async def test_smart_backpressure_policy(self):
        # Verify custom SessionPacketQueue prioritizes non-speech eviction
        queue = SessionPacketQueue(maxsize=3)
        
        # Build 3 packets (2 speech, 1 non-speech)
        p_speech1 = StreamingAudioPacket(memoryview(b"\x00"), 16000, 1, 0, 1, True, StreamingPacketMetadata("f1", "u", "s", 1.0))
        p_nonspeech = StreamingAudioPacket(memoryview(b"\x00"), 16000, 1, 0, 2, False, StreamingPacketMetadata("f2", "u", "s", 1.0))
        p_speech2 = StreamingAudioPacket(memoryview(b"\x00"), 16000, 1, 0, 3, True, StreamingPacketMetadata("f3", "u", "s", 1.0))
        
        await queue.put(SessionQueueItem(p_speech1, 0))
        await queue.put(SessionQueueItem(p_nonspeech, 0))
        await queue.put(SessionQueueItem(p_speech2, 0))
        self.assertEqual(queue.qsize(), 3)
        
        # Put 4th packet (speech). It should evict the oldest NON-speech packet (which is sequence 2)
        p_speech3 = StreamingAudioPacket(memoryview(b"\x00"), 16000, 1, 0, 4, True, StreamingPacketMetadata("f4", "u", "s", 1.0))
        evicted = await queue.put(SessionQueueItem(p_speech3, 0))
        
        self.assertIsNotNone(evicted)
        self.assertEqual(evicted.packet.sequence_number, 2) # Evicted non-speech packet
        self.assertEqual(queue.qsize(), 3)
        
        # Drained queue should contain sequences 1, 3, 4
        items = [q_item.packet.sequence_number for q_item in queue.clear()]
        self.assertEqual(items, [1, 3, 4])

    async def test_stage1_dsp_vad_control_plane(self):
        # Verify StreamingSpeechProcessor emits speech start/ended control plane events
        config = AudioConfig()
        telemetry = AudioTelemetry()
        processor = StreamingSpeechProcessor(config, "test-room", telemetry)
        await processor.initialize()
        
        events = []
        processor.register_listener(events.append)
        
        # Ingest active speech frame (VAD starts speech)
        frame1 = AudioFrame(
            frame_id="u-1",
            sequence_number=1,
            participant_identity="user",
            participant_session_id="sid",
            capture_timestamp_ns=time.perf_counter_ns(),
            pcm_data=b"\xd0\x07" * 320 # High energy (amplitude=2000) triggers speech
        )
        
        await processor.process_frame(frame1)
        
        # Verify speech started event
        self.assertEqual(len(events), 1)
        self.assertIsInstance(events[0], StreamingSpeechStartedEvent)
        
        # Ingest silent frame (VAD ends speech)
        frame2 = AudioFrame(
            frame_id="u-2",
            sequence_number=2,
            participant_identity="user",
            participant_session_id="sid",
            capture_timestamp_ns=time.perf_counter_ns(),
            pcm_data=b"\x00" * 640 # Silence ends speech
        )
        # Flush VAD silence threshold
        for i in range(25): # VAD uses hysteresis (multiple silence frames)
            await processor.process_frame(frame2)
            
        # Verify speech ended event
        self.assertTrue(any(isinstance(ev, StreamingSpeechEndedEvent) for ev in events))
        
        await processor.shutdown()
