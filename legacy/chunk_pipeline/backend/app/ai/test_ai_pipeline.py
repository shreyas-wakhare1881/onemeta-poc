import unittest
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

from backend.app.types.speech import SpeechChunk, SpeechChunkMetadata, FlushReason
from backend.app.ai.config import AIConfig, QueueDropPolicy
from backend.app.ai.events import AIStartedEvent, AIPartialEvent, AICompletedEvent, AIErrorEvent, TranslationFailedEvent
from backend.app.ai.telemetry import AITelemetry
from backend.app.ai.engine import AIEngine
from backend.app.ai.sink import InferenceSink

def create_dummy_chunk(chunk_id: str, seq: int, pcm_data: bytes = None) -> SpeechChunk:
    """
    Creates a dummy SpeechChunk for testing purposes.
    """
    metadata = SpeechChunkMetadata(
        queue_wait_ms=0.0,
        processing_time_ms=0.0,
        end_to_end_age_ms=0.0,
        flush_reason=FlushReason.SILENCE,
        average_rms=100.0,
        peak_rms=150.0,
        speech_ratio=1.0
    )
    return SpeechChunk(
        chunk_id=chunk_id,
        sequence_number=seq,
        participant_identity="test-user",
        participant_session_id="session-123",
        room_name="test-room",
        start_timestamp=time.time(),
        end_timestamp=time.time() + 1.0,
        duration_ms=1000.0,
        frame_count=50,
        pcm_data=pcm_data if pcm_data is not None else b"\x00" * 32000,  # 1 second of silence PCM16 16kHz mono
        is_final=True,
        metadata=metadata
    )

class TestAIPipeline(unittest.IsolatedAsyncioTestCase):
    """
    Unit and integration tests for the AI Pipeline components.
    """
    async def test_queue_drop_oldest(self):
        config = AIConfig(queue_maxsize=2, queue_drop_policy=QueueDropPolicy.DROP_OLDEST)
        telemetry = AITelemetry()
        engine = AIEngine(config, telemetry)
        
        # Mock runtime initialization to avoid loading actual GPU resources
        engine.runtime.initialize = AsyncMock()
        await engine.start()
        
        chunk1 = create_dummy_chunk("chunk-1", 1)
        chunk2 = create_dummy_chunk("chunk-2", 2)
        chunk3 = create_dummy_chunk("chunk-3", 3)
        
        engine.enqueue_chunk(chunk1)
        engine.enqueue_chunk(chunk2)
        self.assertEqual(engine.queue.qsize(), 2)
        
        # Enqueue third chunk, should evict chunk-1 (oldest)
        engine.enqueue_chunk(chunk3)
        self.assertEqual(engine.queue.qsize(), 2)
        self.assertEqual(telemetry.dropped_chunks, 1)
        
        # Verify queue now contains chunk-2 and chunk-3
        q_item1 = engine.queue.get_nowait()
        q_item2 = engine.queue.get_nowait()
        self.assertEqual(q_item1.chunk_id, "chunk-2")
        self.assertEqual(q_item2.chunk_id, "chunk-3")
        
        await engine.shutdown()

    async def test_queue_drop_newest(self):
        config = AIConfig(queue_maxsize=2, queue_drop_policy=QueueDropPolicy.DROP_NEWEST)
        telemetry = AITelemetry()
        engine = AIEngine(config, telemetry)
        
        engine.runtime.initialize = AsyncMock()
        await engine.start()
        
        chunk1 = create_dummy_chunk("chunk-1", 1)
        chunk2 = create_dummy_chunk("chunk-2", 2)
        chunk3 = create_dummy_chunk("chunk-3", 3)
        
        engine.enqueue_chunk(chunk1)
        engine.enqueue_chunk(chunk2)
        self.assertEqual(engine.queue.qsize(), 2)
        
        # Enqueue third chunk, should discard chunk-3 (newest)
        engine.enqueue_chunk(chunk3)
        self.assertEqual(engine.queue.qsize(), 2)
        self.assertEqual(telemetry.dropped_chunks, 1)
        
        # Verify queue still contains chunk-1 and chunk-2
        q_item1 = engine.queue.get_nowait()
        q_item2 = engine.queue.get_nowait()
        self.assertEqual(q_item1.chunk_id, "chunk-1")
        self.assertEqual(q_item2.chunk_id, "chunk-2")
        
        await engine.shutdown()

    async def test_successful_inference_flow(self):
        config = AIConfig(queue_maxsize=2)
        telemetry = AITelemetry()
        engine = AIEngine(config, telemetry)
        
        engine.runtime.initialize = AsyncMock()
        
        # Mock streaming inference output generator
        async def mock_stream_inference(request):
            from backend.app.ai.types import TranslationResult, TranslationMetrics
            metrics = TranslationMetrics(audio_duration_ms=1000.0, payload_size_bytes=100)
            yield TranslationResult(
                chunk_id=request.chunk_id,
                sequence_number=request.sequence_number,
                translated_text="Hola",
                source_language=request.source_language,
                target_language=request.target_language,
                finished=False,
                metrics=metrics
            )
            await asyncio.sleep(0.01)
            yield TranslationResult(
                chunk_id=request.chunk_id,
                sequence_number=request.sequence_number,
                translated_text=" amigo",
                source_language=request.source_language,
                target_language=request.target_language,
                finished=False,
                metrics=metrics
            )
            yield TranslationResult(
                chunk_id=request.chunk_id,
                sequence_number=request.sequence_number,
                translated_text="",
                source_language=request.source_language,
                target_language=request.target_language,
                finished=True,
                metrics=metrics
            )
            
        engine.runtime.stream_generate = mock_stream_inference
        
        events_received = []
        def listener(event):
            events_received.append(event)
            
        engine.register_listener(listener)
        await engine.start()
        
        chunk = create_dummy_chunk("chunk-100", 100)
        
        # Direct write to InferenceSink to test pipeline integration
        sink = InferenceSink(engine)
        await sink.write_chunk(chunk)
        
        # Wait a short duration for background worker to consume and process
        await asyncio.sleep(0.1)
        
        # Verify event sequence
        self.assertTrue(len(events_received) >= 4)  # Started -> Partials -> Completed
        self.assertIsInstance(events_received[0], AIStartedEvent)
        self.assertEqual(events_received[0].chunk_id, "chunk-100")
        self.assertEqual(events_received[0].sequence_number, 100)
        
        self.assertIsInstance(events_received[1], AIPartialEvent)
        self.assertEqual(events_received[1].text_delta, "Hola")
        self.assertEqual(events_received[1].cumulative_text, "Hola")
        
        self.assertIsInstance(events_received[2], AIPartialEvent)
        self.assertEqual(events_received[2].text_delta, " amigo")
        self.assertEqual(events_received[2].cumulative_text, "Hola amigo")
        
        self.assertIsInstance(events_received[-1], AICompletedEvent)
        self.assertEqual(events_received[-1].full_text, "Hola amigo")
        self.assertGreater(events_received[-1].duration_ms, 0.0)
        
        # Verify telemetry records
        report = telemetry.get_report(0)
        self.assertEqual(report["successful_requests"], 1)
        self.assertEqual(report["failed_requests"], 0)
        self.assertEqual(report["dropped_chunks"], 0)
        self.assertGreater(report["avg_gemma_latency_ms"], 0.0)
        self.assertGreater(report["avg_total_ai_latency_ms"], 0.0)
        self.assertEqual(report["total_tokens"], 2)
        
        await engine.shutdown()

    async def test_failed_inference_flow(self):
        config = AIConfig(queue_maxsize=2)
        telemetry = AITelemetry()
        engine = AIEngine(config, telemetry)
        
        engine.runtime.initialize = AsyncMock()
        
        # Mock inference generator to raise an error
        async def mock_failed_stream(request):
            raise ValueError("Inference engine CUDA error")
            yield None
            
        engine.runtime.stream_generate = mock_failed_stream
        
        events_received = []
        engine.register_listener(lambda e: events_received.append(e))
        await engine.start()
        
        chunk = create_dummy_chunk("chunk-error", 5)
        engine.enqueue_chunk(chunk)
        
        await asyncio.sleep(0.05)
        
        # Verify events
        self.assertEqual(len(events_received), 3)  # Started -> Failed -> Error
        self.assertIsInstance(events_received[0], AIStartedEvent)
        self.assertIsInstance(events_received[1], TranslationFailedEvent)
        self.assertIsInstance(events_received[2], AIErrorEvent)
        self.assertEqual(events_received[1].error_message, "Inference engine CUDA error")
        
        # Verify telemetry
        report = telemetry.get_report(0)
        self.assertEqual(report["successful_requests"], 0)
        self.assertEqual(report["failed_requests"], 1)
        
        await engine.shutdown()

    async def test_graceful_shutdown_with_pending_chunks(self):
        config = AIConfig(queue_maxsize=5)
        telemetry = AITelemetry()
        engine = AIEngine(config, telemetry)
        
        engine.runtime.initialize = AsyncMock()
        engine.runtime.shutdown = AsyncMock()
        
        # Slow down inference execution
        async def mock_slow_inference(request):
            from backend.app.ai.types import TranslationResult
            await asyncio.sleep(0.5)
            yield TranslationResult(
                chunk_id=request.chunk_id,
                sequence_number=request.sequence_number,
                translated_text="done",
                source_language=request.source_language,
                target_language=request.target_language,
                finished=False
            )
            yield TranslationResult(
                chunk_id=request.chunk_id,
                sequence_number=request.sequence_number,
                translated_text="",
                source_language=request.source_language,
                target_language=request.target_language,
                finished=True
            )
            
        engine.runtime.stream_generate = mock_slow_inference
        await engine.start()
        
        # Populate queue
        chunk1 = create_dummy_chunk("c1", 1)
        chunk2 = create_dummy_chunk("c2", 2)
        chunk3 = create_dummy_chunk("c3", 3)
        
        engine.enqueue_chunk(chunk1)
        engine.enqueue_chunk(chunk2)
        engine.enqueue_chunk(chunk3)
        
        self.assertEqual(engine.queue.qsize(), 3)
        
        # Yield execution to allow worker to pull the first chunk
        await asyncio.sleep(0.01)
        self.assertEqual(engine.queue.qsize(), 2)  # c1 is being processed, c2 and c3 are pending in queue
        
        # Trigger immediate shutdown
        await engine.shutdown()
        
        # Verify remaining pending chunks are flushed and counted as dropped
        self.assertEqual(engine.queue.qsize(), 0)
        self.assertEqual(telemetry.dropped_chunks, 2)  # c2 and c3 dropped
        
        # Verify runtime clean shutdown was invoked
        engine.runtime.shutdown.assert_called_once()

    async def test_real_gemma_runtime_inference(self):
        """
        Runs the actual LocalGemmaRuntime initialization, processor mapping,
        and inference generation, skipping if model files or requirements are missing.
        """
        import os
        
        def has_real_gemma_env() -> bool:
            try:
                import torch
                import transformers
                model_path = os.getenv("GEMMA_MODEL_PATH", "")
                return bool(model_path and os.path.exists(model_path))
            except ImportError:
                return False
                
        if not has_real_gemma_env():
            self.skipTest("Skipping real Gemma inference test: PyTorch/Transformers not installed or GEMMA_MODEL_PATH not configured.")
            
        config = AIConfig(model_path=os.getenv("GEMMA_MODEL_PATH"), device="cpu")
        telemetry = AITelemetry()
        engine = AIEngine(config, telemetry)
        
        await engine.start()
        
        chunk = create_dummy_chunk("real-integration-chunk", 42)
        
        from backend.app.ai.types import RuntimeRequest
        request = RuntimeRequest(
            audio_bytes=chunk.pcm_data,
            audio_format="pcm16",
            sample_rate=16000,
            source_language="English",
            target_language="Spanish"
        )
        tokens = []
        async for response in engine.runtime.stream_generate(request):
            if response.finished:
                break
            tokens.append(response.translated_text)
            
        self.assertTrue(len(tokens) > 0)
        await engine.shutdown()

    async def test_ollama_runtime_offline_error(self):
        """
        Verifies that OllamaGemmaRuntime initialize fails loudly if the host is offline.
        """
        config = AIConfig(runtime_type="ollama", ollama_host="http://localhost:9999") # Offline port
        telemetry = AITelemetry()
        engine = AIEngine(config, telemetry)
        
        # Initialization should fail loudly
        with self.assertRaises(RuntimeError) as ctx:
            await engine.runtime.initialize()
        self.assertIn("Ollama server unreachable", str(ctx.exception))
    async def test_ollama_runtime_model_missing_error(self):
        """
        Verifies that OllamaGemmaRuntime initialize fails loudly if the model is not pulled.
        """
        config = AIConfig(runtime_type="ollama", ollama_host="http://localhost:9999", ollama_model="gemma4:12b")
        telemetry = AITelemetry()
        engine = AIEngine(config, telemetry)
        
        # Mock requests.get response for tags
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"models": [{"name": "some-other-model:latest"}]}
        
        import requests
        original_get = requests.get
        requests.get = MagicMock(return_value=mock_response)
        
        try:
            with self.assertRaises(RuntimeError) as ctx:
                await engine.runtime.initialize()
            self.assertIn("is not installed", str(ctx.exception))
        finally:
            requests.get = original_get
    async def test_multiple_sequential_chunks(self):
        """
        Verifies that multiple sequential chunks are processed and preserve order.
        """
        config = AIConfig(queue_maxsize=5)
        telemetry = AITelemetry()
        engine = AIEngine(config, telemetry)
        engine.runtime.initialize = AsyncMock()

        processed_seqs = []
        async def mock_sequential_inference(request):
            from backend.app.ai.types import TranslationResult
            processed_seqs.append(request.sequence_number)
            yield TranslationResult(
                chunk_id=request.chunk_id,
                sequence_number=request.sequence_number,
                translated_text=f"text-{request.sequence_number}",
                source_language=request.source_language,
                target_language=request.target_language,
                finished=True
            )

        engine.runtime.stream_generate = mock_sequential_inference
        await engine.start()

        # Enqueue 3 sequential chunks
        engine.enqueue_chunk(create_dummy_chunk("c-1", 1))
        engine.enqueue_chunk(create_dummy_chunk("c-2", 2))
        engine.enqueue_chunk(create_dummy_chunk("c-3", 3))

        await asyncio.sleep(0.1)

        self.assertEqual(processed_seqs, [1, 2, 3])
        self.assertEqual(telemetry.successful_requests, 3)
        await engine.shutdown()

    async def test_empty_silence_chunks(self):
        """
        Verifies that empty/silence chunks are handled safely without crash.
        """
        config = AIConfig(queue_maxsize=2)
        telemetry = AITelemetry()
        engine = AIEngine(config, telemetry)
        engine.runtime.initialize = AsyncMock()

        async def mock_inference(request):
            from backend.app.ai.types import TranslationResult
            yield TranslationResult(
                chunk_id=request.chunk_id,
                sequence_number=request.sequence_number,
                translated_text="",
                source_language=request.source_language,
                target_language=request.target_language,
                finished=True
            )

        engine.runtime.stream_generate = mock_inference
        await engine.start()

        # Empty PCM data chunk
        empty_chunk = create_dummy_chunk("c-empty", 1, pcm_data=b"")
        engine.enqueue_chunk(empty_chunk)

        await asyncio.sleep(0.05)
        self.assertEqual(telemetry.successful_requests, 1)
        await engine.shutdown()

    async def test_long_chunk_timeout(self):
        """
        Verifies that long chunk generation timeouts are handled gracefully.
        """
        config = AIConfig(queue_maxsize=2)
        telemetry = AITelemetry()
        engine = AIEngine(config, telemetry)
        engine.runtime.initialize = AsyncMock()

        # Simulate timeout exception in runtime post call
        async def mock_timeout_inference(request):
            raise requests.exceptions.Timeout("Ollama API timed out")
            yield None

        import requests
        engine.runtime.stream_generate = mock_timeout_inference
        
        events_received = []
        engine.register_listener(lambda e: events_received.append(e))
        await engine.start()

        engine.enqueue_chunk(create_dummy_chunk("c-timeout", 1))
        await asyncio.sleep(0.05)

        # Should emit TranslationFailedEvent & AIErrorEvent
        self.assertTrue(any(isinstance(e, TranslationFailedEvent) for e in events_received))
        self.assertTrue(any(isinstance(e, AIErrorEvent) for e in events_received))
        self.assertEqual(telemetry.failed_requests, 1)
        await engine.shutdown()

if __name__ == "__main__":
    unittest.main()
