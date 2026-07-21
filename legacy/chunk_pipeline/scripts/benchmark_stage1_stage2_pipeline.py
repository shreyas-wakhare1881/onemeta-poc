import os
import sys
import time
import asyncio
import logging
from typing import AsyncIterator

# Suppress verbose backend logs during stress testing to keep output clean
logging.getLogger("onemeta").setLevel(logging.ERROR)

# Ensure backend directory is in sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
BACKEND_DIR = os.path.join(PROJECT_ROOT, "backend")

if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

import numpy as np
from app.types.audio import AudioFrame
from app.types.speech import SpeechChunk
from app.audio.config import AudioConfig
from app.audio.telemetry import AudioTelemetry
from app.audio.processor import StreamingSpeechProcessor

from app.ai.config import AIConfig
from app.ai.telemetry import AITelemetry
from app.ai.engine import AIEngine
from app.ai.sink import InferenceSink
from app.ai.runtimes.base import BaseRuntime
from app.ai.types import RuntimeRequest, TranslationResult, TranslationMetrics
from app.ai.events import AICompletedEvent, AIErrorEvent


class MockGemmaRuntime(BaseRuntime):
    """
    Lightweight Mock Gemma Runtime that immediately returns a fake translation response without
    running LLM inference, GPU operations, or external network requests.
    """
    def __init__(self, config=None):
        super().__init__(config)
        self._ready = False
        self.request_count = 0
        self.response_count = 0

    async def initialize(self) -> None:
        self._ready = True

    async def is_ready(self) -> bool:
        return self._ready

    async def stream_generate(self, request: RuntimeRequest) -> AsyncIterator[TranslationResult]:
        self.request_count += 1
        fake_translation = "Hola"

        # 1. Partial streaming response with fake translation text
        yield TranslationResult(
            chunk_id=request.chunk_id,
            sequence_number=request.sequence_number,
            translated_text=fake_translation,
            source_language=request.source_language,
            target_language=request.target_language,
            finished=False,
            metrics=TranslationMetrics(
                ttft_ms=0.1,
                total_response_time_ms=0.2,
                payload_size_bytes=len(fake_translation.encode("utf-8")),
                audio_duration_ms=len(request.audio_bytes) / 32.0,
                translation_length_chars=len(fake_translation),
                translation_length_tokens=1,
                chunk_number=request.sequence_number
            ),
            metadata={"status": "success", "runtime": "MockGemmaRuntime"}
        )

        self.response_count += 1

        # 2. Final completion marker
        yield TranslationResult(
            chunk_id=request.chunk_id,
            sequence_number=request.sequence_number,
            translated_text="",
            source_language=request.source_language,
            target_language=request.target_language,
            finished=True,
            metrics=TranslationMetrics(),
            metadata={"status": "success", "runtime": "MockGemmaRuntime"}
        )

    async def shutdown(self) -> None:
        self._ready = False


class TrackingInferenceSink(InferenceSink):
    """
    Wrapper around InferenceSink to count chunks submitted to AIEngine.
    """
    def __init__(self, engine: AIEngine):
        super().__init__(engine)
        self.chunks_submitted = 0
        self.generated_chunks = []

    async def write_chunk(self, chunk: SpeechChunk) -> None:
        self.generated_chunks.append(chunk)
        self.chunks_submitted += 1
        await super().write_chunk(chunk)


def generate_pcm_frames(total_frames: int, sample_rate: int = 16000, duration_sec: float = 0.02):
    """
    Pre-generates realistic PCM16 mono audio frames in memory.
    Alternates 25 speech frames (~0.5s audio above VAD threshold) with 5 silence frames (~0.1s silence).
    """
    samples_per_frame = int(sample_rate * duration_sec)
    
    t = np.linspace(0, duration_sec, samples_per_frame, endpoint=False)
    speech_signal = (8000 * np.sin(2 * np.pi * 440 * t)).astype(np.int16)
    speech_pcm = speech_signal.tobytes()
    silence_pcm = b'\x00' * (samples_per_frame * 2)

    frames_pcm = []
    for i in range(total_frames):
        if (i % 30) < 25:
            frames_pcm.append(speech_pcm)
        else:
            frames_pcm.append(silence_pcm)
            
    return frames_pcm


async def run_stage1_stage2_benchmark(num_frames: int):
    audio_config = AudioConfig()
    audio_telemetry = AudioTelemetry()
    
    ai_config = AIConfig(queue_maxsize=2000)
    ai_telemetry = AITelemetry()
    
    engine = AIEngine(config=ai_config, telemetry=ai_telemetry)
    mock_runtime = MockGemmaRuntime(config=ai_config)
    engine.runtime = mock_runtime

    requests_processed = 0
    ai_errors = 0

    def on_ai_event(event):
        nonlocal requests_processed, ai_errors
        if isinstance(event, AICompletedEvent):
            requests_processed += 1
        elif isinstance(event, AIErrorEvent):
            ai_errors += 1

    engine.register_listener(on_ai_event)

    sink = TrackingInferenceSink(engine)
    processor = StreamingSpeechProcessor(
        config=audio_config, 
        room_name="benchmark_stage1_stage2", 
        sink=sink, 
        telemetry=audio_telemetry
    )

    await processor.initialize()

    pcm_list = generate_pcm_frames(num_frames)

    frames_sent = 0
    frames_processed = 0
    frame_errors = 0

    start_time = time.perf_counter()

    for i in range(num_frames):
        frame = AudioFrame(
            frame_id=f"bench-{i}",
            sequence_number=i,
            participant_identity="bench_user",
            participant_session_id="bench_session_1",
            capture_timestamp_ns=time.perf_counter_ns(),
            queue_timestamp_ns=time.perf_counter_ns(),
            processing_timestamp_ns=time.perf_counter_ns(),
            sample_rate=16000,
            channels=1,
            frame_duration=0.02,
            pcm_data=pcm_list[i]
        )
        frames_sent += 1
        try:
            await processor.process_frame(frame)
            frames_processed += 1
        except Exception:
            frame_errors += 1

    await processor.flush()

    # Wait for the AI Engine's queue worker to drain completely
    await engine.queue.join()

    end_time = time.perf_counter()

    await processor.shutdown()

    elapsed_sec = end_time - start_time
    elapsed_ms = elapsed_sec * 1000.0
    throughput = frames_processed / elapsed_sec if elapsed_sec > 0 else 0.0
    dropped_frames = frames_sent - frames_processed
    total_errors = frame_errors + ai_errors

    return {
        "num_frames": num_frames,
        "frames_sent": frames_sent,
        "frames_processed": frames_processed,
        "dropped_frames": dropped_frames,
        "chunks_generated": len(sink.generated_chunks),
        "chunks_submitted": sink.chunks_submitted,
        "requests_created": mock_runtime.request_count,
        "requests_processed": requests_processed,
        "mock_responses": mock_runtime.response_count,
        "elapsed_ms": elapsed_ms,
        "throughput": throughput,
        "errors": total_errors
    }


async def main():
    test_loads = [1000, 5000, 10000]
    
    print("====================================")
    print("Stage 1 + Stage 2 Backend Benchmark")
    print("====================================")
    print()
    print("Fake PCM Frames")
    print("        |")
    print("        v")
    print("StreamingSpeechProcessor")
    print("        |")
    print("        v")
    print("Streaming VAD")
    print("        |")
    print("        v")
    print("Adaptive Chunk Builder")
    print("        |")
    print("        v")
    print("Streaming Context Manager")
    print("        |")
    print("        v")
    print("SpeechChunk")
    print("        |")
    print("        v")
    print("InferenceSink")
    print("        |")
    print("        v")
    print("AIEngine")
    print("        |")
    print("        v")
    print("Inference Queue")
    print("        |")
    print("        v")
    print("Queue Worker")
    print("        |")
    print("        v")
    print("RuntimeRequest Builder")
    print("        |")
    print("        v")
    print("Mock Gemma Runtime")
    print("        |")
    print("        v")
    print("Mock AI Response")
    print()
    print("------------------------------------")
    print()

    for idx, count in enumerate(test_loads):
        res = await run_stage1_stage2_benchmark(count)
        
        print(f"Test: {res['num_frames']:,} Frames")
        print(f"Frames Sent: {res['frames_sent']:,}")
        print(f"Frames Processed: {res['frames_processed']:,}")
        print(f"Dropped Frames: {res['dropped_frames']}")
        print(f"Speech Chunks Generated: {res['chunks_generated']:,}")
        print(f"Chunks submitted to AIEngine: {res['chunks_submitted']:,}")
        print(f"Inference Requests Created: {res['requests_created']:,}")
        print(f"Inference Requests Processed: {res['requests_processed']:,}")
        print(f"Mock Responses Returned: {res['mock_responses']:,}")
        print(f"Processing Time: ~{res['elapsed_ms']:.1f} ms")
        print(f"Throughput: {res['throughput']:,.2f} frames/sec")
        print(f"Errors: {res['errors']}")
        
        if idx < len(test_loads) - 1:
            print()
            print("------------------------------------")
            print()


if __name__ == "__main__":
    asyncio.run(main())
