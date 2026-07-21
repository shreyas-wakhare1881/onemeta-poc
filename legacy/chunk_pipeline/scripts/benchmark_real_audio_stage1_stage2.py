import os
import sys
import time
import wave
import asyncio
import logging
from typing import AsyncIterator

# Suppress verbose backend logs during benchmark to keep stdout clean
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
    Wrapper around InferenceSink to capture generated SpeechChunks and counts.
    """
    def __init__(self, engine: AIEngine):
        super().__init__(engine)
        self.chunks_submitted = 0
        self.generated_chunks = []

    async def write_chunk(self, chunk: SpeechChunk) -> None:
        self.generated_chunks.append(chunk)
        self.chunks_submitted += 1
        await super().write_chunk(chunk)


def load_and_resample_wav(file_path: str):
    """
    Reads a WAV file, converts to PCM16 Mono, and resamples to 16kHz if necessary.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Audio file not found: {file_path}")

    with wave.open(file_path, "rb") as wf:
        nchannels = wf.getnchannels()
        sampwidth = wf.getsampwidth()
        framerate = wf.getframerate()
        nframes = wf.getnframes()
        raw_bytes = wf.readframes(nframes)

    # Decode audio samples based on sample width
    if sampwidth == 2:
        samples = np.frombuffer(raw_bytes, dtype=np.int16)
        bit_depth = 16
    elif sampwidth == 1:
        raw_u8 = np.frombuffer(raw_bytes, dtype=np.uint8)
        samples = ((raw_u8.astype(np.int32) - 128) * 256).astype(np.int16)
        bit_depth = 8
    elif sampwidth == 4:
        samples_32 = np.frombuffer(raw_bytes, dtype=np.int32)
        samples = (samples_32 >> 16).astype(np.int16)
        bit_depth = 32
    else:
        raise ValueError(f"Unsupported WAV sample width: {sampwidth} bytes")

    # Convert Stereo / Multi-channel to Mono
    if nchannels > 1:
        samples = samples.reshape(-1, nchannels).mean(axis=1).astype(np.int16)

    # Resample to 16000 Hz if needed using linear interpolation
    if framerate != 16000:
        num_orig_samples = len(samples)
        num_target_samples = int(num_orig_samples * 16000 / framerate)
        orig_times = np.linspace(0, 1, num_orig_samples)
        target_times = np.linspace(0, 1, num_target_samples)
        samples = np.interp(target_times, orig_times, samples).astype(np.int16)

    pcm16_bytes = samples.tobytes()
    duration_sec = len(samples) / 16000.0

    return pcm16_bytes, duration_sec, framerate, nchannels, bit_depth


def split_into_audio_frames(pcm16_bytes: bytes, frame_duration_sec: float = 0.02, sample_rate: int = 16000) -> list:
    """
    Splits PCM16 audio bytes into 20ms AudioFrame dataclass objects with monotonic audio offsets.
    """
    bytes_per_frame = int(sample_rate * frame_duration_sec * 2) # 640 bytes for 20ms mono PCM16 at 16kHz
    frames = []
    total_bytes = len(pcm16_bytes)
    seq = 0

    for offset in range(0, total_bytes, bytes_per_frame):
        chunk = pcm16_bytes[offset:offset + bytes_per_frame]
        if len(chunk) < bytes_per_frame:
            chunk = chunk + b'\x00' * (bytes_per_frame - len(chunk))
        
        frame_timestamp_ns = int(seq * frame_duration_sec * 1_000_000_000)

        frame = AudioFrame(
            frame_id=f"real-audio-{seq}",
            sequence_number=seq,
            participant_identity="real_speaker",
            participant_session_id="real_session_1",
            capture_timestamp_ns=frame_timestamp_ns,
            queue_timestamp_ns=frame_timestamp_ns,
            processing_timestamp_ns=frame_timestamp_ns,
            sample_rate=16000,
            channels=1,
            frame_duration=0.02,
            pcm_data=chunk
        )
        frames.append(frame)
        seq += 1

    return frames


async def run_real_audio_benchmark(wav_path: str):
    pcm16_bytes, duration_sec, orig_rate, orig_channels, bit_depth = load_and_resample_wav(wav_path)
    frames = split_into_audio_frames(pcm16_bytes)
    num_frames = len(frames)

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
        room_name="real_audio_benchmark", 
        sink=sink, 
        telemetry=audio_telemetry
    )

    await processor.initialize()

    frames_sent = 0
    frames_processed = 0
    frame_errors = 0

    start_time = time.perf_counter()

    for frame in frames:
        frames_sent += 1
        try:
            await processor.process_frame(frame)
            frames_processed += 1
        except Exception:
            frame_errors += 1

    await processor.flush()
    await engine.queue.join()
    end_time = time.perf_counter()

    await processor.shutdown()

    elapsed_sec = end_time - start_time
    elapsed_ms = elapsed_sec * 1000.0
    throughput = frames_processed / elapsed_sec if elapsed_sec > 0 else 0.0
    dropped_frames = frames_sent - frames_processed
    total_errors = frame_errors + ai_errors

    return {
        "wav_path": wav_path,
        "sample_rate": orig_rate,
        "channels": orig_channels,
        "bit_depth": bit_depth,
        "duration_sec": duration_sec,
        "num_frames": num_frames,
        "frames_sent": frames_sent,
        "frames_processed": frames_processed,
        "dropped_frames": dropped_frames,
        "generated_chunks": sink.generated_chunks,
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
    wav_file_path = None
    if len(sys.argv) > 1:
        wav_file_path = sys.argv[1]
    else:
        default_sample = os.path.join(PROJECT_ROOT, "samples", "english_sample.wav")
        if os.path.exists(default_sample):
            wav_file_path = default_sample

    if not wav_file_path or not os.path.exists(wav_file_path):
        print("====================================")
        print("Real Audio Stage 1 + Stage 2 Benchmark")
        print("====================================")
        print()
        print("ERROR:")
        print("No microphone recording found.\n")
        print("Please place a real English microphone recording at:")
        print("samples/english_sample.wav\n")
        print("Or specify a path to a WAV file:")
        print("python scripts/benchmark_real_audio_stage1_stage2.py <path_to_wav_file>\n")
        print("Expected format:")
        print("- PCM16")
        print("- Mono")
        print("- 16kHz")
        print("- Real Human English Speech")
        sys.exit(1)

    res = await run_real_audio_benchmark(wav_file_path)

    print("====================================")
    print("Real Audio Stage 1 + Stage 2 Benchmark")
    print("====================================")
    print()
    print("1. WAV File Validation:")
    print(f"Audio File: {os.path.basename(res['wav_path'])}")
    print(f"Sample Rate: {res['sample_rate']} Hz")
    print(f"Channels: {res['channels']} ('Mono' if 1 else 'Stereo -> converted to Mono')")
    print(f"Bit Depth: {res['bit_depth']}-bit PCM")
    print(f"Duration: {res['duration_sec']:.2f} sec")
    print(f"Total Frames Generated: {res['num_frames']:,}")
    print()

    print("2. Stage 1 Execution (Audio Processing):")
    print(f"Frames Generated: {res['num_frames']:,}")
    print(f"Frames Processed: {res['frames_processed']:,}")
    print(f"Dropped Frames: {res['dropped_frames']}")
    print(f"Speech Chunks Generated: {res['chunks_generated']:,}")
    print()

    print("3. Stage 2 Execution (AI Infrastructure):")
    print(f"Chunks Submitted: {res['chunks_submitted']:,}")
    print(f"Runtime Requests Created: {res['requests_created']:,}")
    print(f"Runtime Requests Processed: {res['requests_processed']:,}")
    print(f"Mock Responses Returned: {res['mock_responses']:,}")
    print()

    print("4. Performance & Metrics:")
    print(f"Backend Processing Time: ~{res['elapsed_ms']:.2f} ms")
    print(f"Throughput: {res['throughput']:,.2f} frames/sec")
    print(f"Errors: {res['errors']}")
    print()

    print("------------------------------------")
    print("5. Speech Chunk Summary")
    print("------------------------------------")
    print()

    if res['generated_chunks']:
        for idx, chunk in enumerate(res['generated_chunks'], 1):
            start_s = chunk.start_timestamp
            end_s = chunk.end_timestamp
            dur_s = chunk.duration_ms / 1000.0
            print(f"Chunk {idx}: Start={start_s:.2f}s | End={end_s:.2f}s | Duration={dur_s:.2f}s | Frames={chunk.frame_count}")
        print()
    else:
        print("No speech chunks detected in audio sample.")
        print()

    # Pipeline Consistency Checklist
    c1 = (res['frames_processed'] == res['num_frames'])
    c2 = (res['dropped_frames'] == 0)
    c3 = (res['chunks_submitted'] == res['chunks_generated'])
    c4 = (res['requests_created'] == res['mock_responses'])
    c5 = (res['errors'] == 0)

    is_pass = c1 and c2 and c3 and c4 and c5

    print("------------------------------------")
    print("6. Final Validation Checklist:")
    print(f"  [OK] Frames Processed == Frames Generated: {c1} ({res['frames_processed']} / {res['num_frames']})")
    print(f"  [OK] Dropped Frames == 0: {c2} ({res['dropped_frames']})")
    print(f"  [OK] Chunks Submitted == Speech Chunks Generated: {c3} ({res['chunks_submitted']} / {res['chunks_generated']})")
    print(f"  [OK] Runtime Requests Created == Mock Responses Returned: {c4} ({res['requests_created']} / {res['mock_responses']})")
    print(f"  [OK] Zero Errors: {c5} ({res['errors']} errors)")
    print("------------------------------------")
    if is_pass:
        print("RESULT: PASS")
    else:
        print("RESULT: FAIL")
        if not c1: print("  - Reason: Frame count mismatch")
        if not c2: print("  - Reason: Dropped frames detected")
        if not c3: print("  - Reason: Speech chunk submission mismatch")
        if not c4: print("  - Reason: Request/Response count mismatch")
        if not c5: print("  - Reason: Processing errors occurred")
    print("------------------------------------")


if __name__ == "__main__":
    asyncio.run(main())
