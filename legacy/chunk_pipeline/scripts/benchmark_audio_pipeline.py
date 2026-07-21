import os
import sys
import time
import asyncio
import numpy as np

# Ensure backend directory is in sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
BACKEND_DIR = os.path.join(PROJECT_ROOT, "backend")

if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from app.types.audio import AudioFrame
from app.types.speech import SpeechChunk
from app.audio.config import AudioConfig
from app.audio.telemetry import AudioTelemetry
from app.audio.sink import BaseSpeechChunkSink
from app.audio.processor import StreamingSpeechProcessor


class BenchmarkSink(BaseSpeechChunkSink):
    """
    In-memory sink for collecting and counting generated SpeechChunks during benchmarking.
    """
    def __init__(self):
        self.chunks = []

    async def write_chunk(self, chunk: SpeechChunk) -> None:
        self.chunks.append(chunk)

    @property
    def chunk_count(self) -> int:
        return len(self.chunks)


def generate_pcm_frames(total_frames: int, sample_rate: int = 16000, duration_sec: float = 0.02):
    """
    Pre-generates realistic PCM16 mono frames in memory.
    Simulates alternating speech patterns (25 speech frames + 5 silence frames)
    so VAD and ChunkBuilder trigger realistic speech chunk flushes.
    """
    samples_per_frame = int(sample_rate * duration_sec)
    
    # 1. Speech frame: Sine wave with amplitude ~8000 (well above VAD start energy threshold of 550)
    t = np.linspace(0, duration_sec, samples_per_frame, endpoint=False)
    speech_signal = (8000 * np.sin(2 * np.pi * 440 * t)).astype(np.int16)
    speech_pcm = speech_signal.tobytes()

    # 2. Silence frame: All zeroes
    silence_pcm = b'\x00' * (samples_per_frame * 2)

    frames_pcm = []
    for i in range(total_frames):
        # 25 frames speech (~0.5s), 5 frames silence (~0.1s)
        if (i % 30) < 25:
            frames_pcm.append(speech_pcm)
        else:
            frames_pcm.append(silence_pcm)
            
    return frames_pcm


async def run_single_benchmark(num_frames: int):
    config = AudioConfig()
    telemetry = AudioTelemetry()
    sink = BenchmarkSink()
    processor = StreamingSpeechProcessor(config, room_name="benchmark_room", sink=sink, telemetry=telemetry)
    
    await processor.initialize()
    
    pcm_list = generate_pcm_frames(num_frames)
    
    frames_sent = 0
    frames_processed = 0
    errors = 0
    
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
            errors += 1

    # Flush remaining buffered frames
    await processor.flush()
    await processor.shutdown()
    
    end_time = time.perf_counter()
    elapsed_sec = end_time - start_time
    elapsed_ms = elapsed_sec * 1000.0
    throughput = frames_processed / elapsed_sec if elapsed_sec > 0 else 0.0
    dropped_frames = frames_sent - frames_processed

    return {
        "num_frames": num_frames,
        "frames_sent": frames_sent,
        "frames_processed": frames_processed,
        "dropped_frames": dropped_frames,
        "chunks_generated": sink.chunk_count,
        "elapsed_ms": elapsed_ms,
        "throughput": throughput,
        "errors": errors
    }


async def main():
    test_loads = [1000, 5000, 10000]
    results = []

    print("====================================")
    print("Backend Audio Pipeline Stress Test")
    print("====================================")
    print()

    for idx, count in enumerate(test_loads):
        res = await run_single_benchmark(count)
        results.append(res)
        
        print(f"Test: {res['num_frames']:,} Frames")
        print(f"Frames Sent: {res['frames_sent']:,}")
        print(f"Frames Processed: {res['frames_processed']:,}")
        print(f"Dropped Frames: {res['dropped_frames']}")
        print(f"Speech Chunks Generated: {res['chunks_generated']:,}")
        print(f"Processing Time: ~{res['elapsed_ms']:.1f} ms")
        print(f"Throughput: {res['throughput']:,.2f} frames/sec")
        
        if idx < len(test_loads) - 1:
            print()
            print("------------------------------------")
            print()


if __name__ == "__main__":
    asyncio.run(main())
