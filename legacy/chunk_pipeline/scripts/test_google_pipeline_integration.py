import os
import sys
from dotenv import load_dotenv

# Ensure backend directory is in sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
BACKEND_DIR = os.path.join(PROJECT_ROOT, "backend")

# Load environment first
load_dotenv(os.path.join(BACKEND_DIR, ".env"))

if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

import time
import wave
import asyncio
import logging
import numpy as np

# Suppress verbose backend logs during validation
logging.getLogger("onemeta").setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

from app.types.audio import AudioFrame
from app.types.speech import SpeechChunk
from app.audio.config import AudioConfig
from app.audio.telemetry import AudioTelemetry
from app.audio.processor import StreamingSpeechProcessor
from app.ai.config import AIConfig
from app.ai.telemetry import AITelemetry
from app.ai.engine import AIEngine
from app.ai.sink import InferenceSink
from app.ai.events import AICompletedEvent, AIErrorEvent


async def main():
    print("==================================================")
    # 1. Load env and verify configuration
    load_dotenv(os.path.join(BACKEND_DIR, ".env"))
    
    ai_config = AIConfig(queue_maxsize=100)
    print("Google Gemini Pipeline Integration Test")
    print("==================================================")
    print(f"Runtime Type : {ai_config.runtime_type}")
    print(f"Google Model : {ai_config.google_model}")
    print("==================================================\n")

    if ai_config.runtime_type != "google":
        print("ERROR: AI_RUNTIME_TYPE is not set to 'google' in backend/.env.")
        sys.exit(1)

    wav_path = os.path.join(PROJECT_ROOT, "samples", "english_sample.wav")
    if not os.path.exists(wav_path):
        print(f"ERROR: Audio file not found at: {wav_path}")
        sys.exit(1)

    # 2. Setup AIEngine and configurations
    ai_telemetry = AITelemetry()
    engine = AIEngine(config=ai_config, telemetry=ai_telemetry)

    # Track translations
    translations_received = []

    def on_ai_event(event):
        if isinstance(event, AICompletedEvent):
            print(f"\n[Translation Received] (Chunk {event.sequence_number}):")
            print(f"Text: {event.full_text}")
            translations_received.append(event.full_text)
        elif isinstance(event, AIErrorEvent):
            print(f"\n[AI Error] Chunk {event.sequence_number}: {event.error_message}")

    engine.register_listener(on_ai_event)

    # 3. Setup Audio Stage 1 components
    audio_config = AudioConfig()
    audio_telemetry = AudioTelemetry()
    sink = InferenceSink(engine)
    processor = StreamingSpeechProcessor(
        config=audio_config,
        room_name="integration_test_room",
        sink=sink,
        telemetry=audio_telemetry
    )

    # 4. Initialize processor (which starts InferenceSink and AIEngine)
    await processor.initialize()

    # 5. Read and feed WAV file
    with wave.open(wav_path, "rb") as wf:
        nchannels = wf.getnchannels()
        sampwidth = wf.getsampwidth()
        framerate = wf.getframerate()
        nframes = wf.getnframes()
        raw_bytes = wf.readframes(nframes)

    # convert stereo to mono, resample if necessary
    import numpy as np
    samples = np.frombuffer(raw_bytes, dtype=np.int16)
    if nchannels > 1:
        samples = samples.reshape(-1, nchannels).mean(axis=1).astype(np.int16)
    if framerate != 16000:
        num_orig_samples = len(samples)
        num_target_samples = int(num_orig_samples * 16000 / framerate)
        orig_times = np.linspace(0, 1, num_orig_samples)
        target_times = np.linspace(0, 1, num_target_samples)
        samples = np.interp(target_times, orig_times, samples).astype(np.int16)

    pcm16_bytes = samples.tobytes()
    frame_bytes_size = 640 # 20ms mono PCM16 @ 16kHz
    total_bytes = len(pcm16_bytes)
    
    print("Feeding audio frames into production pipeline...")
    seq = 0
    for offset in range(0, total_bytes, frame_bytes_size):
        chunk = pcm16_bytes[offset:offset + frame_bytes_size]
        if len(chunk) < frame_bytes_size:
            chunk = chunk + b'\x00' * (frame_bytes_size - len(chunk))
        
        frame = AudioFrame(
            frame_id=f"integration-{seq}",
            sequence_number=seq,
            participant_identity="integration_speaker",
            participant_session_id="integration_session",
            capture_timestamp_ns=int(seq * 0.02 * 1_000_000_000),
            queue_timestamp_ns=int(seq * 0.02 * 1_000_000_000),
            processing_timestamp_ns=int(seq * 0.02 * 1_000_000_000),
            sample_rate=16000,
            channels=1,
            frame_duration=0.02,
            pcm_data=chunk
        )
        await processor.process_frame(frame)
        await asyncio.sleep(0.02)
        seq += 1

    print("Flushing audio pipeline buffers...")
    await processor.flush()

    print("Waiting for AIEngine inference queue to drain...")
    await engine.queue.join()

    print("Shutting down pipeline...")
    await processor.shutdown()
    
    print("\n==================================================")
    print("INTEGRATION VERIFICATION RESULTS")
    print("==================================================")
    print(f"Total Speech Chunks Generated: {len(translations_received)}")
    print("==================================================")
    if translations_received:
        print("RESULT: PASS")
    else:
        print("RESULT: FAIL")
    print("==================================================")


if __name__ == "__main__":
    asyncio.run(main())
