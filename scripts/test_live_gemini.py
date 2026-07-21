import os
import sys
import wave
import time
import asyncio
import base64
import numpy as np
from pathlib import Path
from dotenv import load_dotenv

# Load env variables from backend/.env
workspace_root = Path(__file__).resolve().parents[1]
env_path = workspace_root / "backend" / ".env"
load_dotenv(dotenv_path=env_path)

# Insert backend directory to python path
backend_dir = workspace_root / "backend"
sys.path.insert(0, str(backend_dir))

from app.ai.config import AIConfig
from app.ai.engine import AIEngine
from app.ai.runtimes.gemini_live_runtime import GeminiLiveRuntime
from app.transport.packet import StreamingAudioPacket, StreamingPacketMetadata
from app.ai.events import (
    StreamingPartialTranslationEvent,
    StreamingTranslationCompletedEvent,
    StreamingRuntimeErrorEvent
)

def load_and_resample_wav(filepath: str) -> bytes:
    """
    Reads a stereo 48kHz WAV file and downsamples it to mono 16kHz PCM16 bytes.
    """
    print(f"Loading and decimation filtering: {filepath}...")
    with wave.open(filepath, 'rb') as w:
        nchannels = w.getnchannels()
        sampwidth = w.getsampwidth()
        framerate = w.getframerate()
        nframes = w.getnframes()
        raw_bytes = w.readframes(nframes)
        
        data = np.frombuffer(raw_bytes, dtype=np.int16)
        
        # Stereo to Mono
        if nchannels == 2:
            left = data[0::2]
            right = data[1::2]
            mono = (left.astype(np.float32) + right.astype(np.float32)) / 2
            data = mono.astype(np.int16)
            
        # 48kHz to 16kHz downsampling (factor of 3 decimation with anti-aliasing filter)
        if framerate == 48000:
            length = (len(data) // 3) * 3
            data = data[:length]
            reshaped = data.reshape(-1, 3).astype(np.float32)
            data = np.mean(reshaped, axis=1)
            
        # Normalize/Amplify to peak of 20000
        peak = np.max(np.abs(data))
        if peak > 0:
            data = data * (20000.0 / peak)
            
        data = data.astype(np.int16)
        return data.tobytes()

async def main():
    print("=== ONE META GEMINI LIVE REAL CONNECTION TEST ===")
    
    # 1. Read API credentials
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("ERROR: GOOGLE_API_KEY environment variable is not set in backend/.env!")
        return

    # Check for wav sample
    sample_path = workspace_root / "samples" / "english_sample.wav"
    if not sample_path.exists():
        print(f"ERROR: Sample audio file not found at: {sample_path}")
        return

    # Downsample audio
    pcm_bytes = load_and_resample_wav(str(sample_path))
    print(f"Resampled audio size: {len(pcm_bytes)} bytes ({len(pcm_bytes)/32000:.2f} seconds)")

    # 2. Configure AIConfig
    ai_config = AIConfig(
        streaming_runtime="gemini_live",
        google_api_key=api_key
    )
    ai_engine = AIEngine(ai_config)

    # State tracking
    t_connect_start = time.perf_counter()
    t_first_token = None
    t_completed = None
    
    events_received = []

    def handle_session_event(event):
        nonlocal t_first_token, t_completed
        events_received.append(event)
        
        if isinstance(event, StreamingPartialTranslationEvent):
            if t_first_token is None:
                t_first_token = time.perf_counter()
                ttft = (t_first_token - t_connect_start) * 1000.0
                print(f"\n[TTFT Metrics] Time to First Token: {ttft:.2f} ms")
            print(f"Partial delta: '{event.text_delta}' | Cumulative: '{event.cumulative_text}'")
            
        elif isinstance(event, StreamingTranslationCompletedEvent):
            t_completed = time.perf_counter()
            duration = (t_completed - t_connect_start) * 1000.0
            print(f"\n[Completion Event] Translation Finished. Text: '{event.full_text}'")
            print(f"Total time elapsed: {duration:.2f} ms")
            
        elif isinstance(event, StreamingRuntimeErrorEvent):
            print(f"\n[Error Event] Session Runtime Error: {event.error_message}")

    print("\nStarting AI Engine...")
    await ai_engine.start()

    print("Connecting to real Gemini Live WebSocket endpoint...")
    runtime = GeminiLiveRuntime(ai_config)
    await runtime.initialize()
    
    session_id = "live-real-test-session"
    
    # Establish Connection
    session = await ai_engine.start_streaming_session(
        session_id=session_id,
        runtime=runtime,
        source_lang="English",
        target_lang="Spanish"
    )
    session.register_listener(handle_session_event)
    
    conn_time = (time.perf_counter() - t_connect_start) * 1000.0
    print(f"Handshake connected in: {conn_time:.2f} ms")
    
    print("\nStreaming real 20ms PCM audio packets (simulated real-time)...")
    # A 20ms frame at 16kHz mono is 320 samples of 2 bytes = 640 bytes
    frame_size = 640
    num_frames = len(pcm_bytes) // frame_size
    
    # Simulate speech started boundary in telemetry
    session.record_speech_start()
    
    # Stream first 3 seconds of audio (150 frames) to prevent interruption
    frames_to_stream = min(150, num_frames)
    
    for i in range(frames_to_stream):
        offset = i * frame_size
        chunk_data = pcm_bytes[offset : offset + frame_size]
        
        # Build zero-copy memoryview packet wrapper
        mv = memoryview(chunk_data)
        packet = StreamingAudioPacket(
            pcm_data=mv,
            sample_rate=16000,
            channels=1,
            capture_timestamp_ns=time.perf_counter_ns(),
            sequence_number=i + 1,
            is_speech=True,
            metadata=StreamingPacketMetadata(
                frame_id=f"frame-{i}",
                participant_identity="tester",
                participant_session_id="tester-sid",
                rms=1000.0,
                correlation_id="realtime-corr-1"
            )
        )
        
        await ai_engine.process_audio_packet(session_id, packet)
        
        # Sleep exactly 20ms to simulate real-time ingestion
        await asyncio.sleep(0.02)
        
        # Keep process awake while receiving
        if i % 100 == 0:
            print(f"-> Streamed {i} frames...")

    # Signal end of speaking segment to trigger generation
    session.record_speech_end()

    # Wait 3 seconds to let final translations finish
    print("\nFinished sending audio. Waiting for remaining translation responses...")
    await asyncio.sleep(4.0)

    print("\nClosing session...")
    await ai_engine.stop_streaming_session(session_id)
    await runtime.shutdown()
    await ai_engine.shutdown()
    
    print("\n=== Latency Summary ===")
    print(f"WebSocket Connect Time: {conn_time:.2f} ms")
    if t_first_token:
        print(f"Time to First Token (TTFT): {(t_first_token - t_connect_start)*1000.0:.2f} ms")
    if t_completed:
        print(f"Total Turn Completion Latency: {(t_completed - t_connect_start)*1000.0:.2f} ms")
    
    metrics = session.get_metrics()
    print(f"Queue Average Wait Delay: {metrics.queue_wait_ms:.2f} ms")
    print(f"Average Frame Delay: {metrics.avg_frame_delay_ms:.2f} ms")
    print("Test finished successfully.")

if __name__ == "__main__":
    asyncio.run(main())
