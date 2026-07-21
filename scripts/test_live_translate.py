import os
import sys
import wave
import time
import asyncio
import numpy as np
from pathlib import Path
from dotenv import load_dotenv

# Set paths
workspace_root = Path(__file__).resolve().parents[1]
env_path = workspace_root / "backend" / ".env"
load_dotenv(dotenv_path=env_path)

from google import genai
from google.genai import types

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
            
        # 48kHz to 16kHz downsampling
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

async def receive_loop(session, t_start):
    """
    Asynchronously reads events from the Gemini Live Translation session.
    """
    print("Listening for translation events...")
    audio_chunks_received = 0
    total_audio_bytes = 0
    text_received = []
    
    try:
        async for response in session.receive():
            elapsed = (time.perf_counter() - t_start) * 1000.0
            
            if response.server_content:
                content = response.server_content
                
                # A. Log audio transcription text (Spanish translation deltas)
                if content.output_transcription and content.output_transcription.text:
                    txt = content.output_transcription.text
                    text_received.append(txt)
                    print(f"[{elapsed:.1f}ms] TRANSCRIPT PARTIAL: '{txt}' (Cumulative: '{''.join(text_received)}')")
                
                # B. Log model turn parts (could contain audio data)
                if content.model_turn:
                    for part in content.model_turn.parts:
                        if part.inline_data:
                            audio_chunks_received += 1
                            total_audio_bytes += len(part.inline_data.data)
                            if audio_chunks_received % 10 == 0:
                                print(f"[{elapsed:.1f}ms] AUDIO: Received {audio_chunks_received} audio chunks ({total_audio_bytes} total bytes)")
                        if part.text:
                            print(f"[{elapsed:.1f}ms] TEXT PART: '{part.text}'")
                            
                # C. Turn complete boundary
                if content.turn_complete:
                    print(f"[{elapsed:.1f}ms] TURN COMPLETE")
                    
                # D. Interrupted boundary
                if content.interrupted:
                    print(f"[{elapsed:.1f}ms] INTERRUPTION DETECTED")
                    
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"Error in receive loop: {e}")
        
    return text_received, audio_chunks_received, total_audio_bytes

async def main():
    print("=== ONE META GEMINI 3.5 LIVE TRANSLATE TEST ===")
    
    # 1. Read API key
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("ERROR: GOOGLE_API_KEY not configured in env!")
        return

    # Check for wav sample
    sample_path = Path(__file__).resolve().parents[1] / "samples" / "english_sample.wav"
    if not sample_path.exists():
        print(f"ERROR: Sample audio file not found at: {sample_path}")
        return

    # Downsample audio
    pcm_bytes = load_and_resample_wav(str(sample_path))
    print(f"Resampled audio size: {len(pcm_bytes)} bytes ({len(pcm_bytes)/32000:.2f} seconds)")

    # 2. Configure GenAI client targeting v1alpha for live connect
    client = genai.Client(api_key=api_key, http_options={'api_version': 'v1alpha'})
    
    # Configure translation target to Spanish
    translation_config = types.TranslationConfig(
        target_language_code="es",
        echo_target_language=True
    )
    
    # LiveConnectConfig optimized for continuous translation (AUDIO modality output)
    sdk_config = types.LiveConnectConfig(
        response_modalities=["AUDIO"],
        translation_config=translation_config,
        output_audio_transcription=types.AudioTranscriptionConfig()
    )
    
    model_name = "models/gemini-3.5-live-translate-preview"
    
    print(f"Connecting to live session with model: {model_name}...")
    t_start = time.perf_counter()
    
    ctx = client.aio.live.connect(model=model_name, config=sdk_config)
    async with ctx as session:
        conn_time = (time.perf_counter() - t_start) * 1000.0
        print(f"Connected successfully in: {conn_time:.2f} ms")
        
        # Start receive loop in the background
        recv_task = asyncio.create_task(receive_loop(session, t_start))
        
        print("\nStreaming 10 seconds of speech audio packets (simulated real-time)...")
        print("No explicit end_user_turn or ActivityEnd will be sent during streaming to test continuous behavior.")
        
        frame_size = 640  # 20ms at 16kHz mono
        num_frames = len(pcm_bytes) // frame_size
        frames_to_stream = min(500, num_frames)  # Stream 10 seconds
        
        for i in range(frames_to_stream):
            offset = i * frame_size
            chunk_data = pcm_bytes[offset : offset + frame_size]
            
            # Send the audio chunk to the live connect session
            await session.send_realtime_input(
                media=types.Blob(
                    data=chunk_data,
                    mime_type="audio/pcm;rate=16000"
                )
            )
            
            await asyncio.sleep(0.02)
            if (i + 1) % 100 == 0:
                print(f"-> Streamed {i+1} frames ({(i+1)*20}ms)...")
                
        print("\nStreaming complete. Waiting 10 seconds to see if translation is returned continuously...")
        await asyncio.sleep(10.0)
        
        # Cancel receive task and finalize
        recv_task.cancel()
        try:
            await recv_task
        except asyncio.CancelledError:
            pass
            
    print("\nTest completed successfully.")

if __name__ == "__main__":
    asyncio.run(main())
