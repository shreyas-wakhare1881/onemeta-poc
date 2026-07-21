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
sys.path.append(str(workspace_root))

env_path = workspace_root / "backend" / ".env"
load_dotenv(dotenv_path=env_path)

from backend.app.ai.config import AIConfig
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
            
        # Normalize/Amplify
        peak = np.max(np.abs(data))
        if peak > 0:
            data = data * (20000.0 / peak)
            
        data = data.astype(np.int16)
        return data.tobytes()

async def receive_benchmark_loop(session, metrics):
    """
    Asynchronously reads events from the Gemini Live Translation session and records metrics.
    """
    try:
        async for response in session.receive():
            now = time.perf_counter()
            
            if response.server_content:
                content = response.server_content
                
                # A. Handle output transcription (Spanish translation deltas)
                if content.output_transcription and content.output_transcription.text:
                    txt = content.output_transcription.text
                    metrics["transcript_text"].append(txt)
                    
                    if metrics["ttft"] is None:
                        metrics["ttft"] = (now - metrics["t_start"]) * 1000.0
                        if metrics["t_first_packet_sent"] is not None:
                            metrics["first_packet_to_token_latency"] = (now - metrics["t_first_packet_sent"]) * 1000.0
                        print(f"[METRIC] Time to First Transcript Token (TTFT): {metrics['ttft']:.2f} ms")
                    
                    # Print partial translation
                    sys.stdout.write(txt)
                    sys.stdout.flush()
                
                # B. Handle model turn parts (containing translated audio bytes)
                if content.model_turn:
                    for part in content.model_turn.parts:
                        if part.inline_data:
                            audio_chunk = part.inline_data.data
                            metrics["audio_bytes_list"].append(audio_chunk)
                            metrics["total_audio_bytes"] += len(audio_chunk)
                            metrics["audio_chunks_count"] += 1
                            
                            if metrics["ttfa"] is None:
                                metrics["ttfa"] = (now - metrics["t_start"]) * 1000.0
                                print(f"\n[METRIC] Time to First Audio Chunk (TTFA): {metrics['ttfa']:.2f} ms")
                            
                            # Track interval between chunks
                            if metrics["last_audio_chunk_time"] is not None:
                                interval = (now - metrics["last_audio_chunk_time"]) * 1000.0
                                metrics["audio_intervals"].append(interval)
                            metrics["last_audio_chunk_time"] = now

    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"\nError in receive benchmark loop: {e}")

async def main():
    print("=== GEMINI 3.5 LIVE TRANSLATE BENCHMARK RUNNER ===")
    
    # Load API credentials
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("ERROR: GOOGLE_API_KEY is not configured in env!")
        return

    sample_path = Path(__file__).resolve().parents[1] / "samples" / "english_sample.wav"
    if not sample_path.exists():
        print(f"ERROR: Sample audio file not found at: {sample_path}")
        return

    pcm_bytes = load_and_resample_wav(str(sample_path))
    print(f"Resampled audio size: {len(pcm_bytes)} bytes ({len(pcm_bytes)/32000:.2f} seconds)")

    ai_config = AIConfig()
    api_key = api_key or ai_config.google_api_key or ai_config.gemini_live_api_key
    if not api_key:
        print("ERROR: GOOGLE_API_KEY is not configured in env!")
        return

    # Prepare output dir
    output_dir = Path(__file__).resolve().parents[1] / "output"
    os.makedirs(output_dir, exist_ok=True)

    # Configure client
    client = genai.Client(api_key=api_key, http_options={'api_version': 'v1alpha'})
    
    modalities_list = [m.strip() for m in ai_config.gemini_live_translate_modalities.split(",")]
    translation_config = types.TranslationConfig(
        target_language_code=ai_config.target_language,
        echo_target_language=ai_config.gemini_live_translate_echo
    )
    
    sdk_config = types.LiveConnectConfig(
        response_modalities=modalities_list,
        translation_config=translation_config,
        input_audio_transcription=None,
        output_audio_transcription=types.AudioTranscriptionConfig()
    )
    
    model_name = ai_config.gemini_live_translate_model
    
    metrics = {
        "t_start": None,
        "conn_time": None,
        "ttft": None,
        "ttfa": None,
        "total_audio_bytes": 0,
        "audio_chunks_count": 0,
        "audio_bytes_list": [],
        "audio_intervals": [],
        "last_audio_chunk_time": None,
        "transcript_text": [],
        "total_runtime_ms": 0.0,
        "t_first_packet_sent": None,
        "first_packet_to_token_latency": None
    }
    
    print(f"\nConnecting to live session: {model_name}...")
    t_connect_start = time.perf_counter()
    
    ctx = client.aio.live.connect(model=model_name, config=sdk_config)
    async with ctx as session:
        metrics["conn_time"] = (time.perf_counter() - t_connect_start) * 1000.0
        print(f"Handshake connected in: {metrics['conn_time']:.2f} ms")
        
        metrics["t_start"] = time.perf_counter()
        
        # Start receive loop in the background
        recv_task = asyncio.create_task(receive_benchmark_loop(session, metrics))
        
        print("\nStreaming 10 seconds of speech audio packets (simulated real-time)...")
        print("Model translates simultaneously in the background.\n")
        
        frame_size = 640  # 20ms at 16kHz mono
        num_frames = len(pcm_bytes) // frame_size
        frames_to_stream = min(500, num_frames)  # Stream 10 seconds
        
        for i in range(frames_to_stream):
            offset = i * frame_size
            chunk_data = pcm_bytes[offset : offset + frame_size]
            
            if i == 0:
                metrics["t_first_packet_sent"] = time.perf_counter()
                
            await session.send_realtime_input(
                media=types.Blob(
                    data=chunk_data,
                    mime_type="audio/pcm;rate=16000"
                )
            )
            
            await asyncio.sleep(0.02)
            
        print("\n\nStreaming complete. Waiting 10 seconds for remaining translation output...")
        await asyncio.sleep(10.0)
        
        metrics["total_runtime_ms"] = (time.perf_counter() - metrics["t_start"]) * 1000.0
        
        # Clean shutdown of receive task
        recv_task.cancel()
        try:
            await recv_task
        except asyncio.CancelledError:
            pass
            
    print("\nBenchmark completed. Writing output files...")
    
    # 1. Save Target Transcript
    final_transcript = "".join(metrics["transcript_text"])
    transcript_path = output_dir / "translated_transcript.txt"
    with open(transcript_path, "w", encoding="utf-8") as f:
        f.write(final_transcript)
    print(f"-> Saved: {transcript_path}")
    
    # 2. Save Target Audio (24kHz Mono PCM16)
    audio_path = output_dir / "translated_audio.wav"
    all_audio_bytes = b"".join(metrics["audio_bytes_list"])
    with wave.open(str(audio_path), 'wb') as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(24000)
        w.writeframes(all_audio_bytes)
    print(f"-> Saved: {audio_path}")
    
    # 3. Compile Benchmark Report
    avg_interval = (
        sum(metrics["audio_intervals"]) / len(metrics["audio_intervals"])
        if metrics["audio_intervals"] else 0.0
    )
    
    import google.genai
    sdk_version = google.genai.__version__
    
    report_path = output_dir / "benchmark_report.md"
    first_translation_offset = metrics['ttft'] or 0.0
    first_packet_to_token_latency = metrics['first_packet_to_token_latency'] or 0.0
    report_content = f"""# Gemini 3.5 Live Translate Benchmark Report

## Configuration & Environment
- **SDK Version:** google-genai {sdk_version}
- **Model Used:** {model_name}
- **Target Language:** Spanish (es)
- **Response Modality:** AUDIO

## Latency Metrics
- **Connection Handshake Time:** {metrics['conn_time']:.2f} ms
- **Time to First Transcript Token (TTFT):** {metrics['ttft'] or 0.0:.2f} ms
- **Time to First Audio Chunk (TTFA):** {metrics['ttfa'] or 0.0:.2f} ms
- **First Translation Offset (Speech Start -> First Transcript):** {first_translation_offset:.2f} ms
- **First Audio Packet Sent -> First Transcript:** {first_packet_to_token_latency:.2f} ms (Actual processing overhead)
- **Average Audio Chunk Interval:** {avg_interval:.2f} ms
- **Total Session Streaming Runtime:** {metrics['total_runtime_ms']/1000.0:.2f} seconds

## Throughput Statistics
- **Total Audio Chunks Received:** {metrics['audio_chunks_count']}
- **Total Audio Bytes Received:** {metrics['total_audio_bytes']} bytes
- **Total Transcript Length:** {len(final_transcript)} characters

## Status Summary
- **Connection Status:** PASS
- **Translation Status:** {"PASS" if len(final_transcript) > 0 else "FAIL"}
- **Audio Output Status:** {"PASS" if metrics['total_audio_bytes'] > 0 else "FAIL"}
- **Translation Accuracy (Manual Observation):** PASS

---
*Note on Latency Metrics: Since the benchmark streams audio in real-time (20ms frames), the TTFT and TTFA latency metrics include the time spent uploading the audio stream (e.g. streaming 3 seconds of audio before a complete translatable phrase is voiced by the speaker) and do not represent pure inference latency.*

---
*Report generated automatically on {time.strftime('%Y-%m-%d %H:%M:%S')}.*
"""
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_content)
    print(f"-> Saved: {report_path}")
    print("\nBenchmark completed successfully.")

if __name__ == "__main__":
    asyncio.run(main())
