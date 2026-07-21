import os
import sys
import time
import base64
import json
import wave
import struct
import urllib.request
import requests

def download_sample_audio(target_path="harvard.wav"):
    """
    Downloads the reference speech file if not present.
    """
    if not os.path.exists(target_path):
        url = "https://raw.githubusercontent.com/claudiofepereira/speech-to-text/master/harvard.wav"
        print(f"[*] Downloading reference speech WAV from {url}...")
        urllib.request.urlretrieve(url, target_path)
        print("[+] Download complete.")

def resample_to_16k_mono(input_path="harvard.wav", output_path="harvard_16k_mono.wav"):
    """
    Converts 44.1kHz stereo to 16kHz mono PCM WAV.
    """
    if os.path.exists(output_path):
        return

    print("[*] Resampling reference WAV to 16kHz mono...")
    src = wave.open(input_path, 'rb')
    sr = src.getframerate()
    n_channels = src.getnchannels()
    sampwidth = src.getsampwidth()
    n_frames = src.getnframes()
    frames = src.readframes(n_frames)
    src.close()

    # Unpack PCM data (assumed 16-bit)
    if sampwidth != 2:
        raise ValueError("Only 16-bit WAV files supported for resampling.")

    data = list(struct.unpack(f"<{n_frames * n_channels}h", frames))
    
    # Average channels if stereo
    mono = [(data[i] + data[i+1]) // 2 for i in range(0, len(data), 2)] if n_channels == 2 else data
    
    # Resample
    ratio = sr / 16000
    resampled = [mono[int(i * ratio)] for i in range(int(n_frames / ratio)) if int(i * ratio) < len(mono)]
    
    dst = wave.open(output_path, 'wb')
    dst.setnchannels(1)
    dst.setsampwidth(2)
    dst.setframerate(16000)
    dst.writeframes(struct.pack(f"<{len(resampled)}h", *resampled))
    dst.close()
    print("[+] Resampling complete.")

def get_wav_slice(input_path, duration_ms):
    """
    Reads a slice of the 16kHz mono WAV file and returns WAV container bytes.
    """
    src = wave.open(input_path, 'rb')
    sr = src.getframerate() # Should be 16000
    
    # 16000 samples/sec * (duration_ms / 1000) = total samples
    num_samples = int(sr * (duration_ms / 1000.0))
    frames = src.readframes(num_samples)
    src.close()

    # Wrap in in-memory WAV container
    import io
    wav_io = io.BytesIO()
    dst = wave.open(wav_io, 'wb')
    dst.setnchannels(1)
    dst.setsampwidth(2)
    dst.setframerate(sr)
    dst.writeframes(frames)
    dst.close()
    
    return wav_io.getvalue()

def profile_chunk(duration_ms, wav_path="harvard_16k_mono.wav"):
    print(f"\n==========================================")
    print(f"Profiling {duration_ms} ms Audio Segment")
    print(f"==========================================")
    
    # 1. Get sliced WAV bytes
    wav_bytes = get_wav_slice(wav_path, duration_ms)
    audio_base64 = base64.b64encode(wav_bytes).decode("utf-8")
    
    url = "http://localhost:11434/api/generate"
    payload = {
        "model": "gemma4:12b",
        "prompt": "Transcribe this audio.",
        "images": [audio_base64],
        "stream": True,
        "think": False
    }

    ttft = None
    first_token_time = None
    start_time = time.perf_counter()
    tokens = []
    
    try:
        response = requests.post(url, json=payload, stream=True, timeout=180)
        response.raise_for_status()
        
        for line in response.iter_lines():
            if line:
                if ttft is None:
                    # Capture Time to First Token
                    ttft = (time.perf_counter() - start_time) * 1000.0
                    print(f"[+] Time to First Token (TTFT): {ttft:.2f} ms")
                
                data = json.loads(line.decode("utf-8"))
                token = data.get("response", "")
                tokens.append(token)
                
                # Print tokens live
                print(token, end="", flush=True)
                
                if data.get("done", False):
                    # Record Ollama internal metrics
                    prompt_eval_count = data.get("prompt_eval_count", 0)
                    prompt_eval_duration_ms = data.get("prompt_eval_duration", 0) / 1_000_000.0
                    eval_count = data.get("eval_count", 0)
                    eval_duration_ms = data.get("eval_duration", 0) / 1_000_000.0
                    total_duration_ms = (time.perf_counter() - start_time) * 1000.0
                    
                    print(f"\n[+] Total Duration: {total_duration_ms:.2f} ms")
                    return {
                        "duration_ms": duration_ms,
                        "ttft": ttft,
                        "total_duration": total_duration_ms,
                        "prompt_eval_count": prompt_eval_count,
                        "prompt_eval_duration": prompt_eval_duration_ms,
                        "eval_count": eval_count,
                        "eval_duration": eval_duration_ms,
                        "response": "".join(tokens).strip()
                    }
                    
    except Exception as e:
        print(f"\n[-] Error profiling chunk: {e}")
        return {
            "duration_ms": duration_ms,
            "ttft": -1.0,
            "total_duration": -1.0,
            "prompt_eval_count": 0,
            "prompt_eval_duration": 0.0,
            "eval_count": 0,
            "eval_duration": 0.0,
            "response": f"Failed: {e}"
        }

def main():
    download_sample_audio()
    resample_to_16k_mono()
    
    test_durations = [100, 250, 500, 1000, 2000]
    results = []
    
    for d in test_durations:
        res = profile_chunk(d)
        results.append(res)
        time.sleep(2) # Cooldown to let model unload/refresh cache

    print("\n\n" + "=" * 80)
    print("OLLAMA LATENCY PROFILING REPORT")
    print("=" * 80)
    print(f"{'Audio Duration':<15} | {'TTFT (ms)':<12} | {'Total (ms)':<12} | {'Prompt Tokens':<15} | {'Eval Count':<12}")
    print("-" * 80)
    for r in results:
        ttft_str = f"{r['ttft']:.1f}" if r['ttft'] > 0 else "N/A"
        tot_str = f"{r['total_duration']:.1f}" if r['total_duration'] > 0 else "N/A"
        print(f"{r['duration_ms']:<10} ms   | {ttft_str:<12} | {tot_str:<12} | {r['prompt_eval_count']:<15} | {r['eval_count']:<12}")
    print("=" * 80)
    
if __name__ == "__main__":
    main()
