import wave
import numpy as np
from pathlib import Path

def inspect_wav(filename):
    filepath = Path("../output") / filename
    if not filepath.exists():
        print(f"File {filename} does not exist.")
        return
        
    with wave.open(str(filepath), "rb") as w:
        params = w.getparams()
        nchannels, sampwidth, framerate, nframes = params[:4]
        duration = nframes / framerate
        print(f"\n--- INSPECTING {filename} ---")
        print(f"Channels: {nchannels}")
        print(f"Sample Width: {sampwidth} bytes ({sampwidth * 8} bits)")
        print(f"Sample Rate: {framerate} Hz")
        print(f"Total Frames: {nframes}")
        print(f"Duration: {duration:.2f} seconds")
        
        # Read frames to inspect amplitude / silence
        raw_data = w.readframes(nframes)
        if sampwidth == 2:
            data = np.frombuffer(raw_data, dtype=np.int16)
            # Find max amplitude
            max_amp = np.max(np.abs(data)) if len(data) > 0 else 0
            # Calculate RMS energy in chunks of 1 second
            print(f"Max Amplitude: {max_amp}")
            
            chunk_size = framerate
            num_chunks = len(data) // chunk_size
            print("RMS Energy per second:")
            for i in range(min(num_chunks, 30)): # print first 30 seconds
                chunk = data[i * chunk_size : (i + 1) * chunk_size]
                rms = np.sqrt(np.mean(chunk.astype(np.float32) ** 2))
                print(f"  Sec {i+1}: RMS={rms:.1f}")
        else:
            print("Unsupported sample width for details.")

if __name__ == "__main__":
    inspect_wav("onemeta-demo_input.wav")
    inspect_wav("onemeta-demo_output.wav")
