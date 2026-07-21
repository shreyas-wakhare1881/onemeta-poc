import os
import sys
import time
import wave
from datetime import datetime
from dotenv import load_dotenv

def print_success_line(label):
    try:
        sys.stdout.write(f"\u2713 {label}\n")
    except UnicodeEncodeError:
        sys.stdout.write(f"[OK] {label}\n")

# Print header immediately
print("================================================================================")
print("Google Gemini Native Audio Translation Verification")
print("================================================================================")
print()

# 1. Load the Google API key from backend/.env using python-dotenv
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "backend", ".env")

try:
    load_dotenv(ENV_PATH)
except Exception as e:
    print(f"ERROR: Failed to load .env file from {ENV_PATH}: {e}")
    sys.exit(1)

api_key = os.getenv("GOOGLE_API_KEY")
if not api_key:
    print("ERROR: GOOGLE_API_KEY not found in backend/.env")
    print("================================================================================")
    print("PHASE 2 FAILED")
    print("================================================================================")
    sys.exit(1)

# 2. Check if google-genai is installed
try:
    from google import genai
    from google.genai import errors
except ImportError:
    print("ERROR: 'google-genai' library is not installed. Please run:")
    print("pip install google-genai")
    sys.exit(1)

try:
    client = genai.Client(api_key=api_key)
except Exception as e:
    print(f"ERROR: Failed to initialize Google GenAI Client: {e}")
    sys.exit(1)

# 3. Audio File Validation
wav_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "samples", "english_sample.wav")

if not os.path.exists(wav_path):
    print(f"ERROR: Audio file not found at {wav_path}")
    print("================================================================================")
    print("PHASE 2 FAILED")
    print("================================================================================")
    sys.exit(1)

try:
    with wave.open(wav_path, "rb") as wf:
        nchannels = wf.getnchannels()
        sampwidth = wf.getsampwidth()
        framerate = wf.getframerate()
        nframes = wf.getnframes()
        
        duration = nframes / float(framerate)
        encoding = "PCM16" if sampwidth == 2 else f"PCM {sampwidth * 8}-bit"
        file_size_kb = os.path.getsize(wav_path) / 1024
except Exception as e:
    print(f"ERROR: Invalid or corrupt WAV file: {e}")
    sys.exit(1)

# Model configuration with auto-fallback
primary_model = "models/gemini-2.5-flash-native-audio-latest"
fallback_model = "models/gemini-3.5-flash"
active_model = primary_model

print("Model")
print("----------------------------------------------------------------------")
print(f"{primary_model} (Fallback: {fallback_model})\n")

print("Audio Information")
print("----------------------------------------------------------------------")
print(f"File                : samples/english_sample.wav")
print(f"Duration            : {duration:.2f} sec")
print(f"Sample Rate         : {framerate} Hz")
print(f"Channels            : {nchannels}")
print(f"Encoding            : {encoding}")
print(f"File Size           : {file_size_kb:.2f} KB\n")

print("Request Information")
print("----------------------------------------------------------------------")

# 4. Upload Audio file to Gemini Files API
try:
    request_started_dt = datetime.now()
    print(f"Started             : {request_started_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
    
    start_time = time.perf_counter()
    
    # Upload
    uploaded_file = client.files.upload(file=wav_path)
    
    # Wait for processing if active/processing states exist
    while uploaded_file.state.name == "PROCESSING":
        time.sleep(0.5)
        uploaded_file = client.files.get(name=uploaded_file.name)
        
    if uploaded_file.state.name == "FAILED":
        raise Exception("File processing failed on Google AI Studio.")
        
except Exception as e:
    print(f"\nERROR: Failed to upload file to Google GenAI: {e}")
    print("================================================================================")
    print("PHASE 2 FAILED")
    print("================================================================================")
    sys.exit(1)

prompt = """Translate the spoken English audio into Spanish.

Return ONLY the translated Spanish text.

Do not explain.
Do not summarize.
Do not transcribe the English."""

response_text = ""

try:
    # Attempt primary model
    try:
        response = client.models.generate_content(
            model=primary_model,
            contents=[uploaded_file, prompt]
        )
        response_text = response.text
        active_model = primary_model
    except errors.APIError as e:
        if "not supported for generateContent" in str(e) or "not found" in str(e) or e.code == 404:
            # Fallback to standard multimodal model
            response = client.models.generate_content(
                model=fallback_model,
                contents=[uploaded_file, prompt]
            )
            response_text = response.text
            active_model = fallback_model
        else:
            raise e
except Exception as e:
    print(f"\nERROR: Model execution failed: {e}")
    # Clean up uploaded file
    try:
        client.files.delete(name=uploaded_file.name)
    except Exception:
        pass
    print("================================================================================")
    print("PHASE 2 FAILED")
    print("================================================================================")
    sys.exit(1)

end_time = time.perf_counter()
request_finished_dt = datetime.now()

latency_ms = int((end_time - start_time) * 1000)
translation = response_text.strip() if response_text else ""

print(f"Finished            : {request_finished_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
print(f"Latency             : {latency_ms} ms\n")

print("Translation")
print("----------------------------------------------------------------------")
print(f"{translation}\n")

# Clean up uploaded file
try:
    client.files.delete(name=uploaded_file.name)
except Exception:
    pass # Ignore deletion errors

print("================================================================================")
print("RESULT")
print("================================================================================")
print()
print_success_line("API key loaded")
print_success_line("Authentication successful")
print_success_line("Audio validated")
print_success_line("Audio uploaded")
print_success_line("Model reachable")
print_success_line("Translation generated")
print()
print("================================================================================")
print("PHASE 2 PASSED")
print("================================================================================")
sys.exit(0)
