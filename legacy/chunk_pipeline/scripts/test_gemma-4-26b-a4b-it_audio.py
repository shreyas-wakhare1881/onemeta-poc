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
print("Google Gemma Audio Translation Verification")
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
    print("================================================================================")
    sys.exit(1)

try:
    client = genai.Client(api_key=api_key)
except Exception as e:
    print(f"ERROR: Failed to initialize Google GenAI Client: {e}")
    print("================================================================================")
    sys.exit(1)

# 3. Audio File Validation
wav_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "samples", "english_sample.wav")

if not os.path.exists(wav_path):
    print(f"ERROR: Audio file not found at {wav_path}")
    print("Please make sure a valid recorded WAV file is present at samples/english_sample.wav")
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
    print("================================================================================")
    sys.exit(1)

print("Model")
print("----------------------------------------------------------------------")
print("models/gemma-4-26b-a4b-it\n")

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

# 5. Call Gemma 4 Model to Translate the Audio
model_name = "models/gemma-4-26b-a4b-it"
prompt = """Translate the spoken English audio into Spanish.

Return ONLY the translated Spanish text.

Do not explain.
Do not summarize.
Do not return the English transcription."""

try:
    response = client.models.generate_content(
        model=model_name,
        contents=[
            uploaded_file,
            prompt
        ]
    )
    end_time = time.perf_counter()
    request_finished_dt = datetime.now()
    
    latency_ms = int((end_time - start_time) * 1000)
    translation = response.text.strip() if response.text else ""
    
    print(f"Finished            : {request_finished_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
    print(f"Total Latency       : {latency_ms} ms\n")
    
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
    print_success_line("Audio uploaded successfully")
    print_success_line("Model reachable")
    print_success_line("Translation generated")
    print()
    print("================================================================================")
    print("PHASE 2 PASSED")
    print("================================================================================")
    sys.exit(0)

except errors.APIError as api_err:
    print(f"\nERROR: Google GenAI API Exception: {api_err.message}")
    print("================================================================================")
    print("PHASE 2 FAILED")
    print("================================================================================")
    sys.exit(1)
except Exception as e:
    print(f"\nERROR: Unexpected error occurred: {e}")
    import traceback
    traceback.print_exc()
    print("================================================================================")
    print("PHASE 2 FAILED")
    print("================================================================================")
    sys.exit(1)
