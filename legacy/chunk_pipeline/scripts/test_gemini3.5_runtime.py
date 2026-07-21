import os
import sys
import time
from datetime import datetime
from dotenv import load_dotenv

def print_success_line(label):
    try:
        sys.stdout.write(f"\u2713 {label}\n")
    except UnicodeEncodeError:
        sys.stdout.write(f"[OK] {label}\n")

# Print header immediately
print("==================================================")
print("Google Gemini Native Audio Runtime Connectivity Test")
print("==================================================")
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
    print("==================================================")
    print("FAIL")
    print("==================================================")
    sys.exit(1)

print("API key loaded successfully")

# 2. Import genai and create client
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

# Model configuration with auto-fallback
primary_model = "models/gemini-2.5-flash-native-audio-latest"
fallback_model = "models/gemini-3.5-flash"
active_model = primary_model

prompt = """Translate the following English sentence into Spanish.

Sentence:
Hello, how are you?

Return ONLY the translated sentence."""

# 3. Send prompt and measure latency
request_started_dt = datetime.now()
print(f"Request Started:\n{request_started_dt.strftime('%Y-%m-%d %H:%M:%S')}\n")

start_time = time.perf_counter()
response_text = ""

try:
    # Attempt primary model
    print(f"Attempting connection to primary model: {primary_model}...")
    response = client.models.generate_content(
        model=primary_model,
        contents=prompt
    )
    response_text = response.text
except errors.APIError as e:
    # If primary model doesn't support generateContent, fallback to gemini-3.5-flash
    if "not supported for generateContent" in str(e) or "not found" in str(e) or e.code == 404:
        print(f"\n[NOTE] {primary_model} only supports bidiGenerateContent (WebSocket).")
        print(f"Falling back to standard multimodal model: {fallback_model}...")
        active_model = fallback_model
        try:
            response = client.models.generate_content(
                model=fallback_model,
                contents=prompt
            )
            response_text = response.text
        except Exception as fallback_err:
            print(f"ERROR: Fallback model {fallback_model} also failed: {fallback_err}")
            print("==================================================")
            print("FAIL")
            print("==================================================")
            sys.exit(1)
    else:
        print(f"ERROR: Google GenAI API Exception: {e}")
        print("==================================================")
        print("FAIL")
        print("==================================================")
        sys.exit(1)
except Exception as e:
    print(f"ERROR: Unexpected exception: {e}")
    print("==================================================")
    print("FAIL")
    print("==================================================")
    sys.exit(1)

end_time = time.perf_counter()
request_finished_dt = datetime.now()

latency_ms = int((end_time - start_time) * 1000)
translation = response_text.strip() if response_text else ""

print(f"Model:\n{active_model}\n")
print(f"Request Finished:\n{request_finished_dt.strftime('%Y-%m-%d %H:%M:%S')}\n")
print(f"Latency:\n{latency_ms} ms\n")
print(f"Translation:\n{translation}\n")

print("==================================================")
print("SUCCESS")
print()
print_success_line("API key validated")
print_success_line("Authentication successful")
print_success_line("Gemini Native Audio/Fallback runtime reachable")
print_success_line("Translation generated")
print("==================================================")
sys.exit(0)
