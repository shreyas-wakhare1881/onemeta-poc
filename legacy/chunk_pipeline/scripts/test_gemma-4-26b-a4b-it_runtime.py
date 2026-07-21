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
print("Google Gemma Runtime Connectivity Test")
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
    print("ERROR:")
    print("GOOGLE_API_KEY not found in backend/.env")
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
    print("==================================================")
    print("FAIL")
    print("==================================================")
    sys.exit(1)

try:
    client = genai.Client(api_key=api_key)
except Exception as e:
    print(f"ERROR: Failed to initialize Google GenAI Client: {e}")
    print("==================================================")
    print("FAIL")
    print("==================================================")
    sys.exit(1)

# Model configuration
model_name = "models/gemma-4-26b-a4b-it"
prompt = """Translate the following English sentence into Spanish.

Sentence:
Hello, how are you?

Return ONLY the translated sentence."""

print(f"Model:\n{model_name}\n")

# 3. Send prompt and measure latency
request_started_dt = datetime.now()
print(f"Request Started:\n{request_started_dt.strftime('%Y-%m-%d %H:%M:%S')}\n")

start_time = time.perf_counter()

try:
    response = client.models.generate_content(
        model=model_name,
        contents=prompt
    )
    end_time = time.perf_counter()
    request_finished_dt = datetime.now()
    
    latency_ms = int((end_time - start_time) * 1000)
    translation = response.text.strip() if response.text else ""

    print(f"Request Finished:\n{request_finished_dt.strftime('%Y-%m-%d %H:%M:%S')}\n")
    print(f"Latency:\n{latency_ms} ms\n")
    print(f"Translation:\n{translation}\n")
    
    print("==================================================")
    print("SUCCESS")
    print()
    print_success_line("API key validated")
    print_success_line("Authentication successful")
    print_success_line("Gemma runtime reachable")
    print_success_line("Translation generated")
    print("==================================================")
    sys.exit(0)

except errors.APIError as api_err:
    print("ERROR: Google GenAI API Exception occurred:")
    print(f"Status Code: {api_err.code}")
    print(f"Message: {api_err.message}")
    print("==================================================")
    print("FAIL")
    print("==================================================")
    sys.exit(1)
except Exception as e:
    print(f"ERROR: Network or unexpected error occurred during API request: {e}")
    print("==================================================")
    print("FAIL")
    print("==================================================")
    sys.exit(1)
