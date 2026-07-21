import os
import sys
import time
import base64
import json
import requests

def classify_response(text_response, status_code):
    """
    Classifies whether Ollama supported/ignored/attempted to process the audio payload.
    """
    if status_code == 400:
        return "Unsupported Payload (HTTP 400)", "NO"
        
    text_lower = text_response.lower()
    if "upload" in text_lower or "attach" in text_lower or "no audio" in text_lower or "provide an audio" in text_lower or "please send" in text_lower:
        return "Audio Ignored (Model requested file upload)", "NO"
    elif "invalid" in text_lower:
        return "Audio Field Recognized (Triggered format warning)", "YES (Error)"
    elif "decode" in text_lower or "transcribe" in text_lower or "transcription" in text_lower:
        return "Audio Decoding Attempted", "YES"
    else:
        return "Possible Support (Unknown/General Response)", "YES (Unverified)"

def run_experiment(name, payload_factory, url, audio_base64):
    print("\n" + "=" * 50)
    print(f"Testing Payload Option {name}")
    print("=" * 50)
    
    payload = payload_factory(audio_base64)
    print(f"[*] Payload Keys: {list(payload.keys())}")
    print("[*] Sending request (timeout=180s)...")
    
    start_time = time.perf_counter()
    try:
        response = requests.post(url, json=payload, timeout=180)
        elapsed_time = (time.perf_counter() - start_time) * 1000.0
        
        response.raise_for_status()
        
        print(f"[+] HTTP Status Code : {response.status_code}")
        print(f"[+] Response Time    : {elapsed_time:.2f} ms")
        
        try:
            response_json = response.json()
            model_response = response_json.get("response", "")
            print("\n--- Model Response ---")
            print(model_response if model_response else "(Empty)")
            
            classification, supported = classify_response(model_response, response.status_code)
            return classification, supported
            
        except json.JSONDecodeError:
            print("[-] Error: Response is not valid JSON.")
            return "Invalid JSON Response", "NO"
            
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if e.response is not None else 500
        print(f"[-] HTTP Error (Status {status_code}): {e}")
        if e.response is not None:
            print(f"    Details: {e.response.text}")
        classification, supported = classify_response(e.response.text if e.response else "", status_code)
        return classification, supported
    except requests.exceptions.ConnectionError as e:
        print(f"[-] Connection Error: Could not connect to Ollama server.")
        return "Connection Refused", "NO"
    except requests.exceptions.Timeout as e:
        print(f"[-] Timeout Error: Request exceeded 180s.")
        return "Request Timeout", "NO"
    except Exception as e:
        print(f"[-] Unexpected Error: {e}")
        return f"Error: {type(e).__name__}", "NO"

def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/validate_ollama_audio.py <path_to_audio_file>")
        sys.exit(1)

    audio_path = sys.argv[1]
    if not os.path.exists(audio_path):
        print(f"Error: File not found at '{audio_path}'")
        sys.exit(1)

    try:
        file_size = os.path.getsize(audio_path)
        with open(audio_path, "rb") as f:
            audio_bytes = f.read()
        audio_base64 = base64.b64encode(audio_bytes).decode("utf-8")
        print(f"[*] Loaded '{audio_path}' ({file_size / 1024:.2f} KB)")
        print(f"[*] Base64 length: {len(audio_base64)} characters")
    except Exception as e:
        print(f"Error reading/encoding file: {e}")
        sys.exit(1)

    url = "http://localhost:11434/api/generate"
    results = {}

    # Define Experiment A
    def make_payload_a(b64):
        return {
            "model": "gemma4:12b",
            "prompt": "Transcribe or describe this audio.",
            "audio": b64,
            "stream": False,
            "think": False
        }

    # Define Experiment B
    def make_payload_b(b64):
        return {
            "model": "gemma4:12b",
            "prompt": "Transcribe or describe this audio.",
            "images": [b64],
            "stream": False,
            "think": False
        }

    # Define Experiment C
    def make_payload_c(b64):
        return {
            "model": "gemma4:12b",
            "prompt": "Transcribe or describe this audio.",
            "files": [b64],
            "stream": False,
            "think": False
        }

    # Run Experiments
    results["audio"] = run_experiment("A (audio key)", make_payload_a, url, audio_base64)
    results["images"] = run_experiment("B (images list key)", make_payload_b, url, audio_base64)
    results["files"] = run_experiment("C (files list key)", make_payload_c, url, audio_base64)

    # Print Summary Table
    print("\n" + "=" * 50)
    print("FINAL SUMMARY REPORT")
    print("=" * 50)
    print(f"{'Payload':<15} {'Supported?':<15} {'Classification':<20}")
    print("-" * 50)
    for payload_type, (classification, supported) in results.items():
        print(f"{payload_type:<15} {supported:<15} {classification:<20}")
    print("=" * 50)

if __name__ == "__main__":
    main()
