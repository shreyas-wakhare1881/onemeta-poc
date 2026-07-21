import os
import sys
import time
import base64
import json
import requests

def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/validate_ollama_chat_audio.py <path_to_audio_file>")
        sys.exit(1)

    audio_path = sys.argv[1]
    if not os.path.exists(audio_path):
        print(f"Error: File not found at '{audio_path}'")
        sys.exit(1)

    print("--------------------------------------------------")
    print(f"Starting Ollama Chat Audio Validation (POST /api/chat)")
    print("--------------------------------------------------")

    # 1. Read the audio file and encode to base64
    try:
        file_size = os.path.getsize(audio_path)
        print(f"[*] Reading audio file: {audio_path} ({file_size / 1024:.2f} KB)...")
        
        with open(audio_path, "rb") as f:
            audio_bytes = f.read()
            
        audio_base64 = base64.b64encode(audio_bytes).decode("utf-8")
        print(f"[*] Encoded base64 string length: {len(audio_base64)} characters")
    except Exception as e:
        print(f"[-] Error reading/encoding audio file: {e}")
        sys.exit(1)

    # 2. Build the POST /api/chat payload
    url = "http://localhost:11434/api/chat"
    payload = {
        "model": "gemma4:12b",
        "messages": [
            {
                "role": "user",
                "content": "Transcribe this audio",
                "images": [audio_base64]
            }
        ],
        "stream": False,
        "think": False
    }

    print(f"[*] Request URL: {url}")
    print("[*] Sending chat request to Ollama (timeout=180s)...")
    
    # 3. Post request and measure time
    start_time = time.perf_counter()
    try:
        response = requests.post(url, json=payload, timeout=180)
        elapsed_time = (time.perf_counter() - start_time) * 1000.0
        
        response.raise_for_status()
        
        print("\n=== RESPONSE METRICS ===")
        print(f"HTTP Status Code : {response.status_code}")
        print(f"Response Time    : {elapsed_time:.2f} ms")
        
        print("\n=== RAW RESPONSE TEXT ===")
        print(response.text)
        
        try:
            response_json = response.json()
            print("\n=== PARSED JSON RESPONSE ===")
            print(json.dumps(response_json, indent=2))
            
            message_content = response_json.get("message", {}).get("content", "")
            print("\n=== CHAT MODEL RESPONSE ===")
            print(message_content if message_content else "(Empty message content)")
            
        except json.JSONDecodeError:
            print("\n[-] Error: Response is not valid JSON.")
            
    except requests.exceptions.HTTPError as e:
        print(f"\n[-] HTTP Error: Ollama server returned an HTTP error.")
        print(f"    Status Code: {response.status_code if response is not None else 'Unknown'}")
        if response is not None:
            print(f"    Details: {response.text}")
    except requests.exceptions.ConnectionError as e:
        print(f"\n[-] Connection Error: Could not connect to Ollama at {url}.")
        print(f"    Details: {e}")
    except requests.exceptions.Timeout as e:
        print(f"\n[-] Timeout Error: The request to Ollama exceeded 180s.")
        print(f"    Details: {e}")
    except Exception as e:
        print(f"\n[-] Unexpected Error: {e}")
        
    print("\n--------------------------------------------------")
    print("Validation run complete.")
    print("--------------------------------------------------")

if __name__ == "__main__":
    main()
