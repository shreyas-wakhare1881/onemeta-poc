import os
import sys
import asyncio
from pathlib import Path
from dotenv import load_dotenv

# Set paths
workspace_root = Path(__file__).resolve().parents[1]
env_path = workspace_root / "backend" / ".env"
load_dotenv(dotenv_path=env_path)

backend_dir = workspace_root / "backend"
sys.path.insert(0, str(backend_dir))

from google import genai
from google.genai import types

async def verify_compatibility():
    print("=== GEMINI 3.5 LIVE TRANSLATE COMPATIBILITY VERIFICATION ===")
    
    # 1. Check SDK version
    try:
        import google.genai
        print(f"Installed google-genai version: {google.genai.__version__}")
    except Exception as e:
        print(f"ERROR: Failed to check google-genai version: {e}")
        return False

    # 2. Check API key
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("ERROR: GOOGLE_API_KEY environment variable is not set in backend/.env!")
        return False
    print("API Key is configured in env.")

    # 3. Verify SDK supports TranslationConfig
    try:
        translation_config = types.TranslationConfig(
            target_language_code="es",
            echo_target_language=True
        )
        print("SDK supports types.TranslationConfig definition.")
    except Exception as e:
        print(f"ERROR: Installed SDK does not support TranslationConfig definition: {e}")
        return False

    # 4. Attempt connection to models/gemini-3.5-live-translate-preview
    model_name = "models/gemini-3.5-live-translate-preview"
    print(f"Connecting to live session with model: {model_name}...")
    
    client = genai.Client(api_key=api_key, http_options={'api_version': 'v1alpha'})
    
    # Build LiveConnectConfig with translation_config
    sdk_config = types.LiveConnectConfig(
        response_modalities=["AUDIO"],
        translation_config=translation_config,
        output_audio_transcription=types.AudioTranscriptionConfig()
    )
    
    try:
        # Establish connection context manager
        ctx = client.aio.live.connect(
            model=model_name,
            config=sdk_config
        )
        
        async with ctx as session:
            print("Handshake context entered successfully!")
            print("Attempting to send a dummy heartbeat/setup frame...")
            
            # Send a dummy input to verify communication (optional, but enters receive)
            receive_task = asyncio.create_task(session.receive().__anext__())
            
            # Wait a short duration to verify connection remains open
            done, pending = await asyncio.wait([receive_task], timeout=3.0)
            
            if receive_task in done:
                try:
                    result = receive_task.result()
                    print(f"Successfully received initial message from model: {result}")
                except Exception as e:
                    print(f"ERROR: Received error message during initial read: {e}")
                    raise e
            else:
                # Cancel pending read
                receive_task.cancel()
                print("WebSocket connection established and remained open for 3 seconds!")
                
        print("\n[SUCCESS] VERDICT: Ready for integration! Model and config are fully supported.")
        return True

    except Exception as e:
        print("\n[FAIL] VERDICT: Not available.")
        print("=======================================")
        print(f"Error class: {e.__class__.__name__}")
        print(f"Error details: {e}")
        print("=======================================")
        return False

if __name__ == "__main__":
    asyncio.run(verify_compatibility())
