import os
import sys
import asyncio
import json
from pathlib import Path
from dotenv import load_dotenv

workspace_root = Path(__file__).resolve().parents[1]
env_path = workspace_root / "backend" / ".env"
load_dotenv(dotenv_path=env_path)

# Monkeypatch aiohttp to inspect sent messages
import aiohttp
original_send_str = aiohttp.ClientWebSocketResponse.send_str
async def patched_send_str(self, data, *args, **kwargs):
    print(f"\n[SDK SENT RAW JSON]: {data}")
    return await original_send_str(self, data, *args, **kwargs)
aiohttp.ClientWebSocketResponse.send_str = patched_send_str

from google import genai
from google.genai import types

async def main():
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("GOOGLE_API_KEY not found!")
        return

    print("Initializing GenAI client...")
    client = genai.Client(api_key=api_key, http_options={'api_version': 'v1alpha'})
    
    config = types.LiveConnectConfig(
        response_modalities=["AUDIO"],
        system_instruction=types.Content(
            parts=[types.Part(text="Translate English to Spanish. Speak only in Spanish.")]
        ),
        output_audio_transcription=types.AudioTranscriptionConfig()
    )
    
    model = "models/gemini-2.5-flash-native-audio-latest"
    print(f"Connecting to live session with model: {model}")
    
    try:
        async with client.aio.live.connect(model=model, config=config) as session:
            print("Connected successfully! Sending greeting prompt...")
            
            # Send initial text prompt
            await session.send(
                input="Hello, how are you doing today?",
                end_of_turn=True
            )
            
            print("Waiting for response (listening for 8 seconds)...")
            async def receive_loop():
                try:
                    async for response in session.receive():
                        if response.server_content:
                            content = response.server_content
                            if content.model_turn:
                                for part in content.model_turn.parts:
                                    if part.text:
                                        print(f"-> Model Turn Text: {part.text}")
                                    if part.inline_data:
                                        print(f"-> Model Turn Audio: {len(part.inline_data.data)} bytes")
                            
                            if content.output_transcription:
                                print(f"-> Output Transcription: {content.output_transcription.text}")
                                
                            if content.turn_complete:
                                print("-> Turn Complete.")
                except asyncio.CancelledError:
                    pass
                except Exception as ex:
                    print(f"Error in receive loop: {ex}")
                    
            try:
                await asyncio.wait_for(receive_loop(), timeout=8.0)
            except asyncio.TimeoutError:
                print("\nReceiver timed out (no more responses).")
                
    except Exception as e:
        print(f"SDK Connection Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
