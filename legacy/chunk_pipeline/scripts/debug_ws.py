import os
import sys
import asyncio
import aiohttp
import json
from pathlib import Path
from dotenv import load_dotenv

workspace_root = Path(__file__).resolve().parents[1]
env_path = workspace_root / "backend" / ".env"
load_dotenv(dotenv_path=env_path)

async def main():
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("GOOGLE_API_KEY not found!")
        return
    
    url = f"wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key={api_key}"
    print(f"Connecting to: {url}")
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.ws_connect(url) as ws:
                print("Handshake connected!")
                setup_msg = {
                    "setup": {
                        "model": "models/gemini-2.5-flash-native-audio-latest",
                        "generationConfig": {
                            "responseModalities": ["AUDIO"]
                        }
                    }
                }
                print(f"Sending setup: {setup_msg}")
                await ws.send_json(setup_msg)
                
                # Wait for setup response
                msg = await ws.receive()
                print(f"Initial setup response type: {msg.type}, data: {msg.data}")
                
                # Send text prompt in pure camelCase
                prompt_msg = {
                    "clientContent": {
                        "turns": [
                            {
                                "role": "user",
                                "parts": [
                                    {
                                        "text": "Hello, how are you today?"
                                    }
                                ]
                            }
                        ],
                        "turnComplete": True
                    }
                }
                print(f"Sending prompt: {prompt_msg}")
                await ws.send_json(prompt_msg)
                
                print("Reading responses...")
                # Read messages until we hit a timeout
                message_count = 0
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.receive(), timeout=6.0)
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            message_count += 1
                            obj = json.loads(msg.data)
                            
                            # Log details
                            server_content = obj.get("serverContent", {})
                            model_turn = server_content.get("modelTurn", {})
                            parts = model_turn.get("parts", [])
                            
                            for part in parts:
                                if "text" in part:
                                    print(f"[{message_count}] modelTurn Text: {part['text']}")
                                if "inlineData" in part:
                                    mime = part["inlineData"].get("mimeType", "")
                                    size = len(part["inlineData"].get("data", ""))
                                    print(f"[{message_count}] Audio chunk: mime={mime}, size={size}")
                                    
                            transcription = server_content.get("outputTranscription", {})
                            if "text" in transcription:
                                print(f"[{message_count}] outputTranscription: {transcription['text']}")
                                
                        elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                            print(f"Closed: code={ws.close_code}, extra={msg.extra}")
                            break
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print(f"Error: {ws.exception()}")
                            break
                    except asyncio.TimeoutError:
                        print("Timeout: No more messages from Gemini.")
                        break
        except Exception as e:
            print(f"Connection Exception: {e}")

if __name__ == "__main__":
    asyncio.run(main())
