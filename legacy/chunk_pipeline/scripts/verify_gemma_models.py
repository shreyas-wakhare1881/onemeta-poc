import os
from dotenv import load_dotenv
from google import genai

# Load .env file
load_dotenv("backend/.env")

api_key = os.getenv("GOOGLE_API_KEY")

if not api_key:
    raise ValueError("GOOGLE_API_KEY not found in backend/.env")

client = genai.Client(api_key=api_key)

print("=" * 80)
print("Available Models")
print("=" * 80)

for model in client.models.list():
    print(model.name)

print("=" * 80)