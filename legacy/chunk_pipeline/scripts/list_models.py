import os
import urllib.request
import json
from pathlib import Path
from dotenv import load_dotenv

workspace_root = Path(__file__).resolve().parents[1]
env_path = workspace_root / "backend" / ".env"
load_dotenv(dotenv_path=env_path)

def main():
    key = os.getenv("GOOGLE_API_KEY")
    if not key:
        print("GOOGLE_API_KEY not found!")
        return
        
    url = f"https://generativelanguage.googleapis.com/v1beta/models?key={key}"
    try:
        response = urllib.request.urlopen(url)
        data = json.loads(response.read())
        for model in data.get("models", []):
            name = model.get("name")
            methods = model.get("supportedGenerationMethods", [])
            print(f"{name}: {methods}")
    except Exception as e:
        print(f"Error listing v1beta models: {e}")
        
    url_alpha = f"https://generativelanguage.googleapis.com/v1alpha/models?key={key}"
    try:
        response = urllib.request.urlopen(url_alpha)
        data = json.loads(response.read())
        print("\n--- v1alpha models ---")
        for model in data.get("models", []):
            name = model.get("name")
            methods = model.get("supportedGenerationMethods", [])
            print(f"{name}: {methods}")
    except Exception as e:
        print(f"Error listing v1alpha models: {e}")

if __name__ == "__main__":
    main()
