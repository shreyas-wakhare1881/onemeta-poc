import os
from dotenv import load_dotenv

# Load environment variables from .env if it exists
load_dotenv()

ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')
# Allow a single origin or comma-separated list; default to localhost:3000
BACKEND_CORS_ORIGINS = os.getenv('BACKEND_CORS_ORIGINS', 'http://localhost:3000')

# LiveKit Configuration
LIVEKIT_API_KEY = os.getenv('LIVEKIT_API_KEY', '')
LIVEKIT_API_SECRET = os.getenv('LIVEKIT_API_SECRET', '')
LIVEKIT_URL = os.getenv('LIVEKIT_URL', 'ws://localhost:7880')
