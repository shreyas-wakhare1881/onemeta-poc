import os
from dotenv import load_dotenv

# Load environment variables from .env if it exists
load_dotenv()

ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')
# Comma-separated allowed origins for CORS. Includes both 3000 and 3001 so the
# Next dev server works regardless of which port it auto-selects.
BACKEND_CORS_ORIGINS = os.getenv(
    'BACKEND_CORS_ORIGINS',
    'http://localhost:3000,http://localhost:3001,http://127.0.0.1:3000,http://127.0.0.1:3001'
)

# LiveKit Configuration
LIVEKIT_API_KEY = os.getenv('LIVEKIT_API_KEY', '')
LIVEKIT_API_SECRET = os.getenv('LIVEKIT_API_SECRET', '')
LIVEKIT_URL = os.getenv('LIVEKIT_URL', 'ws://localhost:7880')
