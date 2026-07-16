import os

ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')
# Allow a single origin or comma-separated list; default to localhost:3000
BACKEND_CORS_ORIGINS = os.getenv('BACKEND_CORS_ORIGINS', 'http://localhost:3000')

# Other config values can be added here later
