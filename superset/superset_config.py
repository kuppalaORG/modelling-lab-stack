import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "supersecret")

# If you're accessing Superset via a public IP + no proxy:
ENABLE_PROXY_FIX = True

# Make cookies work reliably over plain HTTP (lab/dev only)
SESSION_COOKIE_SECURE = False
SESSION_COOKIE_SAMESITE = "Lax"

# Helps with CSRF in some setups
WTF_CSRF_TIME_LIMIT = None

# Optional: if you face weird logout due to caching
SESSION_REFRESH_EACH_REQUEST = True