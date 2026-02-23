import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "supersecret")

ENABLE_PROXY_FIX = True
PREFERRED_URL_SCHEME = "http"
WEBSERVER_BASEURL = os.environ.get("WEBSERVER_BASEURL", "http://3.239.82.216:8088")

SESSION_COOKIE_SECURE = False
SESSION_COOKIE_SAMESITE = "Lax"

WTF_CSRF_TIME_LIMIT = None

# Dev-only (if still failing, turn this on temporarily)
# WTF_CSRF_ENABLED = False