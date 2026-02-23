import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "supersecret")

# You're on plain HTTP over public IP -> don't mark cookies Secure
SESSION_COOKIE_SECURE = False

# Safe default for HTTP; avoids cookies being dropped
SESSION_COOKIE_SAMESITE = "Lax"

# Helps if you're behind any proxy/NAT (common on EC2/public IP)
ENABLE_PROXY_FIX = True

# Make Superset generate links as http (prevents scheme confusion)
PREFERRED_URL_SCHEME = "http"
WEBSERVER_BASEURL = os.environ.get("WEBSERVER_BASEURL", "http://3.239.82.216:8088")

# Reduce “token expired” flakiness in dev
WTF_CSRF_TIME_LIMIT = None