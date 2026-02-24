FROM apache/superset:latest

USER root

# Ensure pip exists inside the venv (some images don't ship venv pip)
RUN /app/.venv/bin/python -m ensurepip --upgrade || true

# Upgrade tooling in the venv + install pymysql in the venv
RUN /app/.venv/bin/python -m pip install --no-cache-dir --upgrade pip setuptools wheel \
 && /app/.venv/bin/python -m pip install --no-cache-dir pymysql

USER superset