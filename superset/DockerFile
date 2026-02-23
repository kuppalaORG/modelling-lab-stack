FROM apache/superset:latest

USER root

# Bootstrap pip inside Superset's venv, then install pymysql INTO THAT SAME venv
RUN /app/.venv/bin/python -m ensurepip --upgrade \
 && /app/.venv/bin/python -m pip install --no-cache-dir --upgrade pip setuptools wheel \
 && /app/.venv/bin/python -m pip install --no-cache-dir pymysql

USER superset