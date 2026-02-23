# airflow/dags/mysql_to_starrocks_incremental.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta

import pymysql
import requests
import json
import logging
import uuid

log = logging.getLogger("airflow.task")

MYSQL = dict(
    host="mysql",
    user="user",
    password="user123",
    database="order_management",
)

SR_DB = "order_management_starrocks"
SR_USER = "root"
SR_PASS = ""  # blank for starrocks/allin1 image
SR_FE = "http://starrocks:8030"

TABLES = [
    {"src": "Customers", "dst": "customers_landing", "watermark": "updated_at"},
    {"src": "Categories", "dst": "categories_landing", "watermark": "updated_at"},
    {"src": "Suppliers", "dst": "suppliers_landing", "watermark": "updated_at"},
    {"src": "Shippers", "dst": "shippers_landing", "watermark": "updated_at"},
    {"src": "Employees", "dst": "employees_landing", "watermark": "updated_at"},
    {"src": "Products", "dst": "products_landing", "watermark": "updated_at"},
    {"src": "Orders", "dst": "orders_landing", "watermark": "updated_at"},
    {"src": "OrderDetails", "dst": "orderdetails_landing", "watermark": "updated_at"},
]

default_args = {
    "owner": "lab",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


from urllib.parse import urlparse

def stream_load(db: str, table: str, json_lines: str) -> dict:
    url = f"{SR_FE}/api/{db}/{table}/_stream_load"
    label = f"airflow_{table}_{uuid.uuid4().hex[:12]}"

    headers = {
        "format": "json",
        "read_json_by_line": "true",
        "Content-Type": "application/json",
        "Expect": "100-continue",
        "label": label,
    }

    s = requests.Session()
    s.auth = (SR_USER, SR_PASS)

    # 1) First request WITHOUT following redirects
    r = s.put(url, data=json_lines, headers=headers, timeout=60, allow_redirects=False)

    # 2) If FE redirects to BE, rewrite the host if needed and retry once
    if r.status_code in (301, 302, 303, 307, 308) and "Location" in r.headers:
        loc = r.headers["Location"]
        parsed = urlparse(loc)

        # If FE sent 127.0.0.1, replace it with docker service hostname
        if parsed.hostname in ("127.0.0.1", "localhost"):
            fixed_loc = loc.replace(parsed.hostname, "starrocks")
            log.warning("Stream load redirected to %s; rewriting to %s", loc, fixed_loc)
            r = s.put(fixed_loc, data=json_lines, headers=headers, timeout=60, allow_redirects=False)
        else:
            # Try redirect as-is (might be reachable)
            r = s.put(loc, data=json_lines, headers=headers, timeout=60, allow_redirects=False)

    if r.status_code >= 300:
        raise RuntimeError(f"Stream load HTTP failed: {r.status_code} {r.text[:500]}")

    try:
        resp = r.json()
    except Exception:
        raise RuntimeError(f"Stream load returned non-JSON response: {r.text[:500]}")

    status = str(resp.get("Status", "")).upper()
    if status != "SUCCESS":
        raise RuntimeError(f"Stream load FAILED for {db}.{table}: {resp}")

    return resp


def sync_one_table(src: str, dst: str, watermark_col: str):
    var_key = f"wm_{src}"
    last_wm = Variable.get(var_key, default_var="1970-01-01 00:00:00")
    log.info("Sync start: %s -> %s | last_wm=%s", src, dst, last_wm)

    # Optional but safer: lookback to avoid missing same-timestamp rows
    last_wm_dt = datetime.fromisoformat(last_wm) - timedelta(minutes=2)

    conn = None
    cur = None
    try:
        conn = pymysql.connect(**MYSQL)
        cur = conn.cursor(pymysql.cursors.DictCursor)

        sql = f"""
            SELECT *
            FROM `{src}`
            WHERE `{watermark_col}` > %s
            ORDER BY `{watermark_col}` ASC
        """
        cur.execute(sql, (last_wm_dt,))
        rows = cur.fetchall() or []
        log.info("Fetched %s rows from MySQL table %s", len(rows), src)

        if not rows:
            return

        payload = "\n".join(json.dumps(r, default=str) for r in rows)

        resp = stream_load(SR_DB, dst, payload)

        log.info(
            "%s -> %s OK. mysql_rows=%s loaded=%s filtered=%s label=%s txn=%s",
            src,
            dst,
            len(rows),
            resp.get("NumberLoadedRows"),
            resp.get("NumberFilteredRows"),
            resp.get("Label"),
            resp.get("TxnId"),
        )

        max_wm = max(str(r[watermark_col]) for r in rows)
        Variable.set(var_key, max_wm)
        log.info("Updated watermark %s=%s", var_key, max_wm)

    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def run_all():
    for t in TABLES:
        sync_one_table(t["src"], t["dst"], t["watermark"])


with DAG(
    dag_id="mysql_to_starrocks_incremental_all",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/1 * * * *",
    catchup=False,
    tags=["lab"],
) as dag:
    PythonOperator(
        task_id="sync_all_tables",
        python_callable=run_all,
    )