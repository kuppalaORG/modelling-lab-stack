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


def stream_load(db: str, table: str, json_lines: str) -> dict:
    """
    StarRocks Stream Load (JSON)
    - Must include Expect: 100-continue for your build (based on your error).
    - Must verify response JSON 'Status' == 'SUCCESS' (HTTP 200 can still mean FAILED).
    """
    url = f"{SR_FE}/api/{db}/{table}/_stream_load"

    label = f"airflow_{table}_{uuid.uuid4().hex[:12]}"

    headers = {
        "format": "json",
        "strip_outer_array": "true",
        "Content-Type": "application/json",
        "Expect": "100-continue",  #  required (your error shows SR expects it)
        "label": label,
    }

    # NOTE: Keep data as-is. Requests will send exactly the payload body.
    r = requests.put(
        url,
        data=json_lines,
        headers=headers,
        auth=(SR_USER, SR_PASS),
        timeout=60,
    )

    if r.status_code >= 300:
        raise RuntimeError(f"Stream load HTTP failed: {r.status_code} {r.text}")

    # Stream load usually returns JSON
    try:
        resp = r.json()
    except Exception:
        # fallback if content-type is weird
        try:
            resp = json.loads(r.text)
        except Exception:
            raise RuntimeError(f"Stream load returned non-JSON response: {r.text}")

    status = str(resp.get("Status", "")).upper()
    if status != "SUCCESS":
        #  fail the task so Airflow shows failure + retry works properly
        raise RuntimeError(f"Stream load FAILED for {db}.{table}: {resp}")

    return resp


def sync_one_table(src: str, dst: str, watermark_col: str):
    var_key = f"wm_{src}"
    last_wm = Variable.get(var_key, default_var="1970-01-01 00:00:00")
    log.info("Sync start: %s -> %s | last_wm=%s", src, dst, last_wm)

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
        cur.execute(sql, (last_wm,))
        rows = cur.fetchall() or []
        log.info("Fetched %s rows from MySQL table %s", len(rows), src)

        if not rows:
            return

        # JSON Lines (one json object per line)
        payload = "\n".join(json.dumps(r, default=str) for r in rows)

        #  Load into StarRocks
        resp = stream_load(SR_DB, dst, payload)

        loaded = resp.get("NumberLoadedRows", None)
        filtered = resp.get("NumberFilteredRows", None)
        log.info(
            "%s -> %s stream load SUCCESS. mysql_rows=%s loaded=%s filtered=%s resp=%s",
            src,
            dst,
            len(rows),
            loaded,
            filtered,
            resp,
        )

        #  Only update watermark after SUCCESS
        max_wm = max(str(r[watermark_col]) for r in rows)
        Variable.set(var_key, max_wm)
        log.info("Updated watermark %s=%s", var_key, max_wm)

    finally:
        try:
            if cur:
                cur.close()
        except Exception:
            pass
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def run_all():
    # If any table fails, task fails (good) so you see it in Airflow.
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