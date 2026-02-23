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

        ensure_sr_db_and_table(conn, src, SR_DB, dst)
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

import pymysql as sr_pymysql

def ensure_sr_db_and_table(mysql_conn, src_table: str, sr_db: str, sr_table: str):
    cur = mysql_conn.cursor(pymysql.cursors.DictCursor)
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
        ORDER BY ordinal_position
    """, (MYSQL["database"], src_table))
    cols = cur.fetchall() or []
    cols = [{k.lower(): v for k, v in row.items()} for row in cols]
    if not cols:
        raise RuntimeError(f"MySQL schema not found for {MYSQL['database']}.{src_table}")

    def map_type(t: str) -> str:
        t = (t or "").lower()
        if t in ("tinyint", "smallint", "mediumint", "int", "integer", "bigint"):
            return "BIGINT"
        if t in ("float", "double", "decimal", "numeric"):
            return "DOUBLE"
        if t in ("datetime", "timestamp"):
            return "DATETIME"
        if t == "date":
            return "DATE"
        # everything else -> varchar
        return "VARCHAR(65533)"

    col_names = [c["column_name"] for c in cols]
    col_defs = ",\n  ".join([f"`{c['column_name']}` {map_type(c['data_type'])}" for c in cols])

    # Choose a safe DUPLICATE KEY column
    if "id" in col_names:
        key_col = "id"
    else:
        # find first column that ends with _id
        id_like = next((c for c in col_names if c.lower().endswith("_id")), None)
        key_col = id_like or col_names[0]

    create_db_sql = f"CREATE DATABASE IF NOT EXISTS `{sr_db}`;"
    create_tbl_sql = f"""
        CREATE TABLE IF NOT EXISTS `{sr_db}`.`{sr_table}` (
        {col_defs}
        )
        DUPLICATE KEY(`{key_col}`)
        DISTRIBUTED BY HASH(`{key_col}`) BUCKETS 4
        PROPERTIES ("replication_num"="1");
        """.strip()

    sr = sr_pymysql.connect(
        host="starrocks",
        port=9030,
        user=SR_USER,
        password=SR_PASS,
        autocommit=True,
    )
    try:
        with sr.cursor() as c:
            c.execute(create_db_sql)
            c.execute(create_tbl_sql)
    finally:
        sr.close()
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