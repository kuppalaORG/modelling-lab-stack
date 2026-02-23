# airflow/dags/mysql_to_starrocks_incremental.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pymysql
import requests
import json
import logging

log = logging.getLogger("airflow.task")

MYSQL = dict(host="mysql", user="user", password="user123", database="order_management")

SR_DB = "order_management_starrocks"
SR_USER = "root"
SR_PASS = ""  
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

default_args = {"owner": "lab", "retries": 1, "retry_delay": timedelta(minutes=1)}

def stream_load(db: str, table: str, json_lines: str):
    url = f"{SR_FE}/api/{db}/{table}/_stream_load"   
    headers = {"format": "json", "strip_outer_array": "true"}
    r = requests.put(url, data=json_lines, headers=headers, auth=(SR_USER, SR_PASS), timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"Stream load failed: {r.status_code} {r.text}")
    return r.text

def sync_one_table(src: str, dst: str, watermark_col: str):
    var_key = f"wm_{src}"
    last_wm = Variable.get(var_key, default_var="1970-01-01 00:00:00")
    log.info("Sync start: %s -> %s | last_wm=%s", src, dst, last_wm)

    conn = pymysql.connect(**MYSQL)
    cur = conn.cursor(pymysql.cursors.DictCursor)

    sql = f"""
      SELECT *
      FROM `{src}`
      WHERE `{watermark_col}` > %s
      ORDER BY `{watermark_col}` ASC
    """
    cur.execute(sql, (last_wm,))
    rows = cur.fetchall()
    log.info("Fetched %s rows from MySQL table %s", len(rows), src)

    if not rows:
        cur.close()
        conn.close()
        return

    payload = "\n".join(json.dumps(r, default=str) for r in rows)

    resp = stream_load(SR_DB, dst, payload)  
    log.info("%s -> %s loaded %s rows. resp=%s", src, dst, len(rows), resp)

    max_wm = max(str(r[watermark_col]) for r in rows)
    Variable.set(var_key, max_wm)
    log.info("Updated watermark %s=%s", var_key, max_wm)

    cur.close()
    conn.close()

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
    PythonOperator(task_id="sync_all_tables", python_callable=run_all)