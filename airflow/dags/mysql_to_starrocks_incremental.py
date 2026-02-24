from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlparse

import pymysql
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

log = logging.getLogger("airflow.task")

MYSQL = {
    "host": "mysql",
    "user": "user",
    "password": "user123",
    "database": "order_management",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
}

SR_DB = "order_management_starrocks"
SR_USER = "root"
SR_PASS = ""
SR_FE = "http://starrocks:8030"
SR_QUERY = {
    "host": "starrocks",
    "port": 9030,
    "user": SR_USER,
    "password": SR_PASS,
    "charset": "utf8mb4",
    "autocommit": True,
    "cursorclass": pymysql.cursors.DictCursor,
}

LOOKBACK_MINUTES = 2
DEFAULT_WATERMARK = "1970-01-01 00:00:00"
META_INGESTED_AT = "_ingested_at"
META_IS_DELETED = "_is_deleted"

TABLES = [
    {"src": "Customers", "dst": "customers_landing", "watermark": "updated_at", "pk": ["CustomerID"]},
    {"src": "Categories", "dst": "categories_landing", "watermark": "updated_at", "pk": ["CategoryID"]},
    {"src": "Suppliers", "dst": "suppliers_landing", "watermark": "updated_at", "pk": ["SupplierID"]},
    {"src": "Shippers", "dst": "shippers_landing", "watermark": "updated_at", "pk": ["ShipperID"]},
    {"src": "Employees", "dst": "employees_landing", "watermark": "updated_at", "pk": ["EmployeeID"]},
    {"src": "Products", "dst": "products_landing", "watermark": "updated_at", "pk": ["ProductID"]},
    {"src": "Orders", "dst": "orders_landing", "watermark": "updated_at", "pk": ["OrderID"]},
    {"src": "OrderDetails", "dst": "orderdetails_landing", "watermark": "updated_at", "pk": ["OrderDetailID"]},
]

default_args = {
    "owner": "lab",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def q_ident(name: str) -> str:
    return f"`{name}`"


def lower_keys(row: dict[str, Any]) -> dict[str, Any]:
    return {str(k).lower(): v for k, v in row.items()}


def map_mysql_to_starrocks(mysql_type: str) -> str:
    t = (mysql_type or "").lower()
    if t in {"tinyint", "smallint", "mediumint", "int", "integer", "bigint"}:
        return "BIGINT"
    if t in {"decimal", "numeric"}:
        return "DECIMAL(18,6)"
    if t in {"float", "double", "real"}:
        return "DOUBLE"
    if t in {"datetime", "timestamp"}:
        return "DATETIME"
    if t == "date":
        return "DATE"
    return "VARCHAR(65533)"


def get_mysql_schema(mysql_conn: pymysql.connections.Connection, src_table: str) -> dict[str, str]:
    sql = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
    """
    with mysql_conn.cursor() as cur:
        cur.execute(sql, (MYSQL["database"], src_table))
        rows = cur.fetchall() or []
    normalized = [lower_keys(row) for row in rows]
    schema = {row["column_name"]: row["data_type"] for row in normalized}
    if not schema:
        raise RuntimeError(f"MySQL schema not found for {MYSQL['database']}.{src_table}")
    return schema


def ensure_starrocks_database(sr_conn: pymysql.connections.Connection, db_name: str) -> None:
    with sr_conn.cursor() as cur:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {q_ident(db_name)}")


def table_exists_in_starrocks(
    sr_conn: pymysql.connections.Connection, db_name: str, table_name: str
) -> bool:
    sql = """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name = %s
        LIMIT 1
    """
    with sr_conn.cursor() as cur:
        cur.execute(sql, (db_name, table_name))
        return bool(cur.fetchone())


def get_starrocks_schema(
    sr_conn: pymysql.connections.Connection, db_name: str, table_name: str
) -> dict[str, str]:
    with sr_conn.cursor() as cur:
        cur.execute(f"DESC {q_ident(db_name)}.{q_ident(table_name)}")
        rows = cur.fetchall() or []
    normalized = [lower_keys(row) for row in rows]
    schema = {row["field"]: row["type"] for row in normalized}
    return schema


def create_starrocks_table(
    sr_conn: pymysql.connections.Connection,
    db_name: str,
    dst_table: str,
    mysql_schema: dict[str, str],
    pk_cols: list[str],
) -> None:
    missing_pk = [pk for pk in pk_cols if pk not in mysql_schema]
    if missing_pk:
        raise RuntimeError(f"PK columns missing in MySQL schema for {dst_table}: {missing_pk}")

    column_defs = []
    for col_name, mysql_type in mysql_schema.items():
        sr_type = map_mysql_to_starrocks(mysql_type)
        not_null = " NOT NULL" if col_name in pk_cols else ""
        column_defs.append(f"{q_ident(col_name)} {sr_type}{not_null}")

    column_defs.append(f"{q_ident(META_INGESTED_AT)} DATETIME")
    column_defs.append(f"{q_ident(META_IS_DELETED)} BOOLEAN")

    pk_expr = ", ".join(q_ident(c) for c in pk_cols)
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {q_ident(db_name)}.{q_ident(dst_table)} (
            {", ".join(column_defs)}
        )
        PRIMARY KEY({pk_expr})
        DISTRIBUTED BY HASH({pk_expr}) BUCKETS 4
        PROPERTIES ("replication_num"="1")
    """
    with sr_conn.cursor() as cur:
        cur.execute(ddl)


def add_missing_starrocks_columns(
    sr_conn: pymysql.connections.Connection,
    db_name: str,
    dst_table: str,
    mysql_schema: dict[str, str],
) -> None:
    sr_schema = get_starrocks_schema(sr_conn, db_name, dst_table)
    sr_schema_lower = {name.lower(): name for name in sr_schema}

    for col_name, mysql_type in mysql_schema.items():
        if col_name.lower() in sr_schema_lower:
            continue
        add_sql = (
            f"ALTER TABLE {q_ident(db_name)}.{q_ident(dst_table)} "
            f"ADD COLUMN {q_ident(col_name)} {map_mysql_to_starrocks(mysql_type)}"
        )
        with sr_conn.cursor() as cur:
            cur.execute(add_sql)
        log.info("Schema drift: added column %s to %s.%s", col_name, db_name, dst_table)

    if META_INGESTED_AT.lower() not in sr_schema_lower:
        with sr_conn.cursor() as cur:
            cur.execute(
                f"ALTER TABLE {q_ident(db_name)}.{q_ident(dst_table)} "
                f"ADD COLUMN {q_ident(META_INGESTED_AT)} DATETIME"
            )
        log.info("Schema drift: added metadata column %s to %s.%s", META_INGESTED_AT, db_name, dst_table)

    if META_IS_DELETED.lower() not in sr_schema_lower:
        with sr_conn.cursor() as cur:
            cur.execute(
                f"ALTER TABLE {q_ident(db_name)}.{q_ident(dst_table)} "
                f"ADD COLUMN {q_ident(META_IS_DELETED)} BOOLEAN"
            )
        log.info("Schema drift: added metadata column %s to %s.%s", META_IS_DELETED, db_name, dst_table)


def parse_watermark(watermark: str) -> datetime:
    # Accept ISO8601 with optional trailing Z, plus plain YYYY-mm-dd HH:MM:SS.
    return datetime.fromisoformat(watermark.replace("Z", "+00:00")).replace(tzinfo=None)


def get_effective_watermark(src_table: str) -> datetime:
    var_key = f"wm_{src_table}"
    raw = Variable.get(var_key, default_var=None)
    if not raw:
        log.info("Watermark %s not found; bootstrap from %s", var_key, DEFAULT_WATERMARK)
        raw = DEFAULT_WATERMARK
    return parse_watermark(raw)


def set_watermark(src_table: str, value: datetime) -> None:
    var_key = f"wm_{src_table}"
    formatted = value.strftime("%Y-%m-%d %H:%M:%S")
    Variable.set(var_key, formatted)
    log.info("Updated watermark %s=%s", var_key, formatted)


def fetch_incremental_rows(
    mysql_conn: pymysql.connections.Connection,
    src_table: str,
    watermark_col: str,
    since_time: datetime,
) -> list[dict[str, Any]]:
    sql = f"""
        SELECT *
        FROM {q_ident(src_table)}
        WHERE {q_ident(watermark_col)} > %s
        ORDER BY {q_ident(watermark_col)} ASC
    """
    with mysql_conn.cursor() as cur:
        cur.execute(sql, (since_time,))
        return cur.fetchall() or []


def coerce_to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "t", "yes", "y"}
    return bool(value)


def to_stream_load_json_lines(rows: list[dict[str, Any]], has_source_is_deleted: bool) -> str:
    ingested_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    out_rows = []
    for row in rows:
        row = dict(row)
        if has_source_is_deleted:
            row[META_IS_DELETED] = coerce_to_bool(row.get("is_deleted"))
        else:
            row[META_IS_DELETED] = False
        row[META_INGESTED_AT] = ingested_at
        out_rows.append(row)
    return "\n".join(json.dumps(r, default=str, separators=(",", ":")) for r in out_rows)


def stream_load_json_lines(db_name: str, table_name: str, payload: str) -> dict[str, Any]:
    label = f"airflow_{table_name}_{uuid.uuid4().hex}"
    url = f"{SR_FE}/api/{db_name}/{table_name}/_stream_load"
    headers = {
        "format": "json",
        "read_json_by_line": "true",
        "Content-Type": "application/json",
        "Expect": "100-continue",
        "label": label,
    }

    session = requests.Session()
    session.auth = (SR_USER, SR_PASS)

    response = session.put(url, data=payload, headers=headers, timeout=120, allow_redirects=False)
    if response.status_code in {301, 302, 303, 307, 308} and "Location" in response.headers:
        redirect_url = response.headers["Location"]
        parsed = urlparse(redirect_url)
        if parsed.hostname in {"127.0.0.1", "localhost"}:
            redirect_url = redirect_url.replace(parsed.hostname, "starrocks")
        response = session.put(redirect_url, data=payload, headers=headers, timeout=120, allow_redirects=False)

    if response.status_code >= 300:
        raise RuntimeError(f"Stream Load HTTP failed for {db_name}.{table_name}: {response.status_code} {response.text[:500]}")

    try:
        body = response.json()
    except Exception as exc:
        raise RuntimeError(f"Stream Load returned non-JSON response for {db_name}.{table_name}: {response.text[:500]}") from exc

    status = str(body.get("Status", "")).upper()
    if status != "SUCCESS":
        raise RuntimeError(f"Stream Load failed for {db_name}.{table_name}: {body}")

    body["label"] = label
    return body


def ensure_target_ready(
    mysql_conn: pymysql.connections.Connection,
    sr_conn: pymysql.connections.Connection,
    src_table: str,
    dst_table: str,
    pk_cols: list[str],
) -> dict[str, str]:
    mysql_schema = get_mysql_schema(mysql_conn, src_table)
    ensure_starrocks_database(sr_conn, SR_DB)

    if not table_exists_in_starrocks(sr_conn, SR_DB, dst_table):
        log.info("Creating table %s.%s", SR_DB, dst_table)
        create_starrocks_table(sr_conn, SR_DB, dst_table, mysql_schema, pk_cols)

    add_missing_starrocks_columns(sr_conn, SR_DB, dst_table, mysql_schema)
    return mysql_schema


def sync_table(table_cfg: dict[str, Any]) -> None:
    src_table = table_cfg["src"]
    dst_table = table_cfg["dst"]
    watermark_col = table_cfg["watermark"]
    pk_cols = table_cfg["pk"]

    if not pk_cols:
        raise RuntimeError(f"Invalid config for {src_table}: pk must be non-empty list")

    last_wm = get_effective_watermark(src_table)
    query_wm = last_wm - timedelta(minutes=LOOKBACK_MINUTES)
    log.info(
        "Sync start: %s -> %s | watermark=%s | lookback_minutes=%s | query_since=%s",
        src_table,
        dst_table,
        last_wm,
        LOOKBACK_MINUTES,
        query_wm,
    )

    mysql_conn = pymysql.connect(**MYSQL)
    sr_conn = pymysql.connect(**SR_QUERY)
    try:
        mysql_schema = ensure_target_ready(mysql_conn, sr_conn, src_table, dst_table, pk_cols)

        if watermark_col not in mysql_schema:
            raise RuntimeError(f"Watermark column {watermark_col} not found in MySQL table {src_table}")

        rows = fetch_incremental_rows(mysql_conn, src_table, watermark_col, query_wm)
        log.info("Fetched %s rows from MySQL table %s", len(rows), src_table)
        if not rows:
            return

        has_source_is_deleted = any(col.lower() == "is_deleted" for col in mysql_schema)
        payload = to_stream_load_json_lines(rows, has_source_is_deleted=has_source_is_deleted)
        load_resp = stream_load_json_lines(SR_DB, dst_table, payload)

        max_wm_raw = max(r[watermark_col] for r in rows)
        max_wm = parse_watermark(str(max_wm_raw))
        set_watermark(src_table, max_wm)

        log.info(
            "Loaded %s -> %s | source_rows=%s | loaded_rows=%s | filtered_rows=%s | label=%s | txn_id=%s",
            src_table,
            dst_table,
            len(rows),
            load_resp.get("NumberLoadedRows"),
            load_resp.get("NumberFilteredRows"),
            load_resp.get("Label") or load_resp.get("label"),
            load_resp.get("TxnId"),
        )
    finally:
        mysql_conn.close()
        sr_conn.close()


def run_all_tables() -> None:
    for table_cfg in TABLES:
        sync_table(table_cfg)


with DAG(
    dag_id="mysql_to_starrocks_incremental_all",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=["lab"],
) as dag:
    PythonOperator(
        task_id="sync_all_tables",
        python_callable=run_all_tables,
    )
