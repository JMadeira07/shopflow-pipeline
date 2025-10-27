#!/usr/bin/env python3
"""
Idempotent CSV -> Postgres loader with audit logging & simple versioning.

Env:
  PG_DSN=postgresql://postgres:postgres@localhost:5432/shopflow
  DATA_DIR=./data/raw
  LOAD_DATE=YYYY-MM-DD   # optional; defaults to today (UTC)

Usage:
  python src/etl/load_to_db.py
"""

import os
import csv
import uuid
import datetime as dt
import psycopg
from psycopg.types.json import Json

PG_DSN    = os.getenv("PG_DSN", "postgresql://postgres:postgres@localhost:5432/shopflow")
DATA_DIR  = os.getenv("DATA_DIR", "data/raw")
LOAD_DATE = os.getenv("LOAD_DATE")
LOAD_DATE = dt.date.fromisoformat(LOAD_DATE) if LOAD_DATE else dt.datetime.utcnow().date()

# target files & schemas (table -> (filename, columns, pk))
FILES = {
    "public.customers":     ("customers.csv",    ["id","name","email","registration_date","country"], "id"),
    "public.products":      ("products.csv",     ["id","name","category","price","supplier"],         "id"),
    "public.transactions":  ("transactions.csv", ["id","customer_id","product_id","quantity","timestamp","payment_method"], "id"),
}

DDL = [
    # Audit table (one row per run; details stored as JSONB)
    """
    CREATE TABLE IF NOT EXISTS public.load_audit (
      batch_id        UUID PRIMARY KEY,
      load_date       DATE NOT NULL,
      started_at_utc  TIMESTAMPTZ NOT NULL,
      finished_at_utc TIMESTAMPTZ,
      status          TEXT NOT NULL,       -- running|succeeded|failed
      details         JSONB
    );
    """,
    # Add ingestion metadata for simple versioning
    "ALTER TABLE public.customers    ADD COLUMN IF NOT EXISTS _ingested_at TIMESTAMPTZ;",
    "ALTER TABLE public.products     ADD COLUMN IF NOT EXISTS _ingested_at TIMESTAMPTZ;",
    "ALTER TABLE public.transactions ADD COLUMN IF NOT EXISTS _ingested_at TIMESTAMPTZ;",
]

def upsert_sql(table: str, cols: list[str], pk: str) -> str:
    col_list = ", ".join(cols + ["_ingested_at"])
    placeholders = ", ".join(["%s"] * (len(cols) + 1))
    updates = ", ".join(
        [f"{c}=EXCLUDED.{c}" for c in cols if c != pk] + ["_ingested_at=EXCLUDED._ingested_at"]
    )
    return f"""
        INSERT INTO {table} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT ({pk}) DO UPDATE SET {updates};
    """

def load_table(cur: psycopg.Cursor, table: str, csv_path: str, cols: list[str], pk: str) -> dict:
    """
    Upsert rows from CSV in chunks. Returns how many rows were submitted.
    Note: with ON CONFLICT, exact insert vs update counts aren’t exposed; we report submitted rows.
    """
    submitted = 0
    sql = upsert_sql(table, cols, pk)

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            vals = [row[c] for c in cols] + [dt.datetime.utcnow()]
            batch.append(vals)
            if len(batch) >= 1000:
                cur.executemany(sql, batch)   # psycopg3 returns None here
                submitted += len(batch)
                batch.clear()
        if batch:
            cur.executemany(sql, batch)
            submitted += len(batch)

    return {"rows_submitted": submitted}

def main():
    batch_id = uuid.uuid4()
    started = dt.datetime.utcnow()

    # Ensure files exist before opening a transaction
    for table, (filename, _, _) in FILES.items():
        path = os.path.join(DATA_DIR, filename)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing file for {table}: {path}")

    with psycopg.connect(PG_DSN, autocommit=False) as conn:
        with conn.cursor() as cur:
            # Ensure audit table & metadata columns
            for stmt in DDL:
                cur.execute(stmt)

            # Start audit row
            cur.execute(
                """
                INSERT INTO public.load_audit (batch_id, load_date, started_at_utc, status, details)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (batch_id, LOAD_DATE, started, "running", Json({}))
            )

            details = {}
            try:
                for table, (filename, cols, pk) in FILES.items():
                    csv_path = os.path.join(DATA_DIR, filename)
                    stats = load_table(cur, table, csv_path, cols, pk)
                    details[table] = stats

                finished = dt.datetime.utcnow()
                cur.execute(
                    "UPDATE public.load_audit SET finished_at_utc=%s, status=%s, details=%s WHERE batch_id=%s",
                    (finished, "succeeded", Json(details), batch_id)
                )
                conn.commit()
                print(f"Load succeeded. batch_id={batch_id}")
            except Exception as e:
                finished = dt.datetime.utcnow()
                cur.execute(
                    "UPDATE public.load_audit SET finished_at_utc=%s, status=%s, details=%s WHERE batch_id=%s",
                    (finished, "failed", Json({"error": str(e)}), batch_id)
                )
                conn.commit()
                raise

if __name__ == "__main__":
    main()
