#!/usr/bin/env python3
"""
ShopFlow Iteration 1 - Data Validator
Performs basic data quality checks and logs to logs/validation.log.
"""
import csv
import re
from pathlib import Path
from datetime import datetime

BASE = Path(__file__).resolve().parents[1]
RAW_DIR = BASE / "data" / "raw"
LOG_DIR = BASE / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "validation.log"

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")

def log(msg):
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")

def check_nulls(row, rownum, required_cols, file_label):
    errors = []
    for c in required_cols:
        if row.get(c, "") in ("", None):
            errors.append(f"{file_label}: row {rownum} null in '{c}'")
    return errors

def validate_email(email, rownum):
    if not EMAIL_RE.match(email or ""):
        return [f"customers.csv: row {rownum} invalid email '{email}'"]
    return []

def validate_price(price_str, rownum):
    try:
        price = float(price_str)
        if price <= 0:
            return [f"products.csv: row {rownum} non-positive price '{price_str}'"]
    except Exception:
        return [f"products.csv: row {rownum} invalid price '{price_str}'"]
    return []

def validate_date(date_str, fmt, file_label, rownum):
    try:
        datetime.strptime(date_str, fmt)
        return []
    except Exception:
        return [f"{file_label}: row {rownum} bad date '{date_str}' expected {fmt}"]

def main():
    # Reset log
    LOG_FILE.write_text("", encoding="utf-8")

    # Customers
    cust_path = RAW_DIR / "customers.csv"
    if not cust_path.exists():
        log("ERROR: customers.csv not found. Run data_generator.py first.")
        return
    with cust_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):
            errs = []
            errs += check_nulls(row, i, ["id","name","email","registration_date","country"], "customers.csv")
            errs += validate_email(row.get("email",""), i)
            errs += validate_date(row.get("registration_date",""), "%Y-%m-%d", "customers.csv", i)
            for e in errs:
                log(f"ERROR: {e}")

    # Products
    prod_path = RAW_DIR / "products.csv"
    if not prod_path.exists():
        log("ERROR: products.csv not found. Run data_generator.py first.")
        return
    with prod_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):
            errs = []
            errs += check_nulls(row, i, ["id","name","category","price","supplier"], "products.csv")
            errs += validate_price(row.get("price",""), i)
            for e in errs:
                log(f"ERROR: {e}")

    # Transactions
    trx_path = RAW_DIR / "transactions.csv"
    if not trx_path.exists():
        log("ERROR: transactions.csv not found. Run data_generator.py first.")
        return
    with trx_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):
            errs = []
            errs += check_nulls(row, i, ["id","customer_id","product_id","quantity","timestamp","payment_method"], "transactions.csv")
            # timestamp in ISO-8601 Z
            errs += validate_date(row.get("timestamp",""), "%Y-%m-%dT%H:%M:%SZ", "transactions.csv", i)
            # quantity positive int
            try:
                q = int(row.get("quantity","0"))
                if q <= 0:
                    errs.append(f"transactions.csv: row {i} non-positive quantity '{row.get('quantity')}'")
            except Exception:
                errs.append(f"transactions.csv: row {i} invalid quantity '{row.get('quantity')}'")
            for e in errs:
                log(f"ERROR: {e}")

    log("INFO: Validation completed. Check errors above (if any).")

if __name__ == "__main__":
    main()
