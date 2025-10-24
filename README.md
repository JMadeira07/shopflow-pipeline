# ShopFlow Pipeline — Iteration 1 (Foundation)

This repository contains the **Iteration 1** deliverables for the ShopFlow e‑commerce analytics pipeline. It includes synthetic data generation, basic data validation, and core SQL analytics queries.

## What’s here
- `src/data_generator.py` — generates CSVs for **customers (1000)**, **products (500)**, **transactions (5000)** into `data/raw/`.
- `src/data_validator.py` — runs basic quality checks (nulls, email format, positive prices, date formats) and writes to `logs/validation.log`.
- `sql/analytics/basic_analytics.sql` — SQL answering:
  1. Top 10 customers by total spending
  2. Best-selling products by category
  3. Monthly revenue trends
  4. Average order value (AOV) by country

## Quickstart
```bash
# 1) (optional) create & activate a virtualenv
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 2) Generate data
python src/data_generator.py

# 3) Validate data
python src/data_validator.py

# 4) Inspect outputs
ls -lah data/raw
cat logs/validation.log
```

## Notes
- The generator/validator only use Python’s standard library for portability.
- SQL uses PostgreSQL‑style functions in comments; a portable alternative is provided where needed.
- For later iterations, see the exercise brief.
