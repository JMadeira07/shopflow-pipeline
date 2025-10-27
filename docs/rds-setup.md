# RDS Setup (PostgreSQL)

## 1) Create the instance (AWS Console)
- Engine: PostgreSQL 16 (Template: Sandbox)
- DB identifier: ctw04531-shopflow-rds
- Initial DB name: `postgres`
- Public access: Yes (for dev)
- Security group: inbound TCP 5432 from *My IP*
- Storage: gp3 20GB, Deletion protection: Off
- Copy the **Endpoint** when status = Available.

## 2) Connect (DBeaver)
- Host: <RDS-ENDPOINT> · Port: 5432 · DB: postgres
- User/Pass: your RDS credentials
- SSL: `require`

## 3) Apply schema + indexes
Run on the RDS connection:
```sql
CREATE TABLE IF NOT EXISTS public.customers (
  id INT PRIMARY KEY, name TEXT, email TEXT, registration_date DATE, country TEXT
);
CREATE TABLE IF NOT EXISTS public.products (
  id INT PRIMARY KEY, name TEXT, category TEXT, price NUMERIC(12,2), supplier TEXT
);
CREATE TABLE IF NOT EXISTS public.transactions (
  id INT PRIMARY KEY,
  customer_id INT REFERENCES public.customers(id),
  product_id  INT REFERENCES public.products(id),
  quantity INT, "timestamp" TIMESTAMPTZ, payment_method TEXT
);
CREATE INDEX IF NOT EXISTS idx_trx_customer ON public.transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_trx_product  ON public.transactions(product_id);
CREATE INDEX IF NOT EXISTS idx_trx_ts       ON public.transactions("timestamp");
```

## 4) Create views using public_views.sql

```bash
psql "postgresql://postgres:ctw04531@ctw04531-shopflow-rds.cpo4k6euqs4p.eu-central-1.rds.amazonaws.com:5432/postgres" -f sql/analytics/public_views.sql
```

## 5) Run load_to_db

```bash
export PG_DSN="postgresql://postgres:ctw04531@ctw04531-shopflow-rds.cpo4k6euqs4p.eu-central-
1.rds.amazonaws.com:5432/postgres?sslmode=require"

export DATA_DIR="data/raw"

python src/etl/load_to_db.py
```

## 6) Verification

```bash
SELECT COUNT(*) FROM public.customers;
SELECT COUNT(*) FROM public.products;
SELECT COUNT(*) FROM public.transactions;
SELECT status, load_date, started_at_utc, finished_at_utc
FROM public.load_audit ORDER BY started_at_utc DESC LIMIT 5;
SELECT * FROM public.v_top_customers LIMIT 5;
```