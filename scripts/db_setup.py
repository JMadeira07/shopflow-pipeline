"""
Iteration 2 - Database Setup Script for ShopFlow

Run with:
    PG_DSN=postgresql://postgres:postgres@localhost:5432/shopflow python scripts/db_setup.py
"""

import os
import psycopg

PG_DSN = os.getenv("PG_DSN", "postgresql://postgres:postgres@localhost:5432/shopflow")

DDL = [
    "CREATE SCHEMA IF NOT EXISTS core;"
]

def main():
    with psycopg.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            for stmt in DDL:
                cur.execute(stmt)
            conn.commit()
    print("Fist setup done. Schema core created.")
    
    
if  __name__ == "__main__":
    main()