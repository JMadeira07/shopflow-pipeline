#!/usr/bin/env python3
"""
ShopFlow Iteration 1 - Data Generator
Generates synthetic customers, products, and transactions CSVs.
Outputs to data/raw/.
"""
import csv
import random
import string
from datetime import datetime, timedelta
from pathlib import Path
import unicodedata
import re

BASE = Path(__file__).resolve().parents[1]  # .../shopflow-pipeline
RAW_DIR = BASE / "data" / "raw"

COUNTRIES = ["PT", "ES", "FR", "DE", "IT", "UK", "US", "BR", "CA", "AU"]
CATEGORIES = ["Electronics", "Books", "Home & Kitchen", "Clothing", "Sports", "Beauty", "Toys", "Grocery"]
SUPPLIERS = ["Acme Corp", "Globex", "Umbrella", "Initech", "Stark Industries", "Wayne Enterprises"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]

def slug_ascii(s: str) -> str:
    # normalize accents -> ASCII, keep letters, numbers, dot
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.lower().replace(" ", ".")
    s = re.sub(r"[^a-z0-9.]+", "", s)      # remove anything not a-z 0-9 or dot
    s = re.sub(r"\.{2,}", ".", s).strip(".")  # collapse multiple dots, trim edges
    return s

def random_name():
    first = random.choice(["Ana","João","Maria","Luca","Emma","Noah","Olivia","Liam","Mia","Tiago","Sofia","Mateo","Laura","Eva","Gabriel","Lucas","Inês","Afonso"])
    last = random.choice(["Silva","Santos","Pereira","Costa","Oliveira","Gomes","Martins","Rodrigues","Lopes","Almeida","Ferreira","Carvalho","Sousa","Gonçalves"])
    return f"{first} {last}"

def random_email(name, idx):
    base = slug_ascii(name)
    domain = random.choice(["gmail.com","outlook.com","yahoo.com","example.com","proton.me"])
    return f"{base}.{idx}@{domain}"


def random_product_name(category):
    prefix = random.choice(["Ultra","Pro","Max","Eco","Smart","Lite","Nano","Hyper"])
    noun = {
        "Electronics": ["Headphones","Tablet","Phone","Camera","Speaker","Monitor"],
        "Books": ["Novel","Guide","Handbook","Cookbook","Anthology","Biography"],
        "Home & Kitchen": ["Blender","Toaster","Kettle","Lamp","Vacuum","Mixer"],
        "Clothing": ["T-Shirt","Jeans","Jacket","Sneakers","Dress","Hoodie"],
        "Sports": ["Ball","Racket","Mat","Gloves","Helmet","Shoes"],
        "Beauty": ["Serum","Cream","Lotion","Cleanser","Mask","Oil"],
        "Toys": ["Puzzle","Action Figure","Board Game","Doll","RC Car","Lego Set"],
        "Grocery": ["Coffee","Tea","Pasta","Olive Oil","Chocolate","Cereal"],
    }[category]
    return f"{prefix} {random.choice(noun)}"

def daterange(start_days_ago=365, end_days_ago=0):
    """Return a random datetime within the last year."""
    now = datetime.utcnow()
    start = now - timedelta(days=start_days_ago)
    end = now - timedelta(days=end_days_ago)
    delta = end - start
    rand_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=rand_seconds)

def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Customers
    customers_path = RAW_DIR / "customers.csv"
    with customers_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id","name","email","registration_date","country"])
        for i in range(1, 1000+1):
            name = random_name()
            email = random_email(name, i)
            reg_date = daterange(365*3, 200).strftime("%Y-%m-%d")
            country = random.choice(COUNTRIES)
            writer.writerow([i, name, email, reg_date, country])

    # Products
    products_path = RAW_DIR / "products.csv"
    with products_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id","name","category","price","supplier"])
        for i in range(1, 500+1):
            cat = random.choice(CATEGORIES)
            name = random_product_name(cat)
            price = round(random.uniform(3.0, 999.0), 2)
            supplier = random.choice(SUPPLIERS)
            writer.writerow([i, name, cat, price, supplier])

    # Transactions
    transactions_path = RAW_DIR / "transactions.csv"
    with transactions_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id","customer_id","product_id","quantity","timestamp","payment_method"])
        for i in range(1, 5000+1):
            cust_id = random.randint(1, 1000)
            prod_id = random.randint(1, 500)
            quantity = random.randint(1, 5)
            ts = daterange(365, 0).strftime("%Y-%m-%dT%H:%M:%SZ")
            pay = random.choice(PAYMENT_METHODS)
            writer.writerow([i, cust_id, prod_id, quantity, ts, pay])

    print(f"Generated:\n - {customers_path}\n - {products_path}\n - {transactions_path}")

if __name__ == "__main__":
    random.seed(42)
    main()
