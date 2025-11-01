
import os
from pathlib import Path
import pandas as pd
import numpy as np
import duckdb
from datetime import datetime, timedelta
import random

BASE = Path(__file__).resolve().parents[1]
DATA_DIR = BASE / "data" / "raw"
WAREHOUSE = BASE / "data" / "warehouse.duckdb"

DATA_DIR.mkdir(parents=True, exist_ok=True)

np.random.seed(42)
random.seed(42)

# Customers
cities = [
    ("New York","NY"), ("Chicago","IL"), ("Dallas","TX"), ("San Francisco","CA"),
    ("Seattle","WA"), ("Miami","FL"), ("Boston","MA"), ("Denver","CO"),
    ("Atlanta","GA"), ("Los Angeles","CA")
]
customers = []
for cid in range(1, 501):
    first = f"First{cid}"
    last = f"Last{cid}"
    email = f"user{cid}@example.com"
    city, state = random.choice(cities)
    signup = datetime(2024,1,1) + timedelta(days=random.randint(0, 640))
    customers.append([cid, first, last, email, city, state, signup.date()])
df_customers = pd.DataFrame(customers, columns=[
    "customer_id","first_name","last_name","email","city","state","signup_date"
])

# Products
categories = {
    "Apparel": ["T-Shirt","Hoodie","Jeans","Sneakers","Cap"],
    "Accessories": ["Backpack","Watch","Wallet","Sunglasses"],
    "Home": ["Mug","Bottle","Notebook","Pen Set","Poster"]
}
pid = 1
products = []
for cat, subs in categories.items():
    for sub in subs:
        for i in range(1, 6):
            name = f"{sub} {i}"
            cost = round(random.uniform(5, 40), 2)
            price = round(cost * random.uniform(1.3, 2.2), 2)
            products.append([pid, cat, sub, name, cost, price])
            pid += 1
df_products = pd.DataFrame(products, columns=[
    "product_id","category","subcategory","product_name","unit_cost","unit_price"
])

# Orders and Order Items
start_date = datetime(2024, 7, 1)
end_date = datetime(2025, 10, 31)

orders = []
order_items = []

order_id = 1
order_item_id = 1

curr = start_date
while curr <= end_date:
    base_orders = 40 if curr.weekday() < 5 else 60
    lift = random.uniform(0.8, 1.3)
    num_orders = int(base_orders * lift)
    for _ in range(num_orders):
        cust = random.randint(1, len(df_customers))
        orders.append([order_id, curr.date(), cust])
        for _ in range(random.randint(1,4)):
            prod = random.randint(1, len(df_products))
            qty = random.choices([1,2,3], weights=[0.7,0.2,0.1])[0]
            order_items.append([order_item_id, order_id, prod, qty])
            order_item_id += 1
        order_id += 1
    curr += timedelta(days=1)

df_orders = pd.DataFrame(orders, columns=["order_id","order_date","customer_id"])
df_order_items = pd.DataFrame(order_items, columns=["order_item_id","order_id","product_id","quantity"])

# Inventory movements
inv_moves = []
for prod in df_products["product_id"]:
    on_hand = random.randint(200, 600)
    month = datetime(2024, 7, 1)
    while month <= end_date:
        restock = random.randint(50, 200)
        sold = random.randint(30, 180)
        inv_moves.append([prod, month.date(), restock, sold, on_hand])
        on_hand = max(on_hand + restock - sold, 0)
        month = (month.replace(day=1) + timedelta(days=32)).replace(day=1)

df_inv = pd.DataFrame(inv_moves, columns=["product_id","month","restock_qty","sold_qty","on_hand_estimate"])

# Marketing spend
channels = ["Search","Social","Email","Affiliate","Display"]
mkt = []
month = datetime(2024, 7, 1)
while month <= end_date:
    for ch in channels:
        spend = round(random.uniform(5000,20000),2)
        mkt.append([month.date(), ch, spend])
    month = (month.replace(day=1) + timedelta(days=32)).replace(day=1)
df_mkt = pd.DataFrame(mkt, columns=["month","channel","spend"])

# Save raw CSVs
df_customers.to_csv(DATA_DIR / "customers.csv", index=False)
df_products.to_csv(DATA_DIR / "products.csv", index=False)
df_orders.to_csv(DATA_DIR / "orders.csv", index=False)
df_order_items.to_csv(DATA_DIR / "order_items.csv", index=False)
df_inv.to_csv(DATA_DIR / "inventory_movements.csv", index=False)
df_mkt.to_csv(DATA_DIR / "marketing_spend.csv", index=False)

# Build DuckDB warehouse
if Path(WAREHOUSE).exists():
    Path(WAREHOUSE).unlink()

con = duckdb.connect(str(WAREHOUSE))
con.execute("install httpfs; load httpfs;")

con.execute("create table customers as select * from read_csv_auto(?);", [str(DATA_DIR / "customers.csv")])
con.execute("create table products as select * from read_csv_auto(?);", [str(DATA_DIR / "products.csv")])
con.execute("create table orders as select * from read_csv_auto(?);", [str(DATA_DIR / "orders.csv")])
con.execute("create table order_items as select * from read_csv_auto(?);", [str(DATA_DIR / "order_items.csv")])
con.execute("create table inventory_movements as select * from read_csv_auto(?);", [str(DATA_DIR / "inventory_movements.csv")])
con.execute("create table marketing_spend as select * from read_csv_auto(?);", [str(DATA_DIR / "marketing_spend.csv")])

# Create views from SQL files
def run_sql(path, view_name):
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    con.execute(f"create or replace view {view_name} as {sql}")

BASE_DIR = Path(__file__).resolve().parents[1]
run_sql(BASE_DIR / "analytics" / "marts" / "dim_customers.sql", "dim_customers")
run_sql(BASE_DIR / "analytics" / "marts" / "dim_products.sql", "dim_products")
run_sql(BASE_DIR / "analytics" / "marts" / "fact_order_items.sql", "fact_order_items")

run_sql(BASE_DIR / "analytics" / "metrics" / "kpi_daily_revenue.sql", "kpi_daily_revenue")
run_sql(BASE_DIR / "analytics" / "metrics" / "kpi_repeat_rate.sql", "kpi_repeat_rate")
run_sql(BASE_DIR / "analytics" / "metrics" / "kpi_gross_margin.sql", "kpi_gross_margin")

con.execute("create index if not exists idx_orders_date on orders(order_date)")
con.execute("create index if not exists idx_order_items_order on order_items(order_id)")
con.execute("create index if not exists idx_order_items_prod on order_items(product_id)")

print("Warehouse built at:", WAREHOUSE)
