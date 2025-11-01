
import duckdb
import pandas as pd
import streamlit as st
from pathlib import Path

st.set_page_config(page_title="Retail BI Starter", layout="wide")

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "warehouse.duckdb"

st.title("Retail BI Starter Dashboard")

if not DB_PATH.exists():
    st.error("warehouse.duckdb not found. Run `python src/etl.py` first.")
    st.stop()

con = duckdb.connect(str(DB_PATH))

st.sidebar.header("Filters")
min_date = con.execute("select min(order_date) from orders").fetchone()[0]
max_date = con.execute("select max(order_date) from orders").fetchone()[0]
date_range = st.sidebar.date_input("Order date range", (min_date, max_date))

query_filter = ""
if isinstance(date_range, tuple) and len(date_range) == 2:
    start, end = date_range
    query_filter = f"where order_date between date '{start}' and date '{end}'"

kpi_rev = con.execute(f"""
select sum(line_revenue) from fact_order_items {query_filter};
""").fetchone()[0] or 0.0

kpi_margin_pct = con.execute(f"""
select case when sum(line_revenue)=0 then 0
else sum(line_gross_margin)/sum(line_revenue) end
from fact_order_items {query_filter};
""").fetchone()[0] or 0.0

repeat_rate = con.execute("select * from kpi_repeat_rate").fetchone()[0]

col1, col2, col3 = st.columns(3)
col1.metric("Revenue (filtered)", f"${kpi_rev:,.0f}")
col2.metric("Gross Margin % (all data)", f"{kpi_margin_pct*100:.1f}%")
col3.metric("Repeat Purchase Rate (all data)", f"{repeat_rate*100:.1f}%")

daily_rev = con.execute(f"""
select order_date, sum(line_revenue) as revenue
from fact_order_items
{query_filter}
group by 1 order by 1
""").fetchdf()

st.subheader("Daily Revenue")
st.line_chart(daily_rev, x="order_date", y="revenue")

top_products = con.execute(f"""
select p.product_name, sum(f.line_revenue) as revenue
from fact_order_items f
join products p using(product_id)
{query_filter}
group by 1
order by 2 desc
limit 15
""").fetchdf()

st.subheader("Top 15 Products by Revenue")
st.bar_chart(top_products.set_index("product_name"))

city_rev = con.execute(f"""
select c.city, sum(f.line_revenue) as revenue
from fact_order_items f
join customers c using(customer_id)
{query_filter}
group by 1 order by 2 desc limit 15
""").fetchdf()

st.subheader("Top Cities by Revenue")
st.bar_chart(city_rev.set_index("city"))

st.caption("Data: synthetic. Stack: DuckDB + SQL + Streamlit.")
