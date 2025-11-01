# Retail BI Starter (DuckDB + SQL + Streamlit)

A compact, end-to-end Business Intelligence project you can fork and use to showcase BI fundamentals: clean ETL, an analytics data model, documented SQL, a lightweight dashboard, and basic CI tests.

**Stack**
- DuckDB for an embedded analytics database
- Pandas for simple ETL
- SQL files for metrics logic (documented)
- Streamlit for a shareable dashboard
- pytest for sanity checks
- GitHub Actions for CI (runs tests and lints)

## What's inside

```
bi-starter/
├─ app/
│  └─ streamlit_app.py
├─ analytics/
│  ├─ marts/
│  │  ├─ dim_customers.sql
│  │  ├─ dim_products.sql
│  │  └─ fact_order_items.sql
│  └─ metrics/
│     ├─ kpi_daily_revenue.sql
│     ├─ kpi_repeat_rate.sql
│     └─ kpi_gross_margin.sql
├─ data/
│  ├─ raw/
│  │  ├─ customers.csv
│  │  ├─ products.csv
│  │  ├─ orders.csv
│  │  ├─ order_items.csv
│  │  ├─ inventory_movements.csv
│  │  └─ marketing_spend.csv
│  └─ warehouse.duckdb   (created by ETL)
├─ src/
│  ├─ etl.py
│  └─ build_metrics.py
├─ tests/
│  └─ test_etl.py
├─ .github/workflows/python-ci.yml
├─ requirements.txt
├─ LICENSE
└─ README.md
```

## Quickstart

1) Install deps:
```
pip install -r requirements.txt
```

2) Run ETL to build `data/warehouse.duckdb`:
```
python src/etl.py
```

3) Optional: build materialized metric tables:
```
python src/build_metrics.py
```

4) Launch dashboard:
```
streamlit run app/streamlit_app.py
```

## Use this for your GitHub
- Add a concise project description in the README (what problem, why the stack, key decisions).
- Commit small, well-named PRs to show your process.
- Open Issues for follow-ups (e.g., add cohort analysis, anomaly detection, dbt migration).

## Next steps (ideas)
- Replace synthetic data with a real dataset (e.g., open retail or e-commerce data).
- Add cohort analysis by first purchase month.
- Migrate SQL to dbt for lineage / tests.
- Add Dockerfile + docker-compose for one-command spin up.
- Add Superset/Metabase for BI tool experience.
