
from pathlib import Path
import duckdb

BASE = Path(__file__).resolve().parents[1]
DB = BASE / "data" / "warehouse.duckdb"

con = duckdb.connect(str(DB))

con.execute("create or replace table m_kpi_daily_revenue as select * from kpi_daily_revenue")
con.execute("create or replace table m_kpi_gross_margin as select * from kpi_gross_margin")
con.execute("create or replace table m_kpi_repeat_rate as select * from kpi_repeat_rate")

print("Materialized KPI tables created.")
