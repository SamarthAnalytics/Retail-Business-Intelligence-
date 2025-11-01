from pathlib import Path
import duckdb

def test_tables_exist():
    db = Path(__file__).resolve().parents[1] / "data" / "warehouse.duckdb"
    assert db.exists(), "Run `python src/etl.py` first to build the warehouse."
    con = duckdb.connect(str(db))
    for t in ["customers","products","orders","order_items"]:
        cnt = con.execute(f"select count(*) from {t}").fetchone()[0]
        assert cnt > 0, f"Table {t} should not be empty"

def test_metrics_views():
    db = Path(__file__).resolve().parents[1] / "data" / "warehouse.duckdb"
    con = duckdb.connect(str(db))
    rev = con.execute("select sum(line_revenue) from fact_order_items").fetchone()[0]
    assert rev is not None and rev > 0
