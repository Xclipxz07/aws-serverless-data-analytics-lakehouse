import os
import pytest
import pandas as pd
import duckdb
from src.sales_etl_pipeline import load_raw_sales, clean_and_transform_sales, save_to_parquet

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(BASE_DIR, "data", "raw", "sample_sales_data.csv")
PARQUET_DIR = os.path.join(BASE_DIR, "data", "processed", "sales_parquet")


def test_raw_csv_exists_and_loads():
    """Verify sample_sales_data.csv exists and contains required columns."""
    assert os.path.exists(CSV_PATH), f"CSV not found at: {CSV_PATH}"
    df = load_raw_sales(CSV_PATH)
    assert len(df) > 0
    expected_cols = ["OrderID", "OrderDate", "CustomerID", "CustomerRegion", "ProductCategory", "NetSales", "Profit"]
    for col in expected_cols:
        assert col in df.columns, f"Missing required column: {col}"


def test_clean_and_transform():
    """Verify transformation logic and derived columns."""
    df_raw = load_raw_sales(CSV_PATH)
    df_clean = clean_and_transform_sales(df_raw)
    
    # Assert date parsed and profit margin added
    assert "Year" in df_clean.columns
    assert "Month" in df_clean.columns
    assert "ProfitMarginPct" in df_clean.columns
    assert (df_clean["ProfitMarginPct"] <= 100).all()
    assert (df_clean["Quantity"] > 0).all()


def test_parquet_export_and_read():
    """Verify Parquet export is valid and readable."""
    df_raw = load_raw_sales(CSV_PATH)
    df_clean = clean_and_transform_sales(df_raw)
    parquet_file = save_to_parquet(df_clean, PARQUET_DIR)
    
    assert os.path.exists(parquet_file)
    df_parquet = pd.read_parquet(parquet_file)
    assert len(df_parquet) == len(df_clean)


def test_sql_analytics_simulation():
    """Verify SQL query runs on the Parquet dataset via DuckDB."""
    parquet_file = os.path.join(PARQUET_DIR, "sales_data_unified.parquet")
    con = duckdb.connect()
    
    res = con.execute(f"SELECT COUNT(*), SUM(NetSales) FROM '{parquet_file}'").fetchall()
    total_orders, total_sales = res[0]
    
    assert total_orders == 1000
    assert total_sales > 0
