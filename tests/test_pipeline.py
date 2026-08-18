"""
Automated Unit and Integration Tests for AWS Serverless Data Analytics Pipeline
"""

import os
import shutil
import tempfile
import pytest
import pandas as pd
import duckdb
from datetime import datetime

from data_generator.generate_data import generate_single_event, generate_multi_day_dataset
from src.etl_pipeline import ServerlessDataETLPipeline

@pytest.fixture
def temp_env():
    """Creates a temporary workspace directory for test executions."""
    temp_dir = tempfile.mkdtemp()
    raw_dir = os.path.join(temp_dir, "raw_logs")
    processed_dir = os.path.join(temp_dir, "processed_parquet")
    yield temp_dir, raw_dir, processed_dir
    shutil.rmtree(temp_dir)

def test_data_generator_single_event_structure():
    """Verifies that generated records have all required fields and correct data types."""
    test_date = datetime(2026, 7, 27)
    record = generate_single_event(test_date)
    
    required_fields = [
        "request_id", "timestamp", "year", "month", "day", "service_name", 
        "endpoint", "http_method", "status_code", "response_time_ms", 
        "user_id_raw", "user_id_hash", "client_ip", "aws_region"
    ]
    for field in required_fields:
        assert field in record, f"Missing expected field: {field}"
        
    assert record["year"] == "2026"
    assert record["month"] == "07"
    assert record["day"] == "27"
    assert isinstance(record["response_time_ms"], (int, float))
    assert record["status_code"] in [200, 201, 400, 401, 403, 404, 500, 503]

def test_multi_format_data_generation_and_etl(temp_env):
    """Tests that multi-format files (JSON, CSV, XML) are generated, parsed, anonymized, and written to Parquet."""
    temp_dir, raw_dir, processed_dir = temp_env
    
    # 1. Generate sample multi-format dataset (2 days, 100 records/day)
    files = generate_multi_day_dataset(raw_dir, start_date_str="2026-07-27", num_days=2, records_per_day=100)
    assert len(files) == 6 # 3 files per day (JSON, CSV, XML) * 2 days
    
    # 2. Run ETL
    etl = ServerlessDataETLPipeline()
    processed_files = etl.process_and_convert_to_parquet(raw_dir, processed_dir)
    
    assert len(processed_files) == 2 # 1 parquet file per day partition
    for p_file in processed_files:
        assert os.path.exists(p_file)
        assert p_file.endswith(".parquet")
        
        df = pd.read_parquet(p_file)
        assert "user_id_raw" not in df.columns, "PII raw user ID must be stripped"
        assert "user_id_hash" in df.columns
        assert "is_error" in df.columns
        assert "latency_category" in df.columns
        assert df["user_id_hash"].str.startswith("anon-").all()
        assert len(df) == 100

def test_duckdb_sql_queries_execution(temp_env):
    """Tests that the analytical SQL queries run cleanly against partitioned files."""
    temp_dir, raw_dir, processed_dir = temp_env
    generate_multi_day_dataset(raw_dir, start_date_str="2026-07-27", num_days=2, records_per_day=200)
    
    etl = ServerlessDataETLPipeline()
    etl.process_and_convert_to_parquet(raw_dir, processed_dir)
    
    con = duckdb.connect(database=":memory:")
    parquet_glob = os.path.join(processed_dir, "*", "*", "*", "*.parquet").replace("\\", "/")
    
    con.execute(f"CREATE VIEW test_logs AS SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=true);")
    
    # Test Descriptive Query
    res_desc = con.execute("""
        SELECT COUNT(*) AS total_cnt, SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) AS error_cnt 
        FROM test_logs;
    """).fetchall()
    assert res_desc[0][0] == 400
    
    # Test Diagnostic Query
    res_diag = con.execute("""
        SELECT service_name, COUNT(*) FROM test_logs GROUP BY service_name;
    """).fetchall()
    assert len(res_diag) > 0
