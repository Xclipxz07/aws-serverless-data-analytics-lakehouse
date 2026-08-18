"""
Amazon Athena Query Runner & Cost Optimization Tracker
Dispatches SQL queries to Amazon Athena, monitors execution, retrieves results, and reports scanned data volume/costs.
"""

import time
import logging
import pandas as pd
from typing import Dict, Any, Tuple

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:
    boto3 = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Athena pricing standard: $5.00 per TB ($0.00000476837 per MB) scanned
ATHENA_COST_PER_MB = 5.0 / (1024 * 1024)

class AthenaQueryRunner:
    def __init__(self, database: str, output_s3_uri: str, workgroup: str = "primary", aws_region: str = "eu-west-2"):
        self.database = database
        self.output_s3_uri = output_s3_uri
        self.workgroup = workgroup
        self.aws_region = aws_region
        self.athena_client = boto3.client("athena", region_name=aws_region) if boto3 else None
        self.s3_client = boto3.client("s3", region_name=aws_region) if boto3 else None

    def execute_query(self, query_sql: str, max_wait_seconds: int = 60) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Executes a SQL query on Amazon Athena and blocks until finished or timed out."""
        if not self.athena_client:
            raise RuntimeError("boto3 is not available or AWS credentials are not configured.")

        logger.info(f"🚀 Submitting query to Athena (DB: {self.database}, Workgroup: {self.workgroup})...")
        
        response = self.athena_client.start_query_execution(
            QueryString=query_sql,
            QueryExecutionContext={"Database": self.database},
            ResultConfiguration={"OutputLocation": self.output_s3_uri},
            WorkGroup=self.workgroup
        )
        
        query_id = response["QueryExecutionId"]
        logger.info(f"  📌 Query Execution ID: {query_id}")

        start_time = time.time()
        while time.time() - start_time < max_wait_seconds:
            execution = self.athena_client.get_query_execution(QueryExecutionId=query_id)
            status = execution["QueryExecution"]["Status"]["State"]
            
            if status == "SUCCEEDED":
                stats = execution["QueryExecution"].get("Statistics", {})
                scanned_bytes = stats.get("DataScannedInBytes", 0)
                scanned_mb = scanned_bytes / (1024 * 1024)
                exec_time_ms = stats.get("EngineExecutionTimeInMillis", 0)
                est_cost = scanned_mb * ATHENA_COST_PER_MB

                meta = {
                    "query_id": query_id,
                    "status": status,
                    "data_scanned_mb": round(scanned_mb, 4),
                    "execution_time_ms": exec_time_ms,
                    "estimated_cost_usd": f"${est_cost:.6f}"
                }
                
                logger.info(f"✅ Query succeeded! Scanned: {meta['data_scanned_mb']} MB | Execution: {exec_time_ms} ms | Cost: {meta['estimated_cost_usd']}")
                results_df = self._fetch_results_dataframe(query_id)
                return results_df, meta

            elif status in ["FAILED", "CANCELLED"]:
                reason = execution["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
                raise RuntimeError(f"Athena query {status}: {reason}")

            time.sleep(1.5)

        raise TimeoutError(f"Athena query {query_id} timed out after {max_wait_seconds} seconds.")

    def _fetch_results_dataframe(self, query_id: str) -> pd.DataFrame:
        """Fetches query result set rows and returns as a pandas DataFrame."""
        result_pager = self.athena_client.get_paginator("get_query_results")
        page_iter = result_pager.paginate(QueryExecutionId=query_id)

        rows = []
        headers = []

        for i, page in enumerate(page_iter):
            result_set = page["ResultSet"]
            if i == 0:
                # First row contains column names
                raw_headers = result_set["Rows"][0]["Data"]
                headers = [h.get("VarCharValue", f"col_{idx}") for idx, h in enumerate(raw_headers)]
                data_rows = result_set["Rows"][1:]
            else:
                data_rows = result_set["Rows"]

            for row in data_rows:
                row_data = [item.get("VarCharValue", None) for item in row["Data"]]
                rows.append(row_data)

        return pd.DataFrame(rows, columns=headers)
