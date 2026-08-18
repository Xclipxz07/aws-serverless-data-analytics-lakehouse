"""
AWS Sales Data Analytics - Simple ETL Pipeline
Author: Prabhat Dhar
Description:
    Reads raw retail sales data from 'sample_sales_data.csv',
    cleans and validates records, computes profit margins,
    and converts the data to partitioned Snappy Parquet format
    ready for Amazon S3, AWS Glue Catalog, and Amazon Athena.
"""

import os
import pandas as pd


def load_raw_sales(csv_path: str) -> pd.DataFrame:
    """Load raw sales CSV into a Pandas DataFrame."""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Source file not found: {csv_path}")
    
    df = pd.read_csv(csv_path)
    print(f"📥 Loaded {len(df):,} raw sales records from: {os.path.basename(csv_path)}")
    return df


def clean_and_transform_sales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform data cleansing and transformation:
    1. Parse OrderDate to standard datetime.
    2. Ensure numeric types for financial fields.
    3. Calculate ProfitMarginPct = (Profit / NetSales) * 100.
    4. Remove any duplicate rows.
    """
    df = df.copy()
    
    # 1. Deduplicate
    initial_len = len(df)
    df.drop_duplicates(subset=["OrderID"], inplace=True)
    if len(df) < initial_len:
        print(f"🧹 Removed {initial_len - len(df)} duplicate orders.")

    # 2. Date parsing
    df["OrderDate"] = pd.to_datetime(df["OrderDate"])
    df["Year"] = df["OrderDate"].dt.year
    df["Month"] = df["OrderDate"].dt.strftime("%m")
    df["YearMonth"] = df["OrderDate"].dt.strftime("%Y-%m")

    # 3. Fill missing or NaN values with safe defaults
    df["DiscountPct"] = df["DiscountPct"].fillna(0.0)
    df["ShippingStatus"] = df["ShippingStatus"].fillna("Unknown")

    # 4. Compute Profit Margin (%)
    # Avoid division by zero
    df["ProfitMarginPct"] = (df["Profit"] / df["NetSales"]).replace([float("inf"), -float("inf")], 0.0) * 100
    df["ProfitMarginPct"] = df["ProfitMarginPct"].round(2)

    # 5. Data validation checks
    valid_records = df[(df["Quantity"] > 0) & (df["NetSales"] >= 0)]
    print(f"✨ Transformed & Validated {len(valid_records):,} sales records.")
    return valid_records


def save_to_parquet(df: pd.DataFrame, output_dir: str) -> str:
    """
    Export DataFrame to Apache Parquet format with Snappy compression.
    Partitioned by CustomerRegion for optimized Amazon Athena query scans.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Save standard unified parquet
    unified_parquet_path = os.path.join(output_dir, "sales_data_unified.parquet")
    df.to_parquet(unified_parquet_path, engine="pyarrow", compression="snappy", index=False)
    
    # Save region-partitioned dataset (Simulating AWS S3 Lakehouse structure)
    partitioned_path = os.path.join(output_dir, "partitioned_by_region")
    df.to_parquet(
        partitioned_path,
        engine="pyarrow",
        compression="snappy",
        partition_cols=["CustomerRegion"],
        index=False
    )
    
    print(f"💾 Exported Snappy Parquet dataset to: {output_dir}")
    return unified_parquet_path


def run_pipeline(
    csv_input_path: str = "data/raw/sample_sales_data.csv",
    output_parquet_dir: str = "data/processed/sales_parquet"
) -> pd.DataFrame:
    """Main execution function for the Sales Data ETL Pipeline."""
    print("=" * 65)
    print("🚀 STARTING AWS SALES DATA ANALYTICS ETL PIPELINE")
    print("=" * 65)
    
    raw_df = load_raw_sales(csv_input_path)
    clean_df = clean_and_transform_sales(raw_df)
    save_to_parquet(clean_df, output_parquet_dir)
    
    print("=" * 65)
    print("✅ ETL PIPELINE COMPLETED SUCCESSFULLY!")
    print("=" * 65)
    return clean_df


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_csv = os.path.join(base_dir, "data", "raw", "sample_sales_data.csv")
    output_dir = os.path.join(base_dir, "data", "processed", "sales_parquet")
    
    run_pipeline(input_csv, output_dir)
