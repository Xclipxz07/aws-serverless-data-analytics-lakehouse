"""
AWS Sales Data Analytics - Local Athena SQL Query Runner
Author: Prabhat Dhar
Description:
    Simulates Amazon Athena serverless SQL analytics over S3 Parquet data
    using DuckDB in-process SQL engine. Executes the core business SQL queries
    and formats the executive analytics output.
"""

import os
import duckdb


def run_analytics_queries(parquet_path: str):
    """Execute standard AWS Athena business analytics queries over Parquet data."""
    con = duckdb.connect()
    
    print("\n" + "=" * 70)
    print("📊 1. REGIONAL SALES & PROFITABILITY (Amazon Athena SQL)")
    print("=" * 70)
    q1 = f"""
    SELECT 
        CustomerRegion,
        COUNT(OrderID) AS TotalOrders,
        SUM(Quantity) AS TotalUnitsSold,
        ROUND(SUM(NetSales), 2) AS TotalRevenue_USD,
        ROUND(SUM(Profit), 2) AS TotalProfit_USD,
        ROUND(AVG(ProfitMarginPct), 2) AS AvgProfitMargin_Pct
    FROM '{parquet_path}'
    GROUP BY CustomerRegion
    ORDER BY TotalRevenue_USD DESC;
    """
    df1 = con.execute(q1).fetchdf()
    print(df1.to_string(index=False))

    print("\n" + "=" * 70)
    print("🏆 2. TOP PRODUCT CATEGORIES & AVERAGE ORDER VALUE")
    print("=" * 70)
    q2 = f"""
    SELECT 
        ProductCategory,
        COUNT(DISTINCT OrderID) AS TotalOrders,
        ROUND(SUM(NetSales), 2) AS CategoryRevenue_USD,
        ROUND(AVG(NetSales), 2) AS AvgOrderValue_USD,
        ROUND(SUM(Profit), 2) AS TotalProfit_USD
    FROM '{parquet_path}'
    GROUP BY ProductCategory
    ORDER BY CategoryRevenue_USD DESC;
    """
    df2 = con.execute(q2).fetchdf()
    print(df2.to_string(index=False))

    print("\n" + "=" * 70)
    print("💳 3. PAYMENT METHOD & SHIPPING STATUS BREAKDOWN")
    print("=" * 70)
    q3 = f"""
    SELECT 
        PaymentMethod,
        ShippingStatus,
        COUNT(OrderID) AS OrderCount,
        ROUND(SUM(NetSales), 2) AS NetRevenue_USD
    FROM '{parquet_path}'
    GROUP BY PaymentMethod, ShippingStatus
    ORDER BY PaymentMethod, OrderCount DESC;
    """
    df3 = con.execute(q3).fetchdf()
    print(df3.head(10).to_string(index=False))

    print("\n" + "=" * 70)
    print("📈 4. MONTHLY SALES GROWTH & PERFORMANCE")
    print("=" * 70)
    q4 = f"""
    SELECT 
        YearMonth,
        COUNT(OrderID) AS MonthlyOrders,
        ROUND(SUM(NetSales), 2) AS MonthlyRevenue_USD,
        ROUND(SUM(Profit), 2) AS MonthlyProfit_USD
    FROM '{parquet_path}'
    GROUP BY YearMonth
    ORDER BY YearMonth ASC;
    """
    df4 = con.execute(q4).fetchdf()
    print(df4.to_string(index=False))

    print("\n" + "=" * 70)
    print("🎉 ALL AWS ATHENA SQL ANALYTICS QUERIES EXECUTED SUCCESSFULLY!")
    print("=" * 70)


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    parquet_file = os.path.join(base_dir, "data", "processed", "sales_parquet", "sales_data_unified.parquet")
    
    if not os.path.exists(parquet_file):
        from sales_etl_pipeline import run_pipeline
        input_csv = os.path.join(base_dir, "data", "raw", "sample_sales_data.csv")
        run_pipeline(input_csv, os.path.join(base_dir, "data", "processed", "sales_parquet"))
        
    run_analytics_queries(parquet_file)
