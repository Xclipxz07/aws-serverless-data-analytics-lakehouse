-- 01. CREATE EXTERNAL TABLE IN AWS ATHENA & GLUE CATALOG
-- Reads Snappy Parquet files stored in Amazon S3
CREATE EXTERNAL TABLE IF NOT EXISTS aws_sales_analytics_db.sales_data_parquet (
    OrderID INT,
    OrderDate STRING,
    Year INT,
    Month STRING,
    YearMonth STRING,
    CustomerID STRING,
    ProductCategory STRING,
    ProductName STRING,
    UnitPrice DOUBLE,
    Quantity INT,
    DiscountPct DOUBLE,
    GrossSales DOUBLE,
    DiscountAmount DOUBLE,
    NetSales DOUBLE,
    COGS DOUBLE,
    Profit DOUBLE,
    ProfitMarginPct DOUBLE,
    PaymentMethod STRING,
    ShippingStatus STRING
)
PARTITIONED BY (CustomerRegion STRING)
STORED AS PARQUET
LOCATION 's3://my-aws-sales-analytics-lakehouse/data/processed/sales_parquet/'
TBLPROPERTIES ("parquet.compress"="SNAPPY");
