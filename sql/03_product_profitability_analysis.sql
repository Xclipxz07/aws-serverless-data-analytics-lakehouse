-- 03. PRODUCT CATEGORY & MARGIN ANALYSIS (Amazon Athena)
-- Purpose: Evaluate category revenue contribution, volume, and average order value (AOV)
SELECT 
    ProductCategory,
    COUNT(DISTINCT OrderID) AS TotalOrders,
    SUM(Quantity) AS UnitsSold,
    ROUND(SUM(NetSales), 2) AS CategoryRevenue_USD,
    ROUND(AVG(NetSales), 2) AS AvgOrderValue_USD,
    ROUND(SUM(Profit), 2) AS TotalProfit_USD
FROM aws_sales_analytics_db.sales_data_parquet
GROUP BY ProductCategory
ORDER BY CategoryRevenue_USD DESC;
