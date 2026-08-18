-- 02. REGIONAL SALES & PROFIT MARGIN ANALYSIS (Amazon Athena)
-- Purpose: Identify highest-grossing territories and average profit margin %
SELECT 
    CustomerRegion,
    COUNT(OrderID) AS TotalOrders,
    SUM(Quantity) AS TotalUnitsSold,
    ROUND(SUM(NetSales), 2) AS TotalRevenue_USD,
    ROUND(SUM(Profit), 2) AS TotalProfit_USD,
    ROUND(AVG(ProfitMarginPct), 2) AS AvgProfitMargin_Pct
FROM aws_sales_analytics_db.sales_data_parquet
GROUP BY CustomerRegion
ORDER BY TotalRevenue_USD DESC;
