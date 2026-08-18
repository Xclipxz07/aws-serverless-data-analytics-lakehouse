-- 04. MONTHLY SALES GROWTH & PERFORMANCE (Amazon Athena)
-- Purpose: Track month-over-month revenue trends and order velocity
SELECT 
    YearMonth,
    COUNT(OrderID) AS MonthlyOrders,
    SUM(Quantity) AS TotalUnits,
    ROUND(SUM(NetSales), 2) AS MonthlyRevenue_USD,
    ROUND(SUM(Profit), 2) AS MonthlyProfit_USD,
    ROUND(AVG(ProfitMarginPct), 2) AS AvgMarginPct
FROM aws_sales_analytics_db.sales_data_parquet
GROUP BY YearMonth
ORDER BY YearMonth ASC;
