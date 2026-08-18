import csv
import random
from datetime import datetime, timedelta

random.seed(42)

regions = ["North America", "Europe", "Asia-Pacific", "Latin America", "Middle East"]
categories = {
    "Technology": [
        ("Cloud Server Subscription", 499.00),
        ("Developer Laptop Pro", 1299.00),
        ("AI Vision Camera", 249.00),
        ("Wireless Noise-Canceling Headset", 149.00),
        ("Mechanical Keyboard RGB", 89.00)
    ],
    "Office Supplies": [
        ("Ergonomic Desk Chair", 299.00),
        ("Standing Desk Motorized", 450.00),
        ("Dual Monitor Arm", 79.00),
        ("Desk LED Lighting Bar", 39.00),
        ("Eco Notebook & Pen Set", 19.00)
    ],
    "Furniture": [
        ("Executive Conference Table", 899.00),
        ("Acoustic Soundproofing Panel", 120.00),
        ("Mobile Storage Cabinet", 180.00),
        ("Lounge Waiting Sofa", 550.00)
    ]
}

payment_methods = ["Credit Card", "Bank Transfer", "PayPal", "Corporate Invoice"]
shipping_statuses = ["Delivered", "Delivered", "Delivered", "Shipped", "Processing"]

start_date = datetime(2025, 1, 1)

rows = []
for order_id in range(1001, 2001):
    random_days = random.randint(0, 364)
    order_date = start_date + timedelta(days=random_days)
    
    category = random.choice(list(categories.keys()))
    product_name, unit_price = random.choice(categories[category])
    
    quantity = random.randint(1, 10)
    discount_pct = random.choice([0.0, 0.05, 0.10, 0.15, 0.20])
    gross_amount = round(unit_price * quantity, 2)
    discount_amount = round(gross_amount * discount_pct, 2)
    net_sales = round(gross_amount - discount_amount, 2)
    
    # Cost of goods sold is ~60% of unit price
    cogs = round((unit_price * 0.60) * quantity, 2)
    profit = round(net_sales - cogs, 2)
    
    region = random.choice(regions)
    customer_id = f"CUST-{random.randint(100, 999)}"
    payment = random.choice(payment_methods)
    status = random.choice(shipping_statuses)
    
    rows.append({
        "OrderID": order_id,
        "OrderDate": order_date.strftime("%Y-%m-%d"),
        "Year": order_date.year,
        "Month": order_date.strftime("%m"),
        "CustomerID": customer_id,
        "CustomerRegion": region,
        "ProductCategory": category,
        "ProductName": product_name,
        "UnitPrice": unit_price,
        "Quantity": quantity,
        "DiscountPct": discount_pct,
        "GrossSales": gross_amount,
        "DiscountAmount": discount_amount,
        "NetSales": net_sales,
        "COGS": cogs,
        "Profit": profit,
        "PaymentMethod": payment,
        "ShippingStatus": status
    })

# Write to CSV
import os
os.makedirs("/Users/prabhat/Documents/practice/project4_aws_serverless_data_analytics/data/raw", exist_ok=True)
csv_path = "/Users/prabhat/Documents/practice/project4_aws_serverless_data_analytics/data/raw/sample_sales_data.csv"

fieldnames = list(rows[0].keys())
with open(csv_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print(f"✅ Generated {len(rows)} sales records -> {csv_path}")
