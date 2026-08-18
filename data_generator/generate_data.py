"""
Project 4 - Synthetic Multi-Source Multi-Format Cloud Operational Log & Event Generator
Generates realistic serverless event streams partitioned by year/month/day across JSON, CSV, TSV, and XML.
"""

import os
import json
import uuid
import random
import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

SERVICES = [
    ("auth-service", ["/api/v1/auth/login", "/api/v1/auth/token", "/api/v1/auth/logout"]),
    ("catalog-service", ["/api/v1/products/search", "/api/v1/products/detail", "/api/v1/categories/list"]),
    ("cart-service", ["/api/v1/cart/add", "/api/v1/cart/view", "/api/v1/cart/checkout"]),
    ("payment-gateway", ["/api/v1/payment/charge", "/api/v1/payment/verify", "/api/v1/payment/refund"]),
    ("order-service", ["/api/v1/orders/create", "/api/v1/orders/status", "/api/v1/orders/history"])
]

REGIONS = ["eu-west-2", "eu-west-1", "us-east-1", "us-west-2", "ap-southeast-1"]
DEVICES = ["mobile_ios", "mobile_android", "desktop_mac", "desktop_windows", "tablet"]
HTTP_METHODS = {"GET": 0.60, "POST": 0.25, "PUT": 0.10, "DELETE": 0.05}

ERROR_PATTERNS = {
    400: ["Bad Request: Invalid Payload Format", "Missing Required Parameter 'session_id'"],
    401: ["Unauthorized: Expired JWT Token", "Invalid API Signature"],
    403: ["Forbidden: Insufficient IAM Permissions", "Tenant IP Blocklisted"],
    404: ["Resource Not Found: Product ID does not exist", "Route Endpoint Undefined"],
    500: ["InternalServerError: Database Connection Pool Exhausted", "NullPointer in PaymentWorker"],
    503: ["ServiceUnavailable: Downstream Dependency Timeout", "CircuitBreaker Triggered (High Load)"]
}

def get_status_and_error(service, endpoint, hour):
    is_peak = 14 <= hour <= 16
    if service == "payment-gateway" and is_peak and random.random() < 0.25:
        code = random.choice([500, 503])
        return code, random.choice(ERROR_PATTERNS[code])
    
    rand = random.random()
    if rand < 0.88:
        return 200, None
    elif rand < 0.93:
        code = random.choice([400, 401, 404])
        return code, random.choice(ERROR_PATTERNS[code])
    elif rand < 0.97:
        code = 403
        return code, random.choice(ERROR_PATTERNS[403])
    else:
        code = random.choice([500, 503])
        return code, random.choice(ERROR_PATTERNS[code])

def calculate_latency(service, status_code):
    base_latency = {
        "auth-service": 35.0,
        "catalog-service": 22.0,
        "cart-service": 48.0,
        "payment-gateway": 180.0,
        "order-service": 95.0
    }.get(service, 50.0)
    
    latency = random.gauss(base_latency, base_latency * 0.3)
    if status_code >= 500:
        latency += random.uniform(800.0, 3500.0)
    elif status_code in [401, 403]:
        latency = random.uniform(5.0, 20.0)
        
    return max(5.0, round(latency, 2))

def generate_single_event(target_date: datetime) -> dict:
    service_tuple = random.choice(SERVICES)
    service_name = service_tuple[0]
    endpoint = random.choice(service_tuple[1])
    method = random.choices(list(HTTP_METHODS.keys()), weights=list(HTTP_METHODS.values()))[0]
    
    random_second = random.randint(0, 86399)
    record_time = target_date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=random_second)
    
    status_code, error_msg = get_status_and_error(service_name, endpoint, record_time.hour)
    latency = calculate_latency(service_name, status_code)
    
    user_num = random.randint(1001, 1500)
    raw_user_id = f"user_{user_num:05d}"
    user_id_hash = hashlib.sha256(raw_user_id.encode('utf-8')).hexdigest()[:16]
    ip_addr = f"192.168.{random.randint(1, 20)}.{random.randint(2, 250)}"
    
    return {
        "request_id": str(uuid.uuid4()),
        "timestamp": record_time.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "year": f"{target_date.year:04d}",
        "month": f"{target_date.month:02d}",
        "day": f"{target_date.day:02d}",
        "service_name": service_name,
        "endpoint": endpoint,
        "http_method": method,
        "status_code": status_code,
        "response_time_ms": latency,
        "user_id_raw": raw_user_id,
        "user_id_hash": user_id_hash,
        "client_ip": ip_addr,
        "aws_region": random.choice(REGIONS),
        "device_type": random.choice(DEVICES),
        "bytes_sent": random.randint(120, 8500),
        "error_message": error_msg or ""
    }

def save_as_json(records, filepath):
    with open(filepath, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")

def save_as_csv(records, filepath):
    if not records:
        return
    headers = list(records[0].keys())
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(",".join(headers) + "\n")
        for r in records:
            vals = [str(r.get(h, "")).replace(",", ";") for h in headers]
            f.write(",".join(vals) + "\n")

def save_as_xml(records, filepath):
    root = ET.Element("events")
    for r in records:
        event_elem = ET.SubElement(root, "event")
        for k, v in r.items():
            child = ET.SubElement(event_elem, k)
            child.text = str(v)
    tree = ET.ElementTree(root)
    tree.write(filepath, encoding="utf-8", xml_declaration=True)

def generate_multi_day_dataset(base_dir: str, start_date_str: str = "2026-07-25", num_days: int = 5, records_per_day: int = 1500):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    output_files = []
    
    print(f"🚀 Generating {num_days} days of multi-format operational logs in: {base_dir}")
    for day_offset in range(num_days):
        curr_date = start_date + timedelta(days=day_offset)
        partition_path = os.path.join(
            base_dir,
            f"year={curr_date.year:04d}",
            f"month={curr_date.month:02d}",
            f"day={curr_date.day:02d}"
        )
        os.makedirs(partition_path, exist_ok=True)
        
        # Split day's records across multi-format sources (JSON: 60%, CSV: 25%, XML: 15%)
        count_json = int(records_per_day * 0.60)
        count_csv = int(records_per_day * 0.25)
        count_xml = records_per_day - count_json - count_csv
        
        rec_json = [generate_single_event(curr_date) for _ in range(count_json)]
        rec_csv = [generate_single_event(curr_date) for _ in range(count_csv)]
        rec_xml = [generate_single_event(curr_date) for _ in range(count_xml)]
        
        f_json = os.path.join(partition_path, f"api_events_{curr_date.strftime('%Y%m%d')}.json")
        f_csv = os.path.join(partition_path, f"web_traffic_{curr_date.strftime('%Y%m%d')}.csv")
        f_xml = os.path.join(partition_path, f"mobile_events_{curr_date.strftime('%Y%m%d')}.xml")
        
        save_as_json(rec_json, f_json)
        save_as_csv(rec_csv, f_csv)
        save_as_xml(rec_xml, f_xml)
        
        output_files.extend([f_json, f_csv, f_xml])
        print(f"  ✅ Partition created: {partition_path} -> JSON ({len(rec_json)}), CSV ({len(rec_csv)}), XML ({len(rec_xml)})")
        
    print(f"✨ Total generated: {num_days * records_per_day:,} multi-format events across {num_days} partitions.\n")
    return output_files

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    target_data_dir = os.path.join(project_root, "data", "raw_logs")
    generate_multi_day_dataset(target_data_dir)
