#ecommerce Data Sample

# Creating synthetic datasets and saving as Parquet files, then zipping them.
# This will generate:
# - customers_sample.parquet (10 rows) and customers_large.parquet (100,000 rows)
# - products_sample.parquet (10 rows) and products_large.parquet (10,000 rows)
# - regions_sample.parquet (10 rows) and regions_large.parquet (10,000 rows)
# - orders_returns_sample.parquet (200 rows) and orders_returns_large.parquet (1,000,000 rows)
# Files will be written under /mnt/data/ecommerce_data/ and a final zip will be created.
# After running, the notebook environment will show the files and the zip for download.
import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import random
import string
import pyarrow as pa
import pyarrow.parquet as pq
import zipfile

random.seed(42)
np.random.seed(42)

out_dir = "/mnt/data/ecommerce_data"
os.makedirs(out_dir, exist_ok=True)

def random_dates(n, start="2018-01-01", end="2025-09-30"):
    start_u = int(pd.Timestamp(start).timestamp())
    end_u = int(pd.Timestamp(end).timestamp())
    return pd.to_datetime(np.random.randint(start_u, end_u, n), unit='s').date

# Simple name generator
first_names = ["Amit","Rahul","Sneha","Priya","Rohit","Anjali","Vivek","Kavya","Sanjay","Meera","Arjun","Nisha","Rakesh","Pooja","Kunal"]
last_names = ["Baviskar","Sharma","Patel","Gupta","Singh","Kumar","Verma","Nair","Iyer","Reddy","Joshi","Mehta","Bose","Chatterjee","Ibrahim"]

cities = ["Mumbai","Bengaluru","Delhi","Hyderabad","Chennai","Kolkata","Pune","Jaipur","Lucknow","Ahmedabad","Surat","Visakhapatnam"]
states = ["Maharashtra","Karnataka","Delhi","Telangana","Tamil Nadu","West Bengal","Maharashtra","Rajasthan","Uttar Pradesh","Gujarat","Gujarat","Andhra Pradesh"]
countries = ["India"] * len(cities)

# Generate Regions Large (10k)
n_regions = 10000
region_ids = [f"R{100000+i}" for i in range(n_regions)]
region_df = pd.DataFrame({
    "RegionID": region_ids,
    "Country": np.random.choice(countries, n_regions),
    "State": np.random.choice(states, n_regions),
    "City": np.random.choice(cities, n_regions),
    "PostalCode": np.random.randint(100000,999999, n_regions).astype(str)
})
# sample regions (10 rows)
region_sample = region_df.sample(10, random_state=1).reset_index(drop=True)

# Products Large (10k)
n_products = 10000
product_ids = [f"PROD{100000+i}" for i in range(n_products)]
categories = ["Electronics","Home & Kitchen","Clothing","Beauty","Sports","Toys","Books","Grocery","Automotive","Health"]
vendor_ids = [f"VEND{1000 + (i%500)}" for i in range(n_products)]  # 500 vendors
prod_df = pd.DataFrame({
    "ProductID": product_ids,
    "ProductName": [f"Product_{i}" for i in range(n_products)],
    "Category": np.random.choice(categories, n_products),
    "VendorID": vendor_ids,
    "UnitPrice": np.round(np.random.uniform(50, 20000, n_products),2),
    "StockQty": np.random.randint(0,2000, n_products),
    "LaunchDate": random_dates(n_products).astype('datetime64[ns]')
})
prod_sample = prod_df.sample(10, random_state=2).reset_index(drop=True)

# Customers Large (100k)
n_customers = 100000
customer_ids = [f"CUST{100000+i}" for i in range(n_customers)]
names = [f"{random.choice(first_names)} {random.choice(last_names)}" for _ in range(n_customers)]
emails = [f"user{100000+i}@example.com" for i in range(n_customers)]
genders = np.random.choice(["Male","Female","Other"], n_customers, p=[0.55,0.43,0.02])
ages = np.random.randint(18,75, n_customers)
signup_dates = random_dates(n_customers)
region_for_customers = np.random.choice(region_ids, n_customers)
loyalty = np.random.choice(["Bronze","Silver","Gold","Platinum"], n_customers, p=[0.6,0.25,0.1,0.05])
cust_df = pd.DataFrame({
    "CustomerID": customer_ids,
    "Name": names,
    "Gender": genders,
    "Age": ages,
    "Email": emails,
    "LoyaltyTier": loyalty,
    "SignupDate": signup_dates.astype('datetime64[ns]'),
    "RegionID": region_for_customers
})
cust_sample = cust_df.sample(10, random_state=3).reset_index(drop=True)

# Orders & Returns Large (1,000,000+)
n_orders = 1000000  # 1 million orders
# Generate order ids
order_ids = [f"ORD{2000000+i}" for i in range(n_orders)]
# pick customers and products
order_customers = np.random.choice(customer_ids, n_orders)
order_products = np.random.choice(product_ids, n_orders)
quantities = np.random.randint(1,5, n_orders)
unit_prices = pd.Series(order_products).map(prod_df.set_index("ProductID")["UnitPrice"]).values
# some products price mapping may be expensive; ensure alignment
total_amount = np.round(unit_prices * quantities,2)
order_dates = random_dates(n_orders)

# Introduce returns for ~10% orders
is_return = np.random.rand(n_orders) < 0.10
return_ids = [f"RET{3000000+i}" if is_return[i] else None for i in range(n_orders)]
# return dates after order date by 1-30 days
order_dates_np = np.array(order_dates)
return_dates = []
refund_amounts = []
reasons_pool = ["Defective","No longer needed","Wrong item shipped","Size/fit issue","Item not as described","Late delivery","Other"]
for i in range(n_orders):
    if is_return[i]:
        od = pd.to_datetime(order_dates_np[i])
        rd = od + pd.Timedelta(days=int(np.random.randint(1,31)))
        return_dates.append(rd.date())
        # partial refund possible
        refund_amounts.append(round(total_amount[i] * np.random.choice([0.5,0.75,1.0]),2))
    else:
        return_dates.append(None)
        refund_amounts.append(None)

orders_df = pd.DataFrame({
    "OrderID": order_ids,
    "CustomerID": order_customers,
    "ProductID": order_products,
    "Quantity": quantities,
    "UnitPrice": unit_prices,
    "TotalAmount": total_amount,
    "OrderDate": pd.to_datetime(order_dates).astype('datetime64[ns]'),
    "ReturnID": return_ids,
    "ReturnDate": pd.to_datetime(return_dates, errors='coerce').astype('datetime64[ns]'),
    "RefundAmount": refund_amounts,
    "Reason": [random.choice(reasons_pool) if is_return[i] else None for i in range(n_orders)]
})

# Add VendorID and RegionID by joining product and customer info
prod_map = prod_df.set_index("ProductID")[["VendorID"]]
orders_df = orders_df.join(prod_map, on="ProductID")
orders_df["RegionID"] = pd.Series(orders_df["CustomerID"]).map(cust_df.set_index("CustomerID")["RegionID"])

# Sample orders (200 rows)
orders_sample = orders_df.sample(200, random_state=4).reset_index(drop=True)

# Regions sample already created
# Write parquet files
def write_parquet(df, path):
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path)

base = out_dir
folders = {
    "customers": cust_df,
    "products": prod_df,
    "orders_returns": orders_df,
    "regions": region_df
}

# create directories
for name in folders.keys():
    os.makedirs(os.path.join(base, name), exist_ok=True)

# Write sample and large files
write_parquet(cust_sample, os.path.join(base, "customers", "customers_sample.parquet"))
write_parquet(cust_df, os.path.join(base, "customers", "customers_large.parquet"))

write_parquet(prod_sample, os.path.join(base, "products", "products_sample.parquet"))
write_parquet(prod_df, os.path.join(base, "products", "products_large.parquet"))

write_parquet(orders_sample, os.path.join(base, "orders_returns", "orders_returns_sample.parquet"))
write_parquet(orders_df, os.path.join(base, "orders_returns", "orders_returns_large.parquet"))

write_parquet(region_sample, os.path.join(base, "regions", "regions_sample.parquet"))
write_parquet(region_df, os.path.join(base, "regions", "regions_large.parquet"))

# Zip the entire folder
zip_path = "/mnt/data/ecommerce_data_zip.parquet_datasets.zip"
with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
    for root, _, files in os.walk(base):
        for f in files:
            full = os.path.join(root, f)
            arcname = os.path.relpath(full, "/mnt/data")
            zf.write(full, arcname)

# Show created files sizes
created_files = []
for root, _, files in os.walk(base):
    for f in files:
        full = os.path.join(root, f)
        created_files.append((os.path.relpath(full, "/mnt/data"), os.path.getsize(full)))
created_files.append((os.path.relpath(zip_path, "/mnt/data"), os.path.getsize(zip_path)))

import json
print(json.dumps({"zip": zip_path, "files": created_files}, default=str))

