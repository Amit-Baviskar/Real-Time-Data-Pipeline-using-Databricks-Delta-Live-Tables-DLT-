#ecommerce Data Large

# Retry using pandas' to_parquet which may use fastparquet if available.
# Fallback: if no parquet engine is available, write as CSV and zip; but user requested parquet.
# We'll try to use pandas to_parquet with 'pyarrow' or 'fastparquet' if available; otherwise, install is not possible.
import os, sys, zipfile, json
import numpy as np, pandas as pd, random
from datetime import datetime, timedelta
random.seed(42)
np.random.seed(42)

out_dir = "/mnt/data/ecommerce_data"
os.makedirs(out_dir, exist_ok=True)

def random_dates(n, start="2018-01-01", end="2025-09-30"):
    start_u = int(pd.Timestamp(start).timestamp())
    end_u = int(pd.Timestamp(end).timestamp())
    return pd.to_datetime(np.random.randint(start_u, end_u, n), unit='s').date

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
region_sample = region_df.sample(10, random_state=1).reset_index(drop=True)

# Products Large (10k)
n_products = 10000
product_ids = [f"PROD{100000+i}" for i in range(n_products)]
categories = ["Electronics","Home & Kitchen","Clothing","Beauty","Sports","Toys","Books","Grocery","Automotive","Health"]
vendor_ids = [f"VEND{1000 + (i%500)}" for i in range(n_products)]
prod_df = pd.DataFrame({
    "ProductID": product_ids,
    "ProductName": [f"Product_{i}" for i in range(n_products)],
    "Category": np.random.choice(categories, n_products),
    "VendorID": vendor_ids,
    "UnitPrice": np.round(np.random.uniform(50, 20000, n_products),2),
    "StockQty": np.random.randint(0,2000, n_products),
    "LaunchDate": random_dates(n_products)
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
    "SignupDate": signup_dates,
    "RegionID": region_for_customers
})
cust_sample = cust_df.sample(10, random_state=3).reset_index(drop=True)

# Orders & Returns Large (1,000,000+)
n_orders = 1000000
order_ids = [f"ORD{2000000+i}" for i in range(n_orders)]
order_customers = np.random.choice(customer_ids, n_orders)
order_products = np.random.choice(product_ids, n_orders)
quantities = np.random.randint(1,5, n_orders)
unit_price_map = dict(zip(prod_df["ProductID"], prod_df["UnitPrice"]))
unit_prices = np.array([unit_price_map[p] for p in order_products])
total_amount = np.round(unit_prices * quantities,2)
order_dates = random_dates(n_orders)

is_return = np.random.rand(n_orders) < 0.10
return_ids = [f"RET{3000000+i}" if is_return[i] else None for i in range(n_orders)]
order_dates_np = np.array(order_dates)
return_dates = []
refund_amounts = []
reasons_pool = ["Defective","No longer needed","Wrong item shipped","Size/fit issue","Item not as described","Late delivery","Other"]
for i in range(n_orders):
    if is_return[i]:
        od = pd.to_datetime(order_dates_np[i])
        rd = od + pd.Timedelta(days=int(np.random.randint(1,31)))
        return_dates.append(rd)
        refund_amounts.append(round(total_amount[i] * np.random.choice([0.5,0.75,1.0]),2))
    else:
        return_dates.append(pd.NaT)
        refund_amounts.append(None)

orders_df = pd.DataFrame({
    "OrderID": order_ids,
    "CustomerID": order_customers,
    "ProductID": order_products,
    "Quantity": quantities,
    "UnitPrice": unit_prices,
    "TotalAmount": total_amount,
    "OrderDate": pd.to_datetime(order_dates),
    "ReturnID": return_ids,
    "ReturnDate": pd.to_datetime(return_dates),
    "RefundAmount": refund_amounts,
    "Reason": [random.choice(reasons_pool) if is_return[i] else None for i in range(n_orders)]
})

prod_map = prod_df.set_index("ProductID")[["VendorID"]]
orders_df = orders_df.join(prod_map, on="ProductID")
orders_df["RegionID"] = pd.Series(orders_df["CustomerID"]).map(cust_df.set_index("CustomerID")["RegionID"])

orders_sample = orders_df.sample(200, random_state=4).reset_index(drop=True)

# Write to parquet using pandas; will try 'pyarrow' then 'fastparquet'
write_info = []
def try_write(df, path):
    # try pyarrow
    try:
        df.to_parquet(path, engine='pyarrow', index=False)
        return True, 'pyarrow'
    except Exception as e1:
        try:
            df.to_parquet(path, engine='fastparquet', index=False)
            return True, 'fastparquet'
        except Exception as e2:
            return False, str(e1) + " | " + str(e2)

paths = []
# customers
os.makedirs(os.path.join(out_dir, "customers"), exist_ok=True)
ok, eng = try_write(cust_sample, os.path.join(out_dir, "customers", "customers_sample.parquet"))
write_info.append(("customers_sample.parquet", ok, eng))
ok, eng = try_write(cust_df, os.path.join(out_dir, "customers", "customers_large.parquet"))
write_info.append(("customers_large.parquet", ok, eng))

# products
os.makedirs(os.path.join(out_dir, "products"), exist_ok=True)
ok, eng = try_write(prod_sample, os.path.join(out_dir, "products", "products_sample.parquet"))
write_info.append(("products_sample.parquet", ok, eng))
ok, eng = try_write(prod_df, os.path.join(out_dir, "products", "products_large.parquet"))
write_info.append(("products_large.parquet", ok, eng))

# orders
os.makedirs(os.path.join(out_dir, "orders_returns"), exist_ok=True)
ok, eng = try_write(orders_sample, os.path.join(out_dir, "orders_returns", "orders_returns_sample.parquet"))
write_info.append(("orders_returns_sample.parquet", ok, eng))
ok, eng = try_write(orders_df, os.path.join(out_dir, "orders_returns", "orders_returns_large.parquet"))
write_info.append(("orders_returns_large.parquet", ok, eng))

# regions
os.makedirs(os.path.join(out_dir, "regions"), exist_ok=True)
ok, eng = try_write(region_sample, os.path.join(out_dir, "regions", "regions_sample.parquet"))
write_info.append(("regions_sample.parquet", ok, eng))
ok, eng = try_write(region_df, os.path.join(out_dir, "regions", "regions_large.parquet"))
write_info.append(("regions_large.parquet", ok, eng))

# Zip if parquet files created (check success)
zip_path = "/mnt/data/ecommerce_data_parquet_datasets.zip"
with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
    for root, _, files in os.walk(out_dir):
        for f in files:
            full = os.path.join(root, f)
            # only include parquet files that were written successfully in write_info or any files present
            if f.endswith(".parquet"):
                arcname = os.path.relpath(full, "/mnt/data")
                zf.write(full, arcname)

# prepare output
files_list = []
for root, _, files in os.walk(out_dir):
    for f in files:
        full = os.path.join(root, f)
        files_list.append((os.path.relpath(full, "/mnt/data"), os.path.getsize(full)))

output = {"zip": zip_path, "files": files_list, "write_info": write_info}
print(json.dumps(output))
