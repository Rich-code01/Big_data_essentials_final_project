"""
Generates a synthetic retail sales transaction dataset.
Target size: ~1.1 GB (comfortably over the 1GB course requirement).

Schema:
    transaction_id   - unique row id
    date             - transaction date (YYYY-MM-DD)
    day_of_week      - Mon..Sun
    store_id         - S001..S025
    region           - North/South/East/West/Central
    product_id       - P0001..P0200
    category         - Electronics/Grocery/Apparel/Home/Toys/Sports/Beauty/Books
    units_sold       - integer, influenced by weekday/season/promotion
    unit_price       - base price for the product (small variance)
    promotion_flag   - 0/1
    discount_pct     - 0 if no promotion, else 5-40%
    sales_amount     - units_sold * unit_price * (1 - discount_pct/100)

Run:
    python generate_sales_data.py
Output:
    sales_data.csv in the current folder
"""

import numpy as np
import pandas as pd
import os
from datetime import timedelta, date

# ---------------- Config ----------------
OUTPUT_FILE = "sales_data.csv"
TARGET_BYTES = int(1.1 * 1024 * 1024 * 1024)  # ~1.1 GB
CHUNK_ROWS = 500_000
START_DATE = date(2022, 1, 1)
END_DATE = date(2026, 12, 31)

NUM_STORES = 25
REGIONS = ["North", "South", "East", "West", "Central"]
NUM_PRODUCTS = 200
CATEGORIES = ["Electronics", "Grocery", "Apparel", "Home", "Toys", "Sports", "Beauty", "Books"]

rng = np.random.default_rng(seed=42)

# Fixed store -> region mapping
store_ids = [f"S{str(i).zfill(3)}" for i in range(1, NUM_STORES + 1)]
store_region_map = {s: REGIONS[i % len(REGIONS)] for i, s in enumerate(store_ids)}

# Fixed product -> category + base price mapping
product_ids = [f"P{str(i).zfill(4)}" for i in range(1, NUM_PRODUCTS + 1)]
product_category_map = {p: CATEGORIES[i % len(CATEGORIES)] for i, p in enumerate(product_ids)}
# base price varies by category to feel realistic
category_price_range = {
    "Electronics": (50, 800),
    "Grocery": (2, 40),
    "Apparel": (10, 120),
    "Home": (15, 300),
    "Toys": (5, 90),
    "Sports": (10, 250),
    "Beauty": (5, 100),
    "Books": (5, 60),
}
product_base_price = {}
for p in product_ids:
    lo, hi = category_price_range[product_category_map[p]]
    product_base_price[p] = round(rng.uniform(lo, hi), 2)

WEEKDAY_MULTIPLIER = {0: 0.9, 1: 0.85, 2: 0.9, 3: 0.95, 4: 1.2, 5: 1.5, 6: 1.3}  # Mon..Sun

def seasonal_multiplier(d: date) -> float:
    # Boost around Nov-Dec (holiday season), smaller boost mid-year sales
    if d.month in (11, 12):
        return 1.6
    if d.month == 7:
        return 1.15
    return 1.0


def generate_chunk(n_rows: int, start_id: int, all_dates: list) -> pd.DataFrame:
    dates = rng.choice(all_dates, size=n_rows)
    stores = rng.choice(store_ids, size=n_rows)
    products = rng.choice(product_ids, size=n_rows)

    weekday = np.array([d.weekday() for d in dates])
    day_names = np.array(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"])[weekday]

    wk_mult = np.array([WEEKDAY_MULTIPLIER[w] for w in weekday])
    season_mult = np.array([seasonal_multiplier(d) for d in dates])

    promo_flag = rng.choice([0, 1], size=n_rows, p=[0.8, 0.2])
    discount_pct = np.where(promo_flag == 1, rng.integers(5, 41, size=n_rows), 0)

    base_units = rng.poisson(lam=8, size=n_rows) + 1
    promo_boost = np.where(promo_flag == 1, rng.uniform(1.3, 2.0, size=n_rows), 1.0)
    units_sold = np.round(base_units * wk_mult * season_mult * promo_boost).astype(int)
    units_sold = np.clip(units_sold, 1, None)

    unit_price = np.array([product_base_price[p] for p in products])
    price_noise = rng.uniform(0.97, 1.03, size=n_rows)
    unit_price = np.round(unit_price * price_noise, 2)

    sales_amount = np.round(units_sold * unit_price * (1 - discount_pct / 100), 2)

    region = np.array([store_region_map[s] for s in stores])
    category = np.array([product_category_map[p] for p in products])

    transaction_id = np.arange(start_id, start_id + n_rows)

    df = pd.DataFrame({
        "transaction_id": transaction_id,
        "date": [d.isoformat() for d in dates],
        "day_of_week": day_names,
        "store_id": stores,
        "region": region,
        "product_id": products,
        "category": category,
        "units_sold": units_sold,
        "unit_price": unit_price,
        "promotion_flag": promo_flag,
        "discount_pct": discount_pct,
        "sales_amount": sales_amount,
    })
    return df


def main():
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    all_dates = []
    d = START_DATE
    while d <= END_DATE:
        all_dates.append(d)
        d += timedelta(days=1)

    total_rows = 0
    first_chunk = True

    while True:
        chunk = generate_chunk(CHUNK_ROWS, total_rows + 1, all_dates)
        chunk.to_csv(OUTPUT_FILE, mode="a", header=first_chunk, index=False)
        first_chunk = False
        total_rows += CHUNK_ROWS

        current_size = os.path.getsize(OUTPUT_FILE)
        print(f"Rows written: {total_rows:,} | File size: {current_size / (1024**2):.1f} MB")

        if current_size >= TARGET_BYTES:
            break

    print(f"\nDone. Total rows: {total_rows:,}")
    print(f"Final file size: {os.path.getsize(OUTPUT_FILE) / (1024**3):.2f} GB")
    print(f"Output: {os.path.abspath(OUTPUT_FILE)}")


if __name__ == "__main__":
    main()
