import pandas as pd
from pathlib import Path
import logging

#logging 

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s")


#paths

BASE = Path(__file__).resolve().parents[1]
RAW = BASE / "data" / "raw"
SILVER = BASE / "data" / "clean"
GOLD = BASE / "data" / "gold"

for p in [SILVER, GOLD]:
    p.mkdir(parents=True, exist_ok=True)

#1. Extract 
logging.info("Reading raw dataset...")
file_path = RAW / "Superstore.csv"   
df = pd.read_csv(file_path, encoding="latin-1")
logging.info(f"Loaded {df.shape[0]} rows.")

#2. Transform 
logging.info("Cleaning dataset...")
clean = df.copy()
clean.columns = (
    clean.columns.str.strip()
                 .str.lower()
                 .str.replace(r"[^0-9a-z]+", "_", regex=True)
)
clean["order_date"] = pd.to_datetime(clean["order_date"], errors="coerce")
clean = clean.dropna(subset=["order_date", "sales"])
clean = clean[clean["sales"] > 0]

#3. silver_csv = SILVER / "superstore_clean.csv"
silver_csv = SILVER / "superstore_clean.csv"
clean.to_csv(silver_csv, index=False)
logging.info(f"Saved cleaned data → {silver_csv}")

#4. Aggregate to Gold 
logging.info("Creating monthly revenue summary...")
clean["month"] = clean["order_date"].dt.to_period("M").dt.to_timestamp()
monthly = (
    clean.groupby("month", as_index=False)["sales"].sum()
)
gold_csv = GOLD / "monthly_revenue.csv"
monthly.to_csv(gold_csv, index=False)
logging.info(f"Saved monthly revenue → {gold_csv}")

logging.info("ETL built is cdcomplete ")