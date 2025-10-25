from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, regexp_replace, length, when, to_date,
    year, month, sum as _sum, expr, lit, count
)

# CONFIG / PATHS 
BASE   = Path(__file__).resolve().parents[1]
RAW    = BASE / "data" / "raw"
SILVER = BASE / "data" / "clean"
GOLD   = BASE / "data" / "gold"
QUAR   = BASE / "data" / "quarantine"

for p in [SILVER, GOLD, QUAR]:
    p.mkdir(parents=True, exist_ok=True)

RAW_FILE = RAW / "Superstore.csv"   # change if your filename differs
RUN_ID   = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

# Quality gate: if more than this fraction of rows turn bad → fail the job
MAX_BAD_FRACTION = 0.05   # 5%

#  SPARK SESSION 
spark = (
    SparkSession.builder
        .appName("Retail_ETL_Bronze_Silver_Gold")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
)

print("Spark version:", spark.version)

# HELPERS 
def normalize_columns(df):
    """snake_case all columns (handles spaces, dots, brackets, etc.)"""
    for c in df.columns:
        new_c = c.strip().lower()
        for ch in [" ", ".", "-", "/", "(", ")", "[", "]"]:
            new_c = new_c.replace(ch, "_")
        new_c = "_".join([p for p in new_c.split("_") if p])  # collapse repeats
        if new_c != c:
            df = df.withColumnRenamed(c, new_c)
    return df

def find_col(df, candidates):
    """return the first column present (case-insensitive) from candidates list"""
    lc = [c.lower() for c in df.columns]
    for name in candidates:
        if name in lc:
            return df.columns[lc.index(name)]
    return None

def safe_money_to_double(df, colname):
    """
    Clean a currency/number-as-text column safely:
    - strip currency symbols, spaces, commas
    - empty string -> NULL
    - try_cast to double (Spark 4-friendly)
    returns (df_with_clean_col, clean_col_name)
    """
    raw = colname
    clean = f"{colname}_clean"
    # remove $, ₹, €, £, spaces and commas
    df = df.withColumn(clean, regexp_replace(col(raw), r"[\\$,₹,€,£,\\s,]", ""))
    # empty -> NULL
    df = df.withColumn(clean, when(length(trim(col(clean))) == 0, None).otherwise(col(clean)))
    # try_cast to double (returns NULL if malformed)
    df = df.withColumn(clean, expr(f"try_cast({clean} as double)"))
    return df, clean

# ===================== 1) EXTRACT (Bronze) =====================
df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)   # ok for now; we sanitize anyway
         .csv(str(RAW_FILE))
)

row_in = df.count()
print(f"Loaded raw rows: {row_in}")

#  2) TRANSFORM (to Silver)
df = normalize_columns(df)

# Identify key columns across variants
order_date_col = find_col(df, ["order_date", "orderdate", "order_date_"])
sales_col      = find_col(df, ["sales", "revenue", "amount"])
profit_col     = find_col(df, ["profit"])
discount_col   = find_col(df, ["discount"])
qty_col        = find_col(df, ["quantity", "qty"])

if not order_date_col or not sales_col:
    raise ValueError(f"Missing required columns. Found: {df.columns}")

# Parse order_date (try common US format then generic)
df = df.withColumn(order_date_col, to_date(col(order_date_col), "M/d/yyyy")) \
       .withColumn(order_date_col, to_date(col(order_date_col)))  # fallback

# Clean money-like fields safely (Spark 4 strict casting)
df, sales_clean = safe_money_to_double(df, sales_col)
if profit_col:
    df, profit_clean = safe_money_to_double(df, profit_col)
else:
    profit_clean = None

# Discount can be 0–1 or 0–100; we'll just clean to double and leave interpretation later
if discount_col:
    df, discount_clean = safe_money_to_double(df, discount_col)
else:
    discount_clean = None

# Quantity to integer if present
if qty_col:
    df = df.withColumn(f"{qty_col}_clean",
                       expr(f"try_cast(regexp_replace({qty_col}, '[\\s,]', '') as int)"))
    qty_clean = f"{qty_col}_clean"
else:
    qty_clean = None

# Basic row quality filters (keep good rows)
good = (
    df.filter(col(order_date_col).isNotNull())
      .filter(col(sales_clean).isNotNull())
      .filter(col(sales_clean) > 0)
)

# Collect "bad" rows for quarantine (anything that failed the above conditions)
bad = df.exceptAll(good)

row_good = good.count()
row_bad  = bad.count()
bad_frac = row_bad / max(row_in, 1)

print(f"Rows in: {row_in} | good: {row_good} | bad: {row_bad} ({bad_frac:.2%})")

# Write a small sample of bad rows for debugging (do not block)
if row_bad > 0:
    (bad
        .withColumn("run_id", lit(RUN_ID))
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(str(QUAR / f"superstore_bad_rows_run={RUN_ID}"))
    )
    print(f"Quarantined bad rows sample → {QUAR}/superstore_bad_rows_run={RUN_ID}")

# Quality gate: fail if too many bad rows
if bad_frac > MAX_BAD_FRACTION:
    raise RuntimeError(
        f"Aborting: bad row fraction {bad_frac:.2%} > threshold {MAX_BAD_FRACTION:.2%}. "
        f"Check quarantine folder."
    )

# Standardize dims (optional: category lower/trim if exists)
if "category" in [c.lower() for c in good.columns]:
    idx = [c.lower() for c in good.columns].index("category")
    cat_col = good.columns[idx]
    good = good.withColumn(cat_col, lower(trim(col(cat_col))))
else:
    cat_col = None

# Add derived partitions (year, month)
good = good.withColumn("year", year(order_date_col)).withColumn("month", month(order_date_col))

#  3) LOAD (Silver Parquet, partitioned) 
silver_dir = SILVER / "superstore_clean_parquet"
(good
    .write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(str(silver_dir))
)
print(f"Silver → {silver_dir}")

# 4) AGGREGATE (Gold)
# Monthly revenue
monthly = (
    good.groupBy("year", "month")
        .agg(_sum(col(sales_clean)).alias("total_sales"))
        .orderBy("year", "month")
)

(good
 .select(order_date_col, sales_clean, "year", "month")
 .limit(5)
 .show(truncate=False))

(monthly
    .coalesce(1)  # small output for easy inspection
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(str(GOLD / "monthly_revenue"))
)
print(f"Gold → {GOLD}/monthly_revenue")

# Optional: monthly by category (if exists)
if cat_col:
    monthly_cat = (
        good.groupBy("year", "month", cat_col)
            .agg(_sum(col(sales_clean)).alias("total_sales"))
    )
    (monthly_cat
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(str(GOLD / "monthly_revenue_by_category"))
    )
    print(f"Gold → {GOLD}/monthly_revenue_by_category")

# 5) METRICS (tiny audit) 
metrics = spark.createDataFrame(
    [(RUN_ID, int(row_in), int(row_good), int(row_bad), float(bad_frac))],
    ["run_id", "row_in", "row_good", "row_bad", "bad_fraction"]
)
(metrics
 .coalesce(1)
 .write.mode("overwrite").option("header", True)
 .csv(str(GOLD / f"_metrics_run={RUN_ID}"))
)
print(f"Metrics → {GOLD}/_metrics_run={RUN_ID}")

#DONE
spark.stop()
print("ETL build is complete ")
