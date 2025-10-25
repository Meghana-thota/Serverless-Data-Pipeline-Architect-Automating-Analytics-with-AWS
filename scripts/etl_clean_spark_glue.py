from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, regexp_replace, length, when, to_date,
    year, month, sum as _sum, expr, lit
)

# ===================== CONFIG / S3 PATHS =====================
RAW_FILE = "s3://retail-project-raw/Superstore.csv"
SILVER   = "s3://retail-project-clean/"
GOLD     = "s3://retail-project-gold/"
QUAR     = "s3://retail-project-quarantine/"
RUN_ID   = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

MAX_BAD_FRACTION = 0.05   # 5% quality tolerance

# ===================== SPARK SESSION =====================
spark = (
    SparkSession.builder
        .appName("Retail_ETL_AWS_Glue")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
)

print("Spark version:", spark.version)

# ===================== HELPERS =====================
def normalize_columns(df):
    for c in df.columns:
        new_c = c.strip().lower()
        for ch in [" ", ".", "-", "/", "(", ")", "[", "]"]:
            new_c = new_c.replace(ch, "_")
        new_c = "_".join([p for p in new_c.split("_") if p])
        if new_c != c:
            df = df.withColumnRenamed(c, new_c)
    return df

def find_col(df, candidates):
    lc = [c.lower() for c in df.columns]
    for name in candidates:
        if name in lc:
            return df.columns[lc.index(name)]
    return None

def safe_money_to_double(df, colname):
    raw = colname
    clean = f"{colname}_clean"
    df = df.withColumn(clean, regexp_replace(col(raw), r"[\\$,₹,€,£,\\s,]", ""))
    df = df.withColumn(clean, when(length(trim(col(clean))) == 0, None).otherwise(col(clean)))
    df = df.withColumn(clean, expr(f"try_cast({clean} as double)"))
    return df, clean

# ===================== 1) EXTRACT (Bronze) =====================
df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv(RAW_FILE)
)

row_in = df.count()
print(f"Loaded raw rows: {row_in}")

# ===================== 2) TRANSFORM (Silver) =====================
df = normalize_columns(df)

order_date_col = find_col(df, ["order_date", "orderdate"])
sales_col      = find_col(df, ["sales", "revenue", "amount"])
profit_col     = find_col(df, ["profit"])
discount_col   = find_col(df, ["discount"])
qty_col        = find_col(df, ["quantity", "qty"])

if not order_date_col or not sales_col:
    raise ValueError(f"Missing required columns. Found: {df.columns}")

df = df.withColumn(order_date_col, to_date(col(order_date_col), "M/d/yyyy"))
df = df.withColumn(order_date_col, to_date(col(order_date_col)))

df, sales_clean = safe_money_to_double(df, sales_col)
if profit_col:
    df, profit_clean = safe_money_to_double(df, profit_col)
else:
    profit_clean = None

if discount_col:
    df, discount_clean = safe_money_to_double(df, discount_col)
else:
    discount_clean = None

if qty_col:
    df = df.withColumn(f"{qty_col}_clean",
                       expr(f"try_cast(regexp_replace({qty_col}, '[\\s,]', '') as int)"))
    qty_clean = f"{qty_col}_clean"
else:
    qty_clean = None

good = (
    df.filter(col(order_date_col).isNotNull())
      .filter(col(sales_clean).isNotNull())
      .filter(col(sales_clean) > 0)
)

bad = df.exceptAll(good)
row_good = good.count()
row_bad  = bad.count()
bad_frac = row_bad / max(row_in, 1)
print(f"Rows in: {row_in} | good: {row_good} | bad: {row_bad} ({bad_frac:.2%})")

if row_bad > 0:
    (bad
        .withColumn("run_id", lit(RUN_ID))
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(f"{QUAR}/superstore_bad_rows_run={RUN_ID}")
    )
    print(f"Quarantined bad rows → {QUAR}/superstore_bad_rows_run={RUN_ID}")

if bad_frac > MAX_BAD_FRACTION:
    raise RuntimeError(f"Aborting: bad row fraction {bad_frac:.2%} > {MAX_BAD_FRACTION:.2%}")

if "category" in [c.lower() for c in good.columns]:
    idx = [c.lower() for c in good.columns].index("category")
    cat_col = good.columns[idx]
    good = good.withColumn(cat_col, lower(trim(col(cat_col))))
else:
    cat_col = None

good = good.withColumn("year", year(order_date_col)).withColumn("month", month(order_date_col))

# ===================== 3) LOAD (Silver Parquet) =====================
silver_dir = f"{SILVER}/superstore_clean_parquet"
(good
    .write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(silver_dir)
)
print(f"Silver → {silver_dir}")

# ===================== 4) AGGREGATE (Gold) =====================
monthly = (
    good.groupBy("year", "month")
        .agg(_sum(col(sales_clean)).alias("total_sales"))
        .orderBy("year", "month")
)

(monthly
    .coalesce(1)
    .write.mode("overwrite")
    .option("header", True)
    .csv(f"{GOLD}/monthly_revenue")
)
print(f"Gold → {GOLD}/monthly_revenue")

if cat_col:
    monthly_cat = (
        good.groupBy("year", "month", cat_col)
            .agg(_sum(col(sales_clean)).alias("total_sales"))
    )
    (monthly_cat
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(f"{GOLD}/monthly_revenue_by_category")
    )
    print(f"Gold → {GOLD}/monthly_revenue_by_category")

# ===================== 5) METRICS =====================
metrics = spark.createDataFrame(
    [(RUN_ID, int(row_in), int(row_good), int(row_bad), float(bad_frac))],
    ["run_id", "row_in", "row_good", "row_bad", "bad_fraction"]
)
(metrics
 .coalesce(1)
 .write.mode("overwrite").option("header", True)
 .csv(f"{GOLD}/_metrics_run={RUN_ID}")
)
print(f"Metrics → {GOLD}/_metrics_run={RUN_ID}")

spark.stop()
print("✅ AWS Glue ETL completed successfully!")
