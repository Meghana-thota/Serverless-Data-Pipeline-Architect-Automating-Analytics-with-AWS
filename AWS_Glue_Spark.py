# etl_clean_spark_glue.py  (aligned to your buckets)

import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, regexp_replace, length, when, to_date,
    year, month, sum as _sum, expr, lit
)

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# ---------- CONFIG / S3 PATHS (YOUR BUCKETS) ----------
RAW_FILE = "s3://retail-project-raw/superstore.csv"             # input CSV

# use processed bucket for silver outputs and quarantine
SILVER   = "s3://retail-project-processed/silver/"
QUAR     = "s3://retail-project-processed/quarantine/"

# use dashboard bucket for gold aggregates
GOLD     = "s3://retail-dashboard/gold/"

RUN_ID   = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
MAX_BAD_FRACTION = 0.05  # 5%

# ---------- GLUE / SPARK ----------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext.getOrCreate()
glue_ctx = GlueContext(sc)
spark: SparkSession = glue_ctx.spark_session
job = Job(glue_ctx); job.init(args['JOB_NAME'], args)

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.shuffle.partitions", "200")

# ---------- HELPERS ----------
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
    clean = f"{colname}_clean"
    df = df.withColumn(clean, regexp_replace(col(colname), r"[\$,₹,€,£,\s,]", ""))
    df = df.withColumn(clean, when(length(trim(col(clean))) == 0, None).otherwise(col(clean)))
    df = df.withColumn(clean, expr(f"try_cast({clean} as double)"))
    return df, clean

# ---------- 1) EXTRACT ----------
df = (spark.read.option("header", True).option("inferSchema", True).csv(RAW_FILE))
row_in = df.count()

# ---------- 2) TRANSFORM (Silver) ----------
df = normalize_columns(df)

order_date_col = find_col(df, ["order_date", "orderdate", "order_date_"])
sales_col      = find_col(df, ["sales", "revenue", "amount"])
profit_col     = find_col(df, ["profit"])
discount_col   = find_col(df, ["discount"])
qty_col        = find_col(df, ["quantity", "qty"])

if not order_date_col or not sales_col:
    raise ValueError(f"Missing required columns. Found: {df.columns}")

df = df.withColumn(order_date_col, to_date(col(order_date_col), "M/d/yyyy")) \
       .withColumn(order_date_col, to_date(col(order_date_col)))

df, sales_clean = safe_money_to_double(df, sales_col)
profit_clean = None
if profit_col:    df, profit_clean = safe_money_to_double(df, profit_col)
discount_clean = None
if discount_col:  df, discount_clean = safe_money_to_double(df, discount_col)

qty_clean = None
if qty_col:
    df = df.withColumn(f"{qty_col}_clean",
                       expr(f"try_cast(regexp_replace({qty_col}, '[\\s,]', '') as int)"))
    qty_clean = f"{qty_col}_clean"

good = (df.filter(col(order_date_col).isNotNull())
          .filter(col(sales_clean).isNotNull())
          .filter(col(sales_clean) > 0))
bad = df.exceptAll(good)

row_bad = bad.count()
bad_frac = row_bad / max(row_in, 1)

if row_bad > 0:
    (bad.withColumn("run_id", lit(RUN_ID))
        .coalesce(1).write.mode("overwrite").option("header", True)
        .csv(QUAR + f"superstore_bad_rows_run={RUN_ID}/"))

if bad_frac > MAX_BAD_FRACTION:
    raise RuntimeError(f"Aborting: bad rows {bad_frac:.2%} > {MAX_BAD_FRACTION:.2%}")

lower_cols = [c.lower() for c in good.columns]
cat_col = None
if "category" in lower_cols:
    cat_col = good.columns[lower_cols.index("category")]
    good = good.withColumn(cat_col, lower(trim(col(cat_col))))

good = good.withColumn("year", year(order_date_col)).withColumn("month", month(order_date_col))

# ---------- 3) LOAD (Silver Parquet) ----------
silver_dir = SILVER + "superstore_clean_parquet/"
(good.write.mode("overwrite").partitionBy("year", "month").parquet(silver_dir))

# ---------- 4) GOLD (Aggregates) ----------
monthly = (good.groupBy("year", "month")
                .agg(_sum(col(sales_clean)).alias("total_sales"))
                .orderBy("year", "month"))

(monthly.coalesce(1).write.mode("overwrite").option("header", True)
        .csv(GOLD + "monthly_revenue/"))

if cat_col:
    monthly_cat = (good.groupBy("year", "month", cat_col)
                        .agg(_sum(col(sales_clean)).alias("total_sales")))
    (monthly_cat.coalesce(1).write.mode("overwrite").option("header", True)
        .csv(GOLD + "monthly_revenue_by_category/"))

# ---------- 5) METRICS ----------
metrics = spark.createDataFrame(
    [(RUN_ID, int(row_in), int(good.count()), int(row_bad), float(bad_frac))],
    ["run_id", "row_in", "row_good", "row_bad", "bad_fraction"]
)
(metrics.coalesce(1).write.mode("overwrite").option("header", True)
        .csv(GOLD + f"_metrics_run={RUN_ID}/"))

job.commit()
print("ETL completed.")
