'''from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestSpark").getOrCreate()
print(spark.version)

df = spark.createDataFrame(
    [(1, "Alice", 2000), (2, "Bob", 3000), (3, "Charlie", 2500)],
    ["id", "name", "salary"]
)

df.show()
spark.stop()'''

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Verify").getOrCreate()

silver = spark.read.parquet("/Users/meghanathota/retail_project/data/clean/superstore_clean_parquet")
silver.select("order_date","sales","year","month").show(5)

gold = spark.read.option("header", True).csv("data/gold/monthly_revenue")
gold.orderBy("year","month").show()

spark.stop()

