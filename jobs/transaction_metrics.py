from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count

spark = SparkSession.builder.appName("txn-analytics").getOrCreate()

transactions = spark.read.json("data/transactions.json")

metrics = (
    transactions
    .groupBy("merchant")
    .agg(
        sum("amount").alias("total_amount"),
        count("").alias("txn_count")
    )
)

metrics.show()
