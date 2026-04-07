from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("analytics").getOrCreate()

df = spark.read.json("transactions.json")

result = df.groupBy("merchant").count()

result.show()
