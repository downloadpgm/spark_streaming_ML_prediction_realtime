from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkStreaming").getOrCreate()

tweets = spark.readStream\
    .format("socket")\
    .option("host", "localhost")\
    .option("port", 9009) \
    .load()

query = tweets.coalesce(1).writeStream \
    .outputMode("append") \
    .option("encoding", "utf-8") \
    .format("csv") \
    .option("path", "./csv") \
    .option("checkpointLocation", "./check") \
    .start()

query.awaitTermination()

