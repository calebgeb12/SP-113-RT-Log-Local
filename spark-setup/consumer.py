from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# -------------------------------
# 1. Spark setup (quiet + optimized)
# -------------------------------
spark = SparkSession.builder \
    .appName("KafkaSimpleAnalysis") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

# Hide runtime log spam
spark.sparkContext.setLogLevel("ERROR")

# -------------------------------
# 2. Read from Kafka
# -------------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "web-logs") \
    .option("startingOffsets", "latest") \
    .load()

logs_df = df.selectExpr("CAST(value AS STRING) AS log_line")

# -------------------------------
# 3. Simple analytic
# -------------------------------
count_df = logs_df.groupBy().agg(count("*").alias("total_messages"))

# -------------------------------
# 4. Output cleanly
# -------------------------------
query = count_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
