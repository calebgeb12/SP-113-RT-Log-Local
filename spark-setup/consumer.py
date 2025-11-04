from pyspark.sql import SparkSession
from pyspark.sql.functions import count, window, col, current_timestamp, round
import time

spark = SparkSession.builder \
    .appName("KafkaTrafficRateMonitor") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "web-logs") \
    .option("startingOffsets", "latest") \
    .load()

logs_df = df.selectExpr("CAST(value AS STRING) AS log_line") \
             .withColumn("timestamp", current_timestamp())

windowed_counts = logs_df \
    .groupBy(window(col("timestamp"), "5 seconds")) \
    .agg(count("*").alias("request_count"))


from pyspark.sql.functions import count, window, col, current_timestamp
import time

spark = SparkSession.builder \
    .appName("KafkaTrafficRateMonitor-NoPandas") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "web-logs") \
    .option("startingOffsets", "latest") \
    .load()

logs_df = df.selectExpr("CAST(value AS STRING) AS log_line") \
             .withColumn("timestamp", current_timestamp())

windowed_counts = logs_df \
    .groupBy(window(col("timestamp"), "5 seconds")) \
    .agg(count("*").alias("request_count"))


def detect_spikes(batch_df, batch_id):
    rows = batch_df.collect()   

    if not rows:
        return

    for row in rows:
        count_now = row["request_count"]
        start = row["window"].start
        end = row["window"].end
        rate = count_now / 5.0  

        print(f"\n📊 Window {start} - {end}")
        print(f"   ➤ Total requests: {count_now}")
        print(f"   ➤ Rate: {rate:.2f} msgs/sec")

        if count_now > 15 or rate > 3.0:
            print(f"⚠️  TRAFFIC SPIKE DETECTED — {count_now} requests ({rate:.2f} msg/sec)")

query = windowed_counts.writeStream \
    .outputMode("update") \
    .foreachBatch(detect_spikes) \
    .start()

query.awaitTermination()

def detect_spikes(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    data = batch_df.toPandas()
    for _, row in data.iterrows():
        count_now = row["request_count"]
        start = row["window"]["start"]
        end = row["window"]["end"]

        rate = count_now / 5.0

        SPIKE_THRESHOLD = 15
        SPIKE_RATE_THRESHOLD = 3.0   

        print(f"\n📊 Window {start} - {end}")
        print(f"   ➤ Total requests: {count_now}")
        print(f"   ➤ Rate: {rate:.2f} msgs/sec")

        if count_now > SPIKE_THRESHOLD or rate > SPIKE_RATE_THRESHOLD:
            print(f"⚠️  TRAFFIC SPIKE DETECTED — {count_now} requests ({rate:.2f} msg/sec)")

query = windowed_counts.writeStream \
    .outputMode("update") \
    .foreachBatch(detect_spikes) \
    .start()

query.awaitTermination()
