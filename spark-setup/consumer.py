import threading
import socket
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, window, col, current_timestamp
import time

metrics = {
    "total_messages": 0,
    "spikes_detected": 0,
    "latest_rate": 0.0,
    "latest_window": "",
    "flood_404": 0
}

def start_socket_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("0.0.0.0", 5050))
    server.listen(1)
    conn, addr = server.accept()

    while True:
        try:
            conn.sendall((json.dumps(metrics) + "\n").encode("utf-8"))
        except (BrokenPipeError, ConnectionResetError):
            conn, addr = server.accept()
        time.sleep(1)

threading.Thread(target=start_socket_server, daemon=True).start()

spark = SparkSession.builder \
    .appName("KafkaTrafficRateMonitor") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "web-logs") \
    .option("startingOffsets", "latest") \
    .load()

logs_df = df.selectExpr("CAST(value AS STRING) AS log_line") \
             .withColumn("timestamp", current_timestamp())

spike_counts = logs_df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(window(col("timestamp"), "5 seconds")) \
    .agg(count("*").alias("request_count"))

flood_counts = logs_df.filter(col("log_line").contains("404")) \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(window(col("timestamp"), "5 seconds")) \
    .agg(count("*").alias("error_404s"))

def detect_404_flood(batch_df, batch_id):
    count_404 = batch_df.count()
    start_time = time.strftime("%H:%M:%S")
    metrics["latest_window"] = start_time
    metrics["flood_404"] += count_404
    print(f"404 errors in this batch: {count_404}")
    if count_404 > 5:
        print(f"404 flood detected — {count_404} errors in this window")

def detect_spikes(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return
    for row in rows:
        count_now = row["request_count"]
        start = row["window"].start
        end = row["window"].end
        rate = count_now / 5.0
        metrics["latest_window"] = f"{start.strftime('%H:%M:%S')} - {end.strftime('%H:%M:%S')}"
        metrics["total_messages"] += count_now
        metrics["latest_rate"] = rate
        print(f"\n----> Window {start} - {end}")
        print(f"Total requests: {count_now}")
        print(f"Rate: {rate:.2f} msgs/sec")
        if count_now > 15 or rate > 3.0:
            metrics["spikes_detected"] += 1
            print(f"TRAFFIC SPIKE DETECTED — {count_now} requests ({rate:.2f} msg/sec)")

spike_query = spike_counts.writeStream \
    .outputMode("update") \
    .trigger(processingTime='5 seconds') \
    .foreachBatch(detect_spikes) \
    .start()

flood_query = flood_counts.writeStream \
    .outputMode("update") \
    .trigger(processingTime='5 seconds') \
    .foreachBatch(detect_404_flood) \
    .start()

spark.streams.awaitAnyTermination()

