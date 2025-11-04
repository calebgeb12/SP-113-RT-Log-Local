import threading
import tkinter as tk
from tkinter import ttk
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

def start_gui():
    root = tk.Tk()
    root.title("📊 Anomaly Detection GUI")

    ttk.Label(root, text="Anomaly Detection GUI", font=("Segoe UI", 16, "bold")).pack(pady=10)

    rate_label = ttk.Label(root, text="Logs/sec: 0", font=("Segoe UI", 12))
    rate_label.pack(padx=10, pady=5)

    total_label = ttk.Label(root, text="Total logs: 0", font=("Segoe UI", 12))
    total_label.pack(padx=10, pady=5)

    spikes_label = ttk.Label(root, text="Spikes detected: 0", font=("Segoe UI", 12))
    spikes_label.pack(padx=10, pady=5)

    flood_label = ttk.Label(root, text="404 Floods: 0", font=("Segoe UI", 12))
    flood_label.pack(padx=10, pady=5)

    window_label = ttk.Label(root, text="Current window: N/A", font=("Segoe UI", 11))
    window_label.pack(padx=10, pady=5)

    def refresh():
        rate_label.config(text=f"Logs/sec: {metrics['latest_rate']:.2f}")
        total_label.config(text=f"Total logs: {metrics['total_messages']}")
        spikes_label.config(text=f"Spikes detected: {metrics['spikes_detected']}")
        flood_label.config(text=f"404 Floods: {metrics['flood_404']}")
        window_label.config(text=f"Current window: {metrics['latest_window']}")
        root.after(1000, refresh)

    refresh()
    root.mainloop()


# gui_thread = threading.Thread(target=start_gui, daemon=True)
# gui_thread.start()


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


spike_counts = logs_df.groupBy(window(col("timestamp"), "5 seconds")) \
                      .agg(count("*").alias("request_count"))


flood_counts = logs_df.filter(col("log_line").contains("404")) \
                      .groupBy(window(col("timestamp"), "5 seconds")) \
                      .agg(count("*").alias("error_404s"))


def detect_404_flood(batch_df, batch_id):
    print(f"\n[Batch {batch_id}] Columns: {batch_df.columns}")

    count_404 = batch_df.count()

    start_time = time.strftime("%H:%M:%S")
    metrics["latest_window"] = start_time
    metrics["flood_404"] += count_404

    print(f"\n🧾 Window starting {start_time}")
    print(f"   ➤ 404 errors in this batch: {count_404}")

    if count_404 > 5:
        print(f"🚨  404 FLOOD DETECTED — {count_404} errors in this window")



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
        print(f"\n📊 Window {start} - {end}")
        print(f"   ➤ Total requests: {count_now}")
        print(f"   ➤ Rate: {rate:.2f} msgs/sec")
        if count_now > 15 or rate > 3.0:
            metrics["spikes_detected"] += 1
            print(f"⚠️  TRAFFIC SPIKE DETECTED — {count_now} requests ({rate:.2f} msg/sec)")


spike_query = spike_counts.writeStream \
    .outputMode("update") \
    .foreachBatch(detect_spikes) \
    .start()

flood_query = flood_counts.writeStream \
    .outputMode("update") \
    .foreachBatch(detect_404_flood) \
    .start()

spark.streams.awaitAnyTermination()
