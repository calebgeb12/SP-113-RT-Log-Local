import socket
import json
import tkinter as tk
from tkinter import ttk
import threading

HOST = "localhost"
PORT = 5050
metrics = {}

def listen_to_spark():
    global metrics
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        buffer = ""
        while True:
            data = s.recv(4096).decode("utf-8")
            if not data:
                break
            buffer += data
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                try:
                    metrics = json.loads(line)
                except json.JSONDecodeError:
                    pass

def start_gui():
    root = tk.Tk()
    root.title("SP-113 RT Log Proc")

    ttk.Label(root, text="Anomaly Detection GUI", font=("Segoe UI", 16, "bold")).pack(pady=10)

    labels = {
        "rate": ttk.Label(root, text="Logs/sec: 0", font=("Segoe UI", 12)),
        "total": ttk.Label(root, text="Total logs: 0", font=("Segoe UI", 12)),
        "spikes": ttk.Label(root, text="Spikes detected: 0", font=("Segoe UI", 12)),
        "floods": ttk.Label(root, text="404 Floods: 0", font=("Segoe UI", 12)),
        "window": ttk.Label(root, text="Current window: N/A", font=("Segoe UI", 11)),
    }
    for l in labels.values():
        l.pack(padx=10, pady=5)

    def refresh():
        if metrics:
            labels["rate"].config(text=f"Logs/sec: {metrics.get('latest_rate', 0):.2f}")
            labels["total"].config(text=f"Total logs: {metrics.get('total_messages', 0)}")
            labels["spikes"].config(text=f"Spikes detected: {metrics.get('spikes_detected', 0)}")
            labels["floods"].config(text=f"404 Floods: {metrics.get('flood_404', 0)}")
            labels["window"].config(text=f"Current window: {metrics.get('latest_window', 'N/A')}")
        root.after(1000, refresh)

    refresh()
    root.mainloop()

threading.Thread(target=listen_to_spark, daemon=True).start()
start_gui()
