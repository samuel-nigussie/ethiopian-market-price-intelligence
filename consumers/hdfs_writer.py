"""
HDFS Writer - Consumer Group A
Reads price events from the Kafka 'market-prices' topic and archives them
to HDFS in batches, partitioned by date.
"""

import json
import os
import subprocess
import time
from datetime import datetime, timezone
from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
TOPIC = "market-prices"
GROUP_ID = "hdfs-writer-group"

LOCAL_STAGING_DIR = "data/landing_zone"
HDFS_TARGET_DIR = "/market-prices"

BATCH_SIZE = 100
BATCH_TIMEOUT_SECONDS = 10


def ensure_local_staging_dir():
    os.makedirs(LOCAL_STAGING_DIR, exist_ok=True)


def flush_batch(buffer, batch_num):
    if not buffer:
        return

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    filename = f"prices_{today}_batch{batch_num}_{int(time.time())}.json"
    local_path = os.path.join(LOCAL_STAGING_DIR, filename)

    with open(local_path, "w", encoding="utf-8") as f:
        for event in buffer:
            f.write(json.dumps(event) + "\n")

    hdfs_date_dir = f"{HDFS_TARGET_DIR}/{today}"
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_date_dir], check=False)

    result = subprocess.run(
        ["hdfs", "dfs", "-put", local_path, f"{hdfs_date_dir}/{filename}"],
        capture_output=True, text=True
    )

    if result.returncode == 0:
        print(f"[HDFS Writer] Flushed {len(buffer)} events -> {hdfs_date_dir}/{filename}")
    else:
        print(f"[HDFS Writer] ERROR writing to HDFS: {result.stderr}")


def main():
    ensure_local_staging_dir()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    print(f"[HDFS Writer] Listening on topic '{TOPIC}' as group '{GROUP_ID}'...")

    buffer = []
    batch_num = 0
    last_flush_time = time.time()

    try:
        for message in consumer:
            buffer.append(message.value)

            time_since_flush = time.time() - last_flush_time
            if len(buffer) >= BATCH_SIZE or time_since_flush >= BATCH_TIMEOUT_SECONDS:
                batch_num += 1
                flush_batch(buffer, batch_num)
                consumer.commit()
                buffer = []
                last_flush_time = time.time()

    except KeyboardInterrupt:
        print("\n[HDFS Writer] Stopping... flushing remaining buffer.")
        if buffer:
            batch_num += 1
            flush_batch(buffer, batch_num)
            consumer.commit()
    finally:
        consumer.close()
        print("[HDFS Writer] Shut down cleanly.")


if __name__ == "__main__":
    main()
