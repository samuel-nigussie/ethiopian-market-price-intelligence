"""
Replay producer: reads the cleaned Ethiopia price CSV in chronological order
and streams each row to Kafka as if it were arriving live.
"""

import csv
import json
import time
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC = "market-prices"
CSV_PATH = "data/raw/ethiopia_prices_clean.csv"

SECONDS_PER_BATCH = 0.05
BATCH_SIZE = 25


def to_float(value):
    if value in (None, "", "NaN", "nan"):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def row_to_event(row):
    return {
        "market": row["mkt_name"],
        "region": row["adm1_name"],
        "zone": row["adm2_name"],
        "lat": to_float(row["lat"]),
        "lon": to_float(row["lon"]),
        "commodity": row["commodity"],
        "price_date": row["price_date"],
        "currency": row["currency"],
        "price_open": to_float(row["open"]),
        "price_high": to_float(row["high"]),
        "price_low": to_float(row["low"]),
        "price_close": to_float(row["close"]),
        "inflation_pct": to_float(row["inflation"]),
        "trust_score": to_float(row["trust"]),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    rows.sort(key=lambda r: datetime.strptime(r["price_date"], "%Y-%m-%d"))

    print(f"Loaded {len(rows)} rows. Starting replay to topic '{TOPIC}'...")

    sent = 0
    for row in rows:
        event = row_to_event(row)
        if event["price_close"] is None:
            continue

        producer.send(TOPIC, value=event)
        sent += 1

        if sent % BATCH_SIZE == 0:
            producer.flush()
            print(f"Sent {sent} events... last: {event['market']} / {event['commodity']} / {event['price_date']}")
            time.sleep(SECONDS_PER_BATCH)

    producer.flush()
    print(f"Done. Total events sent: {sent}")


if __name__ == "__main__":
    main()
