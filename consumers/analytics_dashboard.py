import json
from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
TOPIC = "market-prices"
GROUP_ID = "analytics-dashboard-group-v2"


def safe_json_decode(raw_bytes):
    try:
        return json.loads(raw_bytes.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP_ID,
    value_deserializer=safe_json_decode,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)

print(
    f"[Dashboard] Listening on topic '{TOPIC}' "
    f"as group '{GROUP_ID}'..."
)

for message in consumer:
    event = message.value

    if event is None:
        continue

    print(event)
