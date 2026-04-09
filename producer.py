from confluent_kafka import Producer
import uuid
import json

producer_config = {
    "bootstrap.servers": "kafka-dev.lexidevice.com:9092"
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered {msg.value().decode('utf-8')}")

order = {
    "order_id": str(uuid.uuid4()),
    "user": "hritik",
    "item": "korean fired chicken",
    "quantity": 2
}

value = json.dumps(order).encode("utf-8")

producer.produce(
    topic="orders22",
    value=value,
    callback=delivery_report
)

producer.flush()