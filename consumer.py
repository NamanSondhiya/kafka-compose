from confluent_kafka import Consumer
import json

consumer_config = {
    "bootstrap.servers": "kafka-dev.lexidevice.com:9092",
    "group.id": "order-tracker",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)

consumer.subscribe(["orders22"])

print("Consumer is running and subscribed to orders22 topic")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Error", msg.error())
            continue
        
        value = msg.value().decode("utf-8")
        order = json.loads(value)
        
        print(f"Received order: {order['quantity']} x {order['item']} from {order['user']}")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()