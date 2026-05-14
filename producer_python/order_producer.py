import json
import random
import time
from datetime import datetime

from confluent_kafka import Producer
from faker import Faker

from config.kafka_config import (
    BOOTSTRAP_SERVERS,
    TOPIC_NAME
)

fake = Faker()

# Kafka producer configuration
producer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS
}

producer = Producer(producer_config)

products = [
    "Laptop",
    "Phone",
    "Keyboard",
    "Monitor",
    "Mouse",
    "Headphones",
    "Tablet"
]

regions = [
    "Cape Town",
    "Johannesburg",
    "Durban",
    "Pretoria"
]


def generate_order():
    return {
        "order_id": fake.uuid4(),
        "customer_name": fake.name(),
        "product": random.choice(products),
        "amount": round(random.uniform(100, 50000), 2),
        "region": random.choice(regions),
        "timestamp": datetime.now().isoformat()
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Order sent to topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


print("Starting StreamMind Producer...\n")

while True:

    order = generate_order()

    producer.produce(
        TOPIC_NAME,
        json.dumps(order).encode("utf-8"),
        callback=delivery_report
    )

    producer.poll(1)

    print("Produced Order:")
    print(json.dumps(order, indent=2))
    print("-" * 50)

    time.sleep(2)