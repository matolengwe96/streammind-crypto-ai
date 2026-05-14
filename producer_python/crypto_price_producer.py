import json
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

from config.kafka_config import BOOTSTRAP_SERVERS

TOPIC_NAME = "crypto_prices"

producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS
})

COINS = "bitcoin,ethereum,solana,cardano"
VS_CURRENCY = "usd"

URL = (
    "https://api.coingecko.com/api/v3/simple/price"
    f"?ids={COINS}"
    f"&vs_currencies={VS_CURRENCY}"
    "&include_24hr_change=true"
    "&include_last_updated_at=true"
)


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Sent to {msg.topic()} partition={msg.partition()} offset={msg.offset()}")


def fetch_prices():
    response = requests.get(URL, timeout=10)
    response.raise_for_status()
    return response.json()


while True:
    try:
        prices = fetch_prices()

        for coin_id, values in prices.items():
            event = {
                "coin_id": coin_id,
                "currency": VS_CURRENCY,
                "price": values.get(VS_CURRENCY),
                "change_24h": values.get(f"{VS_CURRENCY}_24h_change"),
                "last_updated_at": values.get("last_updated_at"),
                "streamed_at": datetime.now(timezone.utc).isoformat()
            }

            producer.produce(
                TOPIC_NAME,
                json.dumps(event).encode("utf-8"),
                callback=delivery_report
            )

            print(json.dumps(event, indent=2))

        producer.poll(1)
        producer.flush()

        print("-" * 60)
        time.sleep(10)

    except Exception as error:
        print(f"Error fetching or producing prices: {error}")
        time.sleep(30)