import json
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

from config.kafka_config import BOOTSTRAP_SERVERS


# =========================================================
# Kafka settings
# =========================================================

TOPIC_NAME = "crypto_prices"

producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS
})


# =========================================================
# CoinGecko settings
# =========================================================

COINS = [
    "bitcoin",
    "ethereum",
    "solana",
    "cardano",
]

VS_CURRENCY = "usd"

COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"


# =========================================================
# Kafka delivery callback
# =========================================================

def delivery_report(err, msg):
    if err:
        print(f"Kafka delivery failed: {err}")
    else:
        print(
            f"Sent to {msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


# =========================================================
# Fetch live crypto prices from CoinGecko
# =========================================================

def fetch_prices():
    params = {
        "ids": ",".join(COINS),
        "vs_currencies": VS_CURRENCY,
        "include_24hr_change": "true",
        "include_last_updated_at": "true",
    }

    response = requests.get(
        COINGECKO_URL,
        params=params,
        timeout=15,
        headers={
            "accept": "application/json",
            "user-agent": "streammind-crypto-ai/1.0",
        },
    )

    response.raise_for_status()

    return response.json()


# =========================================================
# Build one Kafka event per coin
# =========================================================

def build_event(coin_id, values):
    price = values.get(VS_CURRENCY)
    change_24h = values.get(f"{VS_CURRENCY}_24h_change")
    last_updated_at = values.get("last_updated_at")

    return {
        "coin_id": coin_id,
        "currency": VS_CURRENCY,
        "price": float(price) if price is not None else None,
        "change_24h": float(change_24h) if change_24h is not None else 0.0,
        "last_updated_at": last_updated_at,
        "streamed_at": datetime.now(timezone.utc).isoformat(),
        "source": "coingecko",
    }


# =========================================================
# Main streaming loop
# =========================================================

def main():
    print("Starting StreamMind real-time CoinGecko producer...")
    print(f"Kafka broker: {BOOTSTRAP_SERVERS}")
    print(f"Topic: {TOPIC_NAME}")
    print(f"Coins: {', '.join(COINS)}")
    print("-" * 60)

    while True:
        try:
            prices = fetch_prices()

            for coin_id in COINS:
                values = prices.get(coin_id)

                if not values:
                    print(f"No data returned for {coin_id}")
                    continue

                event = build_event(coin_id, values)

                producer.produce(
                    TOPIC_NAME,
                    key=coin_id.encode("utf-8"),
                    value=json.dumps(event).encode("utf-8"),
                    callback=delivery_report,
                )

                print(json.dumps(event, indent=2))

            producer.poll(1)
            producer.flush()

            print("-" * 60)
            time.sleep(10)

        except requests.exceptions.HTTPError as error:
            print(f"CoinGecko HTTP error: {error}")
            time.sleep(60)

        except requests.exceptions.RequestException as error:
            print(f"CoinGecko request error: {error}")
            time.sleep(30)

        except KeyboardInterrupt:
            print("Producer stopped by user.")
            break

        except Exception as error:
            print(f"Unexpected producer error: {error}")
            time.sleep(30)


if __name__ == "__main__":
    main()