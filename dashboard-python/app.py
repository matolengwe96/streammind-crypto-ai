import requests
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

API_BASE_URL = "http://localhost:3000"

st.set_page_config(
    page_title="StreamMind Crypto AI",
    layout="wide"
)

st_autorefresh(
    interval=5000,
    key="crypto_dashboard_refresh"
)

st.title("StreamMind Crypto AI")
st.subheader("Real-Time Crypto Event Streaming Platform")

try:
    analytics_response = requests.get(f"{API_BASE_URL}/crypto/analytics")
    latest_response = requests.get(f"{API_BASE_URL}/crypto/latest")

    analytics_response.raise_for_status()
    latest_response.raise_for_status()

    analytics = analytics_response.json()
    latest_events = latest_response.json()

    col1, col2, col3 = st.columns(3)

    col1.metric("Total Events", analytics["totalEvents"])
    col2.metric("Tracked Coins", analytics["trackedCoins"])

    biggest_gainer = analytics.get("biggestGainer")
    biggest_loser = analytics.get("biggestLoser")

    if biggest_gainer:
        col3.metric(
            "Biggest Gainer",
            f"{biggest_gainer['coin_id'].upper()} "
            f"({float(biggest_gainer['change_24h']):.2f}%)"
        )
    else:
        col3.metric("Biggest Gainer", "Waiting...")

    st.divider()

    st.subheader("Latest Coin Prices")

    latest_by_coin = analytics.get("latestByCoin", {})
    latest_prices = []

    for coin_name, coin_data in latest_by_coin.items():
        latest_prices.append({
            "Coin": coin_name.upper(),
            "Price (USD)": coin_data.get("price"),
            "24h Change %": round(float(coin_data.get("change_24h", 0)), 2),
            "Streamed At": coin_data.get("streamed_at"),
            "Kafka Partition": coin_data.get("partition"),
            "Kafka Offset": coin_data.get("offset"),
        })

    latest_prices_df = pd.DataFrame(latest_prices)

    if not latest_prices_df.empty:
        st.dataframe(latest_prices_df, use_container_width=True)
    else:
        st.info("No latest prices available yet.")

    st.divider()

    st.subheader("Crypto Price Comparison")

    if not latest_prices_df.empty:
        chart_df = latest_prices_df[["Coin", "Price (USD)"]]
        st.bar_chart(chart_df, x="Coin", y="Price (USD)")
    else:
        st.info("Waiting for price data.")

    st.divider()

    st.subheader("Market Movers")

    col4, col5 = st.columns(2)

    if biggest_gainer:
        col4.success(
            f"Top Gainer: {biggest_gainer['coin_id'].upper()} "
            f"({float(biggest_gainer['change_24h']):.2f}%)"
        )
    else:
        col4.info("Waiting for gainer data.")

    if biggest_loser:
        col5.error(
            f"Top Loser: {biggest_loser['coin_id'].upper()} "
            f"({float(biggest_loser['change_24h']):.2f}%)"
        )
    else:
        col5.info("Waiting for loser data.")

    st.divider()

    st.subheader("Latest Kafka Crypto Events")

    if latest_events:
        latest_events_df = pd.DataFrame(latest_events)
        st.dataframe(latest_events_df, use_container_width=True)
    else:
        st.info("No crypto events received yet.")

except requests.exceptions.ConnectionError:
    st.error(
        "Could not connect to Node.js API. "
        "Make sure it is running at http://localhost:3000"
    )

except Exception as error:
    st.error(f"Something went wrong: {error}")