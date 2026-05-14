import os
import json
from typing import List, Dict, Any

import psycopg2
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from openai import OpenAI
from pydantic import BaseModel


# =========================================================
# Load environment variables
# =========================================================

load_dotenv()


# =========================================================
# FastAPI app
# =========================================================

app = FastAPI(
    title="StreamMind Crypto AI Service",
    description="AI assistant for real-time crypto streaming analytics",
    version="1.0.0",
)


# =========================================================
# CORS
# =========================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================================================
# OpenAI client
# =========================================================

client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY")
)


# =========================================================
# Request models
# =========================================================

class ChatRequest(BaseModel):
    question: str


# =========================================================
# PostgreSQL connection
# =========================================================

def get_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
    )


# =========================================================
# Fetch latest crypto data
# =========================================================

def fetch_latest_crypto_data(limit: int = 50) -> List[Dict[str, Any]]:
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            id,
            coin_id,
            currency,
            price,
            change_24h,
            last_updated_at,
            streamed_at,
            kafka_topic,
            kafka_partition,
            kafka_offset,
            created_at
        FROM crypto_prices
        ORDER BY id DESC
        LIMIT %s;
        """,
        (limit,),
    )

    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    cursor.close()
    conn.close()

    return [dict(zip(columns, row)) for row in rows]


# =========================================================
# Build local analytics
# =========================================================

def build_local_analytics(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    latest_by_coin = {}

    for event in data:
        coin = event["coin_id"]

        if coin not in latest_by_coin:
            latest_by_coin[coin] = event

    coins = list(latest_by_coin.values())

    if not coins:
        return {
            "totalEvents": 0,
            "trackedCoins": 0,
            "latestByCoin": {},
            "biggestGainer": None,
            "biggestLoser": None,
            "sentiment": "Neutral",
            "confidence": 0,
        }

    biggest_gainer = max(
        coins,
        key=lambda coin: float(coin["change_24h"])
    )

    biggest_loser = min(
        coins,
        key=lambda coin: float(coin["change_24h"])
    )

    negative_count = sum(
        1 for coin in coins
        if float(coin["change_24h"]) < 0
    )

    positive_count = sum(
        1 for coin in coins
        if float(coin["change_24h"]) > 0
    )

    if negative_count == len(coins):
        sentiment = "Bearish"
        confidence = 88
    elif positive_count == len(coins):
        sentiment = "Bullish"
        confidence = 88
    elif negative_count > positive_count:
        sentiment = "Cautiously Bearish"
        confidence = 72
    elif positive_count > negative_count:
        sentiment = "Cautiously Bullish"
        confidence = 72
    else:
        sentiment = "Neutral"
        confidence = 60

    return {
        "totalEvents": len(data),
        "trackedCoins": len(coins),
        "latestByCoin": latest_by_coin,
        "biggestGainer": biggest_gainer,
        "biggestLoser": biggest_loser,
        "sentiment": sentiment,
        "confidence": confidence,
    }


# =========================================================
# Root endpoint
# =========================================================

@app.get("/")
def root():
    return {
        "message": "StreamMind Crypto AI Service is running",
        "endpoints": [
            "/ai/raw-analytics",
            "/ai/market-summary",
            "/ai/chat",
        ],
    }


# =========================================================
# Raw analytics endpoint
# =========================================================

@app.get("/ai/raw-analytics")
def raw_analytics():
    data = fetch_latest_crypto_data()
    analytics = build_local_analytics(data)

    return {
        **analytics,
        "latestDatabaseEvents": data,
    }


# =========================================================
# AI market summary endpoint
# =========================================================

@app.get("/ai/market-summary")
def market_summary():
    data = fetch_latest_crypto_data()
    analytics = build_local_analytics(data)

    prompt = f"""
You are StreamMind AI, an institutional-grade crypto intelligence platform.

Analyze the latest real-time Kafka streaming crypto data.

Return a professional report with:

1. Market sentiment
2. Strongest asset
3. Weakest asset
4. Volatility observations
5. Risk observations
6. Short-term trader guidance
7. Why real-time streaming matters here

Important:
- Do not provide financial advice.
- Be concise.
- Be investor-ready.

System Sentiment:
{analytics["sentiment"]}

Confidence:
{analytics["confidence"]}%

Latest Market Data:
{json.dumps(data, default=str)}
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a professional crypto market analyst. "
                        "You explain market data clearly and never provide financial advice."
                    ),
                },
                {
                    "role": "user",
                    "content": prompt,
                },
            ],
            temperature=0.4,
        )

        return {
            "summary": response.choices[0].message.content,
            "sentiment": analytics["sentiment"],
            "confidence": analytics["confidence"],
            "source": "openai",
            "model": "gpt-4o-mini",
        }

    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=f"OpenAI request failed: {error}",
        )


# =========================================================
# AI chat endpoint
# =========================================================

@app.post("/ai/chat")
def chat_with_market_data(request: ChatRequest):
    data = fetch_latest_crypto_data()
    analytics = build_local_analytics(data)

    prompt = f"""
You are StreamMind AI, a real-time crypto streaming analytics copilot.

Answer the user's question using ONLY the live market data below.

Do not provide financial advice.
Be concise, practical, and professional.

User Question:
{request.question}

Market Sentiment:
{analytics["sentiment"]}

AI Confidence:
{analytics["confidence"]}%

Biggest Gainer:
{analytics["biggestGainer"]}

Biggest Loser:
{analytics["biggestLoser"]}

Latest Market Data:
{json.dumps(data, default=str)}
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are StreamMind AI, a professional crypto analytics assistant. "
                        "Use streaming data only. Do not provide financial advice."
                    ),
                },
                {
                    "role": "user",
                    "content": prompt,
                },
            ],
            temperature=0.4,
        )

        return {
            "question": request.question,
            "answer": response.choices[0].message.content,
            "sentiment": analytics["sentiment"],
            "confidence": analytics["confidence"],
        }

    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=f"OpenAI chat request failed: {error}",
        )