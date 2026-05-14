import os
import json
import asyncio
from typing import List, Dict, Any

import psycopg2
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
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
    description="AI assistant, trade signals, and WebSocket service for crypto streaming analytics",
    version="1.2.0",
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

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


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
# Fetch latest crypto data from PostgreSQL
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
# Trade signal logic
# =========================================================

def generate_trade_signal(change_24h: float) -> Dict[str, Any]:
    """
    Rule-based signal engine.
    This is NOT financial advice. It is a demo analytics signal.
    """

    if change_24h <= -5:
        return {
            "signal": "AVOID",
            "risk": "Very High",
            "confidence": 90,
            "reason": "Sharp negative 24h movement indicates heavy selling pressure.",
        }

    if change_24h <= -3:
        return {
            "signal": "SELL / AVOID",
            "risk": "High",
            "confidence": 84,
            "reason": "Strong downside movement suggests elevated short-term risk.",
        }

    if change_24h <= -1:
        return {
            "signal": "WATCH",
            "risk": "Medium",
            "confidence": 72,
            "reason": "Asset is declining, but not in extreme territory.",
        }

    if -1 < change_24h < 1:
        return {
            "signal": "HOLD",
            "risk": "Moderate",
            "confidence": 68,
            "reason": "Price movement is relatively stable.",
        }

    if change_24h >= 3:
        return {
            "signal": "MOMENTUM BUY",
            "risk": "Medium",
            "confidence": 78,
            "reason": "Strong positive movement indicates bullish momentum.",
        }

    return {
        "signal": "HOLD / WATCH",
        "risk": "Low-Medium",
        "confidence": 70,
        "reason": "Positive movement exists, but trend confirmation is limited.",
    }


# =========================================================
# Build analytics from latest events
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
            "riskLevel": "Unknown",
            "confidence": 0,
            "tradeSignals": {},
        }

    biggest_gainer = max(coins, key=lambda coin: float(coin["change_24h"]))
    biggest_loser = min(coins, key=lambda coin: float(coin["change_24h"]))

    negative_count = sum(1 for coin in coins if float(coin["change_24h"]) < 0)
    positive_count = sum(1 for coin in coins if float(coin["change_24h"]) > 0)

    average_change = sum(float(coin["change_24h"]) for coin in coins) / len(coins)

    if negative_count == len(coins):
        sentiment = "Bearish"
        confidence = 88
    elif positive_count == len(coins):
        sentiment = "Bullish"
        confidence = 88
    elif negative_count > positive_count:
        sentiment = "Cautiously Bearish"
        confidence = 74
    elif positive_count > negative_count:
        sentiment = "Cautiously Bullish"
        confidence = 74
    else:
        sentiment = "Neutral"
        confidence = 60

    if average_change <= -4:
        risk_level = "Very High"
    elif average_change <= -2:
        risk_level = "High"
    elif average_change < 0:
        risk_level = "Medium"
    elif average_change < 2:
        risk_level = "Low-Medium"
    else:
        risk_level = "Medium"

    trade_signals = {}

    for coin in coins:
        coin_id = coin["coin_id"]
        change_24h = float(coin["change_24h"])

        trade_signals[coin_id] = {
            "coin_id": coin_id,
            "price": float(coin["price"]),
            "change_24h": change_24h,
            **generate_trade_signal(change_24h),
        }

    return {
        "totalEvents": len(data),
        "trackedCoins": len(coins),
        "latestByCoin": latest_by_coin,
        "biggestGainer": biggest_gainer,
        "biggestLoser": biggest_loser,
        "sentiment": sentiment,
        "riskLevel": risk_level,
        "confidence": confidence,
        "averageChange24h": round(average_change, 2),
        "tradeSignals": trade_signals,
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
            "/ai/trade-signals",
            "/ai/chat",
            "/ws/market",
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
# Trade signals endpoint
# =========================================================

@app.get("/ai/trade-signals")
def trade_signals():
    data = fetch_latest_crypto_data()
    analytics = build_local_analytics(data)

    return {
        "sentiment": analytics["sentiment"],
        "riskLevel": analytics["riskLevel"],
        "confidence": analytics["confidence"],
        "averageChange24h": analytics["averageChange24h"],
        "tradeSignals": analytics["tradeSignals"],
        "disclaimer": "Signals are for educational demo purposes only and are not financial advice.",
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
2. Risk level
3. Strongest asset
4. Weakest asset
5. Trade signal overview
6. Volatility observations
7. Short-term trader guidance
8. Why real-time streaming matters

Important:
- Do not provide financial advice.
- Be concise.
- Be investor-ready.

System Sentiment:
{analytics["sentiment"]}

Risk Level:
{analytics["riskLevel"]}

Confidence:
{analytics["confidence"]}%

Trade Signals:
{json.dumps(analytics["tradeSignals"], default=str)}

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
            "riskLevel": analytics["riskLevel"],
            "confidence": analytics["confidence"],
            "tradeSignals": analytics["tradeSignals"],
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

Risk Level:
{analytics["riskLevel"]}

AI Confidence:
{analytics["confidence"]}%

Trade Signals:
{json.dumps(analytics["tradeSignals"], default=str)}

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
            "riskLevel": analytics["riskLevel"],
            "confidence": analytics["confidence"],
            "tradeSignals": analytics["tradeSignals"],
        }

    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=f"OpenAI chat request failed: {error}",
        )


# =========================================================
# WebSocket endpoint
# =========================================================

@app.websocket("/ws/market")
async def websocket_market(websocket: WebSocket):
    await websocket.accept()

    try:
        while True:
            data = fetch_latest_crypto_data(limit=50)
            analytics = build_local_analytics(data)

            payload = {
                "type": "market_update",
                "analytics": {
                    **analytics,
                    "latestDatabaseEvents": data,
                },
            }

            await websocket.send_text(json.dumps(payload, default=str))

            await asyncio.sleep(2)

    except WebSocketDisconnect:
        print("WebSocket client disconnected")

    except Exception as error:
        print("WebSocket error:", error)

        try:
            await websocket.close()
        except Exception:
            pass