import os
import json
import asyncio
from datetime import datetime
from typing import List, Dict, Any

import psycopg2
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from openai import OpenAI
from pydantic import BaseModel


# =========================================================
# ENV
# =========================================================

load_dotenv()


# =========================================================
# FASTAPI
# =========================================================

app = FastAPI(
    title="StreamMind Crypto AI",
    version="2.0.0",
    description="Real-time AI crypto analytics platform",
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
# OPENAI
# =========================================================

client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY")
)


# =========================================================
# MODELS
# =========================================================

class ChatRequest(BaseModel):
    question: str


# =========================================================
# DATABASE
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
# FETCH DATA
# =========================================================

def fetch_latest_crypto_data(limit=50):

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
            kafka_partition,
            kafka_offset,
            streamed_at
        FROM crypto_prices
        ORDER BY id DESC
        LIMIT %s;
        """,
        (limit,)
    )

    rows = cursor.fetchall()

    columns = [desc[0] for desc in cursor.description]

    cursor.close()
    conn.close()

    return [
        dict(zip(columns, row))
        for row in rows
    ]


# =========================================================
# SIGNAL ENGINE
# =========================================================

def generate_trade_signal(change):

    if change <= -5:
        return {
            "signal": "STRONG SELL",
            "risk": "Very High",
            "confidence": 92,
            "reason": "Heavy downside pressure detected.",
        }

    if change <= -2:
        return {
            "signal": "SELL",
            "risk": "High",
            "confidence": 84,
            "reason": "Negative momentum accelerating.",
        }

    if -2 < change < 1:
        return {
            "signal": "HOLD",
            "risk": "Moderate",
            "confidence": 70,
            "reason": "Market consolidating.",
        }

    if change >= 4:
        return {
            "signal": "STRONG BUY",
            "risk": "Medium",
            "confidence": 90,
            "reason": "Strong bullish momentum detected.",
        }

    return {
        "signal": "BUY",
        "risk": "Low-Medium",
        "confidence": 78,
        "reason": "Positive momentum building.",
    }


# =========================================================
# ANALYTICS ENGINE
# =========================================================

def build_analytics(data):

    latest_by_coin = {}

    for event in data:

        coin = event["coin_id"]

        if coin not in latest_by_coin:
            latest_by_coin[coin] = event

    coins = list(latest_by_coin.values())

    if not coins:

        return {
            "sentiment": "Unknown",
            "riskLevel": "Unknown",
            "confidence": 0,
            "tradeSignals": {},
        }

    average_change = (
        sum(float(c["change_24h"]) for c in coins)
        / len(coins)
    )

    positive = len([
        c for c in coins
        if float(c["change_24h"]) > 0
    ])

    negative = len([
        c for c in coins
        if float(c["change_24h"]) < 0
    ])

    if positive == len(coins):
        sentiment = "Bullish"
        confidence = 90

    elif negative == len(coins):
        sentiment = "Bearish"
        confidence = 90

    elif positive > negative:
        sentiment = "Cautiously Bullish"
        confidence = 76

    elif negative > positive:
        sentiment = "Cautiously Bearish"
        confidence = 76

    else:
        sentiment = "Neutral"
        confidence = 60

    if average_change <= -4:
        risk = "Very High"

    elif average_change <= -2:
        risk = "High"

    elif average_change < 1:
        risk = "Medium"

    else:
        risk = "Low-Medium"

    trade_signals = {}

    for coin in coins:

        trade_signals[coin["coin_id"]] = {
            "coin_id": coin["coin_id"],
            "price": float(coin["price"]),
            "change_24h": float(coin["change_24h"]),
            **generate_trade_signal(
                float(coin["change_24h"])
            ),
        }

    return {
        "sentiment": sentiment,
        "riskLevel": risk,
        "confidence": confidence,
        "averageChange24h": round(average_change, 2),
        "tradeSignals": trade_signals,
        "latestByCoin": latest_by_coin,
        "trackedCoins": len(coins),
        "totalEvents": len(data),
    }


# =========================================================
# ROOT
# =========================================================

@app.get("/")
def root():

    return {
        "message": "StreamMind Crypto AI running",
        "time": str(datetime.utcnow()),
    }


# =========================================================
# HEALTH
# =========================================================

@app.get("/health")
def health():

    return {
        "status": "healthy",
        "service": "streammind-ai",
    }


# =========================================================
# RAW ANALYTICS
# =========================================================

@app.get("/ai/raw-analytics")
def raw_analytics():

    data = fetch_latest_crypto_data()
    analytics = build_analytics(data)

    return {
        **analytics,
        "latestDatabaseEvents": data,
    }


# =========================================================
# TRADE SIGNALS
# =========================================================

@app.get("/ai/trade-signals")
def trade_signals():

    data = fetch_latest_crypto_data()
    analytics = build_analytics(data)

    return analytics


# =========================================================
# AI MARKET SUMMARY
# =========================================================

@app.get("/ai/market-summary")
def market_summary():

    data = fetch_latest_crypto_data()
    analytics = build_analytics(data)

    prompt = f"""
You are StreamMind AI.

Analyze this live crypto market.

Market Sentiment:
{analytics["sentiment"]}

Risk:
{analytics["riskLevel"]}

Trade Signals:
{json.dumps(analytics["tradeSignals"])}

Provide:
1. Market outlook
2. Strongest asset
3. Weakest asset
4. Risk analysis
5. Trading interpretation
6. Momentum discussion

Keep concise and professional.
"""

    try:

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": """
You are a professional crypto market analyst.
Never give financial advice.
Use concise institutional-style analysis.
"""
                },
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            temperature=0.4,
        )

        return {
            "summary": response.choices[0].message.content,
            "analytics": analytics,
        }

    except Exception as error:

        raise HTTPException(
            status_code=500,
            detail=str(error)
        )


# =========================================================
# AI CHAT
# =========================================================

@app.post("/ai/chat")
def ai_chat(request: ChatRequest):

    data = fetch_latest_crypto_data()
    analytics = build_analytics(data)

    system_prompt = f"""
You are StreamMind AI.

You are a live crypto intelligence assistant.

Current Market Sentiment:
{analytics["sentiment"]}

Risk Level:
{analytics["riskLevel"]}

Trade Signals:
{json.dumps(analytics["tradeSignals"])}

Rules:
- Never provide financial advice
- Keep answers concise
- Explain reasoning clearly
- Use live market data
- Mention strongest and weakest assets when relevant
"""

    try:

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": system_prompt,
                },
                {
                    "role": "user",
                    "content": request.question,
                }
            ],
            temperature=0.5,
        )

        return {
            "question": request.question,
            "answer": response.choices[0].message.content,
            "timestamp": str(datetime.utcnow()),
        }

    except Exception as error:

        raise HTTPException(
            status_code=500,
            detail=f"AI chat failed: {error}"
        )


# =========================================================
# WEBSOCKET
# =========================================================

@app.websocket("/ws/market")
async def websocket_market(websocket: WebSocket):

    await websocket.accept()

    try:

        while True:

            data = fetch_latest_crypto_data()
            analytics = build_analytics(data)

            payload = {
                "type": "market_update",
                "timestamp": str(datetime.utcnow()),
                "analytics": analytics,
                "latestDatabaseEvents": data,
            }

            await websocket.send_text(
                json.dumps(payload, default=str)
            )

            await asyncio.sleep(2)

    except WebSocketDisconnect:

        print("WebSocket disconnected")

    except Exception as error:

        print("WebSocket error:", error)

        try:
            await websocket.close()

        except:
            pass