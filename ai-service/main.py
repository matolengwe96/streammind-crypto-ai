import os
import json
import asyncio
from datetime import datetime
from typing import Dict, Any, List, Optional

import psycopg2
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from openai import OpenAI
from pydantic import BaseModel


# =========================================================
# ENVIRONMENT SETUP
# =========================================================

load_dotenv()


# =========================================================
# FASTAPI APP
# =========================================================

app = FastAPI(
    title="StreamMind Crypto AI",
    version="2.2.0",
    description="Real-time AI crypto analytics, predictions, alerts, and portfolio intelligence.",
)


# =========================================================
# CORS FOR NEXT.JS FRONTEND
# =========================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================================================
# OPENAI CLIENT
# =========================================================

client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY")
)


# =========================================================
# REQUEST MODELS
# =========================================================

class ChatRequest(BaseModel):
    question: str


class PortfolioHolding(BaseModel):
    coin_id: str
    quantity: float


class PortfolioRiskRequest(BaseModel):
    holdings: List[PortfolioHolding]


# =========================================================
# POSTGRES CONNECTION
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
# FETCH LATEST MARKET DATA
# =========================================================

def fetch_latest_crypto_data(limit: int = 50):
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
        (limit,),
    )

    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    cursor.close()
    conn.close()

    return [dict(zip(columns, row)) for row in rows]


# =========================================================
# TRADE SIGNAL ENGINE
# =========================================================

def generate_trade_signal(change: float):
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
            "reason": "Negative momentum is accelerating.",
        }

    if -2 < change < 1:
        return {
            "signal": "HOLD",
            "risk": "Moderate",
            "confidence": 70,
            "reason": "Market is consolidating with no clear breakout.",
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
        "reason": "Positive momentum is building.",
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
            "averageChange24h": 0,
            "tradeSignals": {},
            "latestByCoin": {},
            "trackedCoins": 0,
            "totalEvents": 0,
            "biggestGainer": None,
            "biggestLoser": None,
        }

    average_change = sum(float(c["change_24h"]) for c in coins) / len(coins)

    positive = len([c for c in coins if float(c["change_24h"]) > 0])
    negative = len([c for c in coins if float(c["change_24h"]) < 0])

    biggest_gainer = max(coins, key=lambda c: float(c["change_24h"]))
    biggest_loser = min(coins, key=lambda c: float(c["change_24h"]))

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
        change = float(coin["change_24h"])

        trade_signals[coin["coin_id"]] = {
            "coin_id": coin["coin_id"],
            "price": float(coin["price"]),
            "change_24h": change,
            **generate_trade_signal(change),
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
        "biggestGainer": biggest_gainer,
        "biggestLoser": biggest_loser,
    }


# =========================================================
# PREDICTION ENGINE
# =========================================================

def build_predictions(analytics):
    predictions = {}

    for coin_id, coin in analytics["tradeSignals"].items():
        change = float(coin["change_24h"])

        if change >= 3:
            direction = "Strong Uptrend"
            predicted_change = round(change * 0.45, 2)
            confidence = 86
            risk = "Medium"
        elif change >= 1:
            direction = "Mild Uptrend"
            predicted_change = round(change * 0.35, 2)
            confidence = 74
            risk = "Low-Medium"
        elif change <= -3:
            direction = "Strong Downtrend"
            predicted_change = round(change * 0.45, 2)
            confidence = 86
            risk = "High"
        elif change <= -1:
            direction = "Mild Downtrend"
            predicted_change = round(change * 0.35, 2)
            confidence = 74
            risk = "Medium"
        else:
            direction = "Sideways"
            predicted_change = round(change * 0.2, 2)
            confidence = 62
            risk = "Moderate"

        predictions[coin_id] = {
            "coin_id": coin_id,
            "current_price": coin["price"],
            "change_24h": change,
            "forecast_direction": direction,
            "predicted_1h_change": predicted_change,
            "confidence": confidence,
            "risk": risk,
            "reason": f"{coin_id} is classified as {direction} based on recent 24h momentum.",
            "disclaimer": "Educational demo prediction only. Not financial advice.",
        }

    return predictions


# =========================================================
# NEWS SENTIMENT MOCK ENGINE
# =========================================================

def build_news_sentiment(analytics):
    sentiment = analytics["sentiment"]
    risk = analytics["riskLevel"]

    if "Bullish" in sentiment:
        news = [
            {
                "headline": "Institutional interest in digital assets remains strong",
                "impact": "Bullish",
                "confidence": 82,
            },
            {
                "headline": "Crypto market momentum improves as major assets recover",
                "impact": "Bullish",
                "confidence": 78,
            },
            {
                "headline": "Traders monitor volatility before next macro data release",
                "impact": "Neutral",
                "confidence": 65,
            },
        ]
    elif "Bearish" in sentiment:
        news = [
            {
                "headline": "Risk assets weaken as crypto volatility increases",
                "impact": "Bearish",
                "confidence": 84,
            },
            {
                "headline": "Altcoin momentum slows as traders reduce exposure",
                "impact": "Bearish",
                "confidence": 76,
            },
            {
                "headline": "Market awaits stronger confirmation before risk-on flows return",
                "impact": "Neutral",
                "confidence": 66,
            },
        ]
    else:
        news = [
            {
                "headline": "Crypto market consolidates with mixed signals",
                "impact": "Neutral",
                "confidence": 70,
            },
            {
                "headline": "Bitcoin and altcoins trade cautiously amid uncertain momentum",
                "impact": "Neutral",
                "confidence": 68,
            },
            {
                "headline": "Traders wait for breakout confirmation",
                "impact": "Neutral",
                "confidence": 64,
            },
        ]

    return {
        "marketSentiment": sentiment,
        "riskLevel": risk,
        "news": news,
        "note": "Demo news sentiment generated from live market conditions.",
    }


# =========================================================
# PORTFOLIO RISK ENGINE
# =========================================================

def analyze_portfolio_risk(holdings, analytics):
    latest_by_coin = analytics["latestByCoin"]

    rows = []
    total_value = 0

    for holding in holdings:
        coin = latest_by_coin.get(holding.coin_id)

        price = float(coin["price"]) if coin else 0
        value = holding.quantity * price
        change_24h = float(coin["change_24h"]) if coin else 0

        total_value += value

        rows.append({
            "coin_id": holding.coin_id,
            "quantity": holding.quantity,
            "price": price,
            "value": value,
            "change_24h": change_24h,
        })

    biggest_exposure = None

    if rows:
        biggest_exposure = max(rows, key=lambda row: row["value"])

    if analytics["riskLevel"] in ["Very High", "High"]:
        portfolio_risk = "High"
        explanation = "Market-wide risk is elevated, so portfolio exposure should be monitored carefully."
    elif analytics["sentiment"].lower().find("bearish") >= 0:
        portfolio_risk = "Medium"
        explanation = "Market sentiment is bearish, which raises short-term portfolio risk."
    else:
        portfolio_risk = "Low-Medium"
        explanation = "Portfolio risk appears controlled under current market conditions."

    return {
        "totalValue": round(total_value, 2),
        "holdings": rows,
        "biggestExposure": biggest_exposure,
        "portfolioRisk": portfolio_risk,
        "explanation": explanation,
        "marketSentiment": analytics["sentiment"],
        "marketRiskLevel": analytics["riskLevel"],
        "disclaimer": "Educational portfolio analysis only. Not financial advice.",
    }


# =========================================================
# ROOT + HEALTH
# =========================================================

@app.get("/")
def root():
    return {
        "message": "StreamMind Crypto AI running",
        "version": "2.2.0",
        "time": str(datetime.utcnow()),
        "endpoints": [
            "/health",
            "/ai/raw-analytics",
            "/ai/market-summary",
            "/ai/trade-signals",
            "/ai/predictions",
            "/ai/news-sentiment",
            "/ai/portfolio-risk",
            "/ai/chat",
            "/ws/market",
        ],
    }


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "service": "streammind-ai",
        "time": str(datetime.utcnow()),
    }


# =========================================================
# ANALYTICS ENDPOINTS
# =========================================================

@app.get("/ai/raw-analytics")
def raw_analytics():
    data = fetch_latest_crypto_data()
    analytics = build_analytics(data)

    return {
        **analytics,
        "latestDatabaseEvents": data,
    }


@app.get("/ai/trade-signals")
def trade_signals():
    data = fetch_latest_crypto_data()
    analytics = build_analytics(data)

    return analytics


@app.get("/ai/predictions")
def ai_predictions():
    data = fetch_latest_crypto_data()
    analytics = build_analytics(data)
    predictions = build_predictions(analytics)

    return {
        "sentiment": analytics["sentiment"],
        "riskLevel": analytics["riskLevel"],
        "predictions": predictions,
        "disclaimer": "Predictions are generated from simple momentum logic for demo purposes only. Not financial advice.",
    }


@app.get("/ai/news-sentiment")
def news_sentiment():
    data = fetch_latest_crypto_data()
    analytics = build_analytics(data)

    return build_news_sentiment(analytics)


@app.post("/ai/portfolio-risk")
def portfolio_risk(request: PortfolioRiskRequest):
    data = fetch_latest_crypto_data()
    analytics = build_analytics(data)

    return analyze_portfolio_risk(request.holdings, analytics)


# =========================================================
# AI MARKET SUMMARY
# =========================================================

@app.get("/ai/market-summary")
def market_summary():
    data = fetch_latest_crypto_data()
    analytics = build_analytics(data)
    predictions = build_predictions(analytics)

    prompt = f"""
You are StreamMind AI, a real-time crypto intelligence analyst.

Analyze the live crypto market.

Market Sentiment:
{analytics["sentiment"]}

Risk Level:
{analytics["riskLevel"]}

Average 24h Change:
{analytics["averageChange24h"]}

Trade Signals:
{json.dumps(analytics["tradeSignals"])}

Predictions:
{json.dumps(predictions)}

Provide:
1. Market outlook
2. Strongest asset
3. Weakest asset
4. Risk analysis
5. Momentum interpretation
6. Prediction interpretation

Rules:
- Keep concise
- Do not provide financial advice
- Use professional analyst language
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a professional crypto market analyst. Never give financial advice.",
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
            "analytics": analytics,
            "predictions": predictions,
        }

    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=f"Market summary failed: {error}",
        )


# =========================================================
# AI CHAT
# =========================================================

@app.post("/ai/chat")
def ai_chat(request: ChatRequest):
    data = fetch_latest_crypto_data()
    analytics = build_analytics(data)
    predictions = build_predictions(analytics)
    news = build_news_sentiment(analytics)

    system_prompt = f"""
You are StreamMind AI, a live crypto intelligence assistant.

Current Market Sentiment:
{analytics["sentiment"]}

Risk Level:
{analytics["riskLevel"]}

Trade Signals:
{json.dumps(analytics["tradeSignals"])}

Predictions:
{json.dumps(predictions)}

News Sentiment:
{json.dumps(news)}

Rules:
- Never provide financial advice
- Use live market data
- Be concise but useful
- Explain reasoning clearly
- Mention risks
- Mention strongest and weakest assets when relevant
- If asked for buy/sell advice, frame it as educational signal interpretation only
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
                },
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
            detail=f"AI chat failed: {error}",
        )


# =========================================================
# WEBSOCKET MARKET STREAM
# =========================================================

@app.websocket("/ws/market")
async def websocket_market(websocket: WebSocket):
    await websocket.accept()

    try:
        while True:
            data = fetch_latest_crypto_data()
            analytics = build_analytics(data)
            predictions = build_predictions(analytics)

            payload = {
                "type": "market_update",
                "timestamp": str(datetime.utcnow()),
                "analytics": analytics,
                "predictions": predictions,
                "latestDatabaseEvents": data,
            }

            await websocket.send_text(json.dumps(payload, default=str))
            await asyncio.sleep(2)

    except WebSocketDisconnect:
        print("WebSocket disconnected")

    except Exception as error:
        print("WebSocket error:", error)

        try:
            await websocket.close()
        except Exception:
            pass