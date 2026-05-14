/**
 * =========================================================
 * StreamMind Crypto AI
 * Kafka Consumer + PostgreSQL Persistence + Analytics API
 * =========================================================
 */

require("dotenv").config();

const express = require("express");
const { Kafka } = require("kafkajs");
const { Pool } = require("pg");

const app = express();

/**
 * =========================================================
 * PostgreSQL Connection Pool
 * =========================================================
 */

const pool = new Pool({
    host: process.env.PG_HOST,
    port: process.env.PG_PORT,
    database: process.env.PG_DATABASE,
    user: process.env.PG_USER,
    password: process.env.PG_PASSWORD,
});

/**
 * =========================================================
 * Kafka Configuration
 * =========================================================
 */

const kafka = new Kafka({
    clientId: "streammind-crypto-api",
    brokers: [process.env.KAFKA_BROKER],
});

const consumer = kafka.consumer({
    groupId: "streammind-crypto-api-group",
});

/**
 * =========================================================
 * In-Memory Cache
 * =========================================================
 */

let latestEvents = [];

/**
 * =========================================================
 * Save Event To PostgreSQL
 * =========================================================
 */

async function saveCryptoEvent(eventData) {
    try {

        const query = `
            INSERT INTO crypto_prices (
                coin_id,
                currency,
                price,
                change_24h,
                last_updated_at,
                streamed_at,
                kafka_topic,
                kafka_partition,
                kafka_offset
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        `;

        const values = [
            eventData.coin_id,
            eventData.currency,
            eventData.price,
            eventData.change_24h,
            eventData.last_updated_at,
            eventData.streamed_at,
            eventData.topic,
            eventData.partition,
            eventData.offset,
        ];

        await pool.query(query, values);

        console.log(`Saved ${eventData.coin_id} to PostgreSQL`);

    } catch (error) {

        console.error("PostgreSQL insert error:", error);

    }
}

/**
 * =========================================================
 * Kafka Consumer Startup
 * =========================================================
 */

async function startConsumer() {

    await consumer.connect();

    console.log("Kafka consumer connected");

    await consumer.subscribe({
        topic: process.env.KAFKA_TOPIC,
        fromBeginning: false,
    });

    console.log("Listening for crypto events...");

    await consumer.run({

        eachMessage: async ({ topic, partition, message }) => {

            try {

                const data = JSON.parse(
                    message.value.toString()
                );

                /**
                 * Build event object
                 */

                const event = {
                    ...data,
                    topic,
                    partition,
                    offset: message.offset,
                };

                /**
                 * Save locally
                 */

                latestEvents.unshift(event);

                /**
                 * Limit memory usage
                 */

                latestEvents = latestEvents.slice(0, 100);

                /**
                 * Save to PostgreSQL
                 */

                await saveCryptoEvent(event);

                console.log("Crypto Event:", event);

            } catch (error) {

                console.error("Consumer processing error:", error);

            }
        },
    });
}

/**
 * =========================================================
 * API Endpoint
 * =========================================================
 */

app.get("/crypto/analytics", async (req, res) => {

    try {

        /**
         * Query latest database records
         */

        const dbResult = await pool.query(`
            SELECT *
            FROM crypto_prices
            ORDER BY id DESC
            LIMIT 20
        `);

        /**
         * Latest coin per crypto
         */

        const latestByCoin = {};

        latestEvents.forEach((event) => {

            if (!latestByCoin[event.coin_id]) {
                latestByCoin[event.coin_id] = event;
            }
        });

        /**
         * Biggest gainer
         */

        const eventsArray = Object.values(latestByCoin);

        const biggestGainer = eventsArray.reduce((prev, current) =>
            prev.change_24h > current.change_24h ? prev : current
        );

        /**
         * Biggest loser
         */

        const biggestLoser = eventsArray.reduce((prev, current) =>
            prev.change_24h < current.change_24h ? prev : current
        );

        /**
         * API Response
         */

        res.json({
            totalEvents: latestEvents.length,

            trackedCoins: Object.keys(latestByCoin).length,

            latestByCoin,

            biggestGainer,

            biggestLoser,

            latestDatabaseEvents: dbResult.rows,
        });

    } catch (error) {

        console.error(error);

        res.status(500).json({
            error: "Failed to fetch analytics",
        });
    }
});

/**
 * =========================================================
 * Start Everything
 * =========================================================
 */

async function startServer() {

    try {

        await startConsumer();

        app.listen(3000, () => {

            console.log(
                "StreamMind Crypto API running at http://localhost:3000"
            );
        });

    } catch (error) {

        console.error("Startup error:", error);

    }
}

startServer();