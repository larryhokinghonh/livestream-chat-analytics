import asyncio
import json
import httpx
import websockets
import os
import sys
from dotenv import load_dotenv
from utils import db

from confluent_kafka import Producer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window
from pyspark.sql.types import (StructType, StructField, StringType)

load_dotenv()

EVENTSUB_URL = os.getenv("EVENTSUB_URL")
CLIENT_ID = os.getenv("CLIENT_ID")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

# Twitch account associated with ACCESS_TOKEN
USER_ID = os.getenv("USER_ID")

BROADCASTER_USERNAME = os.getenv("BROADCASTER_USERNAME").split(",")

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

spark = (
    SparkSession.builder
    .appName("TwitchChatAnalytics")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "twitch-chat")
    .option("startingOffsets", "latest")
    .load()
)

messages = raw_stream.select(
    col("value").cast("string").alias("json")
)

schema = StructType([
    StructField("event_id", StringType()),
    StructField("channel_id", StringType()),
    StructField("channel_name", StringType()),
    StructField("user_id", StringType()),
    StructField("username", StringType()),
    StructField("text", StringType()),
    StructField("timestamp", StringType())
])

parsed = (
    messages
    .select(
        from_json(col("json"), schema).alias("data")
    )
    .select("data.*")
)

parsed = parsed.withColumn(
    "event_time",
    to_timestamp("timestamp")
)

messages_per_minute = (
    parsed
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("channel_id"),
        col("channel_name")
    )
    .count()
)

query = (
    messages_per_minute
    .writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", False)
    .trigger(processingTime="5 seconds")
    .start()
)

chat_message_query = (
    parsed
    .writeStream
    .foreachBatch(db.store_chat_messages)
    .option(
        "checkpointLocation",
        "./checkpoints/chat_messages"
    )
    .start()
)

chat_messages_per_min_query = (
    messages_per_minute
    .writeStream
    .outputMode("update")
    .foreachBatch(db.store_chat_messages_per_min)
    .option(
        "checkpointLocation",
        "./checkpoints/messages_per_minute"
    )
    .start()
)

def get_twitch_user_id():
    broadcaster_ids = {}

    headers = {
        "Client-Id": CLIENT_ID,
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }

    params = [
        ("login", username) for username in BROADCASTER_USERNAME
    ]

    response = httpx.get(
        "https://api.twitch.tv/helix/users",
        headers=headers,
        params=params
    )

    try:
        response.raise_for_status()

        for broadcaster in response.json()["data"]:
            broadcaster_ids[broadcaster["login"]] = broadcaster["id"]

        return broadcaster_ids
    except httpx.HTTPStatusError as e:
        print(f"Twitch API error: {e.response.status_code} - {e.response.text}")
        sys.exit()

async def create_chat_subscription(broadcaster_id, broadcaster_username, session_id):
    url = "https://api.twitch.tv/helix/eventsub/subscriptions"

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Client-Id": CLIENT_ID,
        "Content-Type": "application/json",
    }

    body = {
        "type": "channel.chat.message",
        "version": "1",
        "condition": {
            "broadcaster_user_id": broadcaster_id,
            "user_id": USER_ID,
        },
        "transport": {
            "method": "websocket",
            "session_id": session_id,
        }
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(
            url,
            headers=headers,
            json=body,
        )

        response.raise_for_status()

        print(f"Subscription created for {broadcaster_username}, with id {broadcaster_id}.")

async def main():
    broadcaster_ids = get_twitch_user_id()

    async with websockets.connect(EVENTSUB_URL) as websocket:
        async for raw_message in websocket:
            message = json.loads(raw_message)
            message_type = message["metadata"]["message_type"]

            if message_type == "session_welcome":
                session_id = message["payload"]["session"]["id"]

                print("Connected!")
                # print("Session:", session_id)

                for broadcaster_username, broadcaster_id in broadcaster_ids.items():
                    await create_chat_subscription(broadcaster_id, broadcaster_username, session_id)
            elif message_type == "notification":
                event = message["payload"]["event"]
                event_timestamp = message["metadata"]["message_timestamp"]

                chat_event = {
                    "event_id": event["message_id"],
                    "channel_id": event["broadcaster_user_id"],
                    "channel_name": event["broadcaster_user_name"],
                    "user_id": event["chatter_user_id"],
                    "username": event["chatter_user_name"],
                    "text": event["message"]["text"],
                    "timestamp": event_timestamp
                }

                producer.produce(
                    topic="twitch-chat",
                    value=json.dumps(chat_event).encode("utf-8")
                )

                producer.poll(0)

                channel_name = chat_event["channel_name"]
                username = chat_event["username"]
                text = chat_event["text"]

                print(f"[{channel_name}] - {username}: {text}")
            elif message_type == "session_keepalive":
                print("keepalive")
            elif message_type == "session_reconnect":
                reconnect_url = (message["payload"]["session"]["reconnect_url"])

                print(f"Twitch requested reconnect: {reconnect_url}")

try:
    asyncio.run(main())
finally:
    producer.flush()
    query.stop()
    spark.stop()