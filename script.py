import asyncio
import json
import httpx
import websockets
import os
import sys
import json
from dotenv import load_dotenv
from utils import db

from confluent_kafka import Producer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window
from pyspark.sql.types import (StructType, StructField, StringType)

from contextlib import AsyncExitStack
from websockets.exceptions import ConnectionClosed

load_dotenv()

EVENTSUB_URL = os.getenv("EVENTSUB_URL")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# Twitch account associated with ACCESS_TOKEN
USER_ID = os.getenv("USER_ID")

BROADCASTER_USERNAME = os.getenv("BROADCASTER_USERNAME").split(",")
ONLINE_STREAMS = dict()

# Token refresh timing for synchronization
TOKEN_REFRESH_MARGIN = 5 * 60
TOKEN_VALIDATION_INTERVAL = 60
TOKEN_RETRY_DELAY = 60

TOKEN_REFRESH_LOCK = asyncio.Lock()
TOKEN_PATH = "./runtime/twitch_tokens.json"

with open("./runtime/twitch_tokens.json", "r", encoding="utf-8") as file:
    tokens = json.load(file)

    ACCESS_TOKEN = tokens["access_token"]
    REFRESH_TOKEN = tokens["refresh_token"]

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
    .option("startingOffsets", "earliest")
    .load()
)

messages = raw_stream.select(
    col("value").cast("string").alias("json")
)

schema = StructType([
    StructField("event_id", StringType()),
    StructField("channel_id", StringType()),
    StructField("channel_name", StringType()),
    StructField("stream_id", StringType()),
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
        col("channel_name"),
        col("stream_id")
    )
    .count()
)

messages_every_five_seconds = (
    parsed
    .withWatermark("event_time", "30 seconds")
    .groupBy(
        window(col("event_time"), "5 seconds"),
        col("channel_id"),
        col("channel_name"),
        col("stream_id")
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
        "./runtime/spark/checkpoints/chat_messages"
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
        "./runtime/spark/checkpoints/messages_per_minute"
    )
    .start()
)

chat_messages_every_five_seconds = (
    messages_every_five_seconds
    .writeStream
    .outputMode("update")
    .foreachBatch(db.store_chat_messages_every_five_seconds)
    .option(
        "checkpointLocation",
        "./runtime/spark/checkpoints/messages_every_five_seconds"
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

async def create_eventsub_subscription(subscription_type, broadcaster_id, session_id):
    body = {
        "type": subscription_type,
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
        for attempt in range(2):
            token_used = ACCESS_TOKEN
            response = await client.post(
                "https://api.twitch.tv/helix/eventsub/subscriptions",
                headers={
                    "Authorization": f"Bearer {token_used}",
                    "Client-Id": CLIENT_ID,
                    "Content-Type": "application/json",
                },
                json=body
            )

            if response.status_code != 401 or attempt == 1:
                if response.is_error:
                    print("Subscription request: ", json.dumps(body))
                    print("Response status: ", response.status_code)
                    print("Response body: ", response.text)
                response.raise_for_status()
                return response.json()

            await refresh_rejected_token(token_used)

async def create_stream_chat_subscription(broadcaster_id, broadcaster_username, session_id):
    await create_eventsub_subscription("channel.chat.message", broadcaster_id, session_id)
    print(f"Subscription created for {broadcaster_username}'s chat")

async def initialize_online_streams(broadcaster_ids):
    global ONLINE_STREAMS

    streams = await check_for_online_streams(broadcaster_ids.values())

    for livestream in streams:
        ONLINE_STREAMS[livestream["user_id"]] = {
            "stream_id": livestream["id"],
            "started_at": livestream["started_at"]
        }

        await asyncio.to_thread(
            db.store_stream_sessions,
            livestream["id"],
            livestream["user_id"],
            livestream["user_name"],
            livestream["started_at"]
        )

        print(f"{livestream["user_name"]} is currently live, since {livestream["started_at"]}.")

async def check_for_online_streams(broadcaster_ids):
    params = [
        ("user_id", broadcaster_id)
        for broadcaster_id in broadcaster_ids
    ]

    async with httpx.AsyncClient() as client:
        for attempt in range(2):
            token_used = ACCESS_TOKEN
            response = await client.get(
                "https://api.twitch.tv/helix/streams",
                headers = {
                    "Authorization": f"Bearer {token_used}",
                    "Client-Id": CLIENT_ID
                },
                params=params
            )

            if response.status_code != 401 or attempt == 1:
                response.raise_for_status()
                return response.json()["data"]

            await refresh_rejected_token(token_used)

async def update_online_streams_dict(broadcaster_ids):
    global ONLINE_STREAMS

    streams = await check_for_online_streams(broadcaster_ids)

    current_streams = {}

    for stream in streams:
        broadcaster_id = stream["user_id"]

        current_streams[broadcaster_id] = {
            "stream_id": stream["id"],
            "channel_id": broadcaster_id,
            "channel_name": stream["user_name"],
            "started_at": stream["started_at"]
        }

    for broadcaster_id, curr_stream_id in current_streams.items():
        prev = ONLINE_STREAMS.get(broadcaster_id)

        if prev is None or prev["stream_id"] != curr_stream_id["stream_id"]:
            await asyncio.to_thread(
                db.store_stream_sessions,
                curr_stream_id["stream_id"],
                broadcaster_id,
                curr_stream_id["channel_name"],
                curr_stream_id["started_at"]
            )

    for broadcaster_id, prev_stream_id in ONLINE_STREAMS.items():
        curr = current_streams.get(broadcaster_id)

        if curr is None or curr["stream_id"] != prev_stream_id["stream_id"]:
            await asyncio.to_thread(
                db.update_stream_sessions,
                prev_stream_id["stream_id"]
            )

    ONLINE_STREAMS = current_streams

async def monitor_online_streams(broadcasters_ids):
    while True:
        try:
            await update_online_streams_dict(broadcasters_ids)
        except httpx.HTTPError as e:
            print(f"Failed to retrieve livestreams: {e}")

        await asyncio.sleep(60)

async def validate_access_token():
    async with httpx.AsyncClient() as client:
        response = await client.get(
            "https://id.twitch.tv/oauth2/validate",
            headers={
                "Authorization": f"OAuth {ACCESS_TOKEN}"
            }
        )

    if response.status_code == 200:
        return response.json()

    if response.status_code == 401:
        return None

    raise httpx.HTTPStatusError(
        f"Unexpected validation response: {response.status_code}",
        request=response.request,
        response=response
    )

async def get_new_access_token():
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "https://id.twitch.tv/oauth2/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": REFRESH_TOKEN,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET
            }
        )

    response.raise_for_status()

    return response.json()

async def replace_access_token():
    global ACCESS_TOKEN, REFRESH_TOKEN

    new_tokens = await get_new_access_token()

    new_access_token = new_tokens["access_token"]
    new_refresh_token = new_tokens["refresh_token"]

    store_tokens(new_access_token, new_refresh_token)

    ACCESS_TOKEN = new_access_token
    REFRESH_TOKEN = new_refresh_token

    print("Access tokens has been refreshed.")
    return new_tokens

async def refresh_access_token():
    retry_delay = TOKEN_RETRY_DELAY

    while True:
        try:
            expires_in = await ensure_valid_access_token()

            sleep_seconds = min(
                TOKEN_VALIDATION_INTERVAL,
                max(60, expires_in - TOKEN_REFRESH_MARGIN)
            )

            retry_delay = TOKEN_RETRY_DELAY

        except (httpx.TimeoutException, httpx.NetworkError) as error:
            print(f"Temporary authentication network failure: {error}")
            sleep_seconds = retry_delay
            retry_delay = min(retry_delay * 2, TOKEN_REFRESH_MARGIN)

        except httpx.HTTPStatusError as error:
            status_code = error.response.status_code

            if status_code in (400, 401):
                raise RuntimeError(
                    "Twitch rejected the refresh credentials; "
                    "user authorization is required again"
                ) from error

            print(f"Temporary authentication HTTP failure: {error}")
            sleep_seconds = retry_delay
            retry_delay = min(retry_delay * 2, TOKEN_REFRESH_MARGIN)

        await asyncio.sleep(sleep_seconds)

async def ensure_valid_access_token():
    async with TOKEN_REFRESH_LOCK:
        validation = await validate_access_token()

        if (validation is not None and validation["expires_in"] > TOKEN_REFRESH_MARGIN):
            return validation["expires_in"]

        new_tokens = await replace_access_token()
        return new_tokens["expires_in"]

def store_tokens(access_token, refresh_token):
    temporary_path = f"{TOKEN_PATH}.tmp"

    data = {
        "access_token": access_token,
        "refresh_token": refresh_token
    }

    with open(temporary_path, "w") as file:
        json.dump(data, file)
        file.flush()
        os.fsync(file.fileno())

    os.replace(temporary_path, TOKEN_PATH)

async def refresh_rejected_token(rejected_token):
    async with TOKEN_REFRESH_LOCK:
        if ACCESS_TOKEN != rejected_token:
            return

        await replace_access_token()

async def main():
    asyncio.create_task(refresh_access_token())

    broadcaster_ids = get_twitch_user_id()
    await initialize_online_streams(broadcaster_ids)

    asyncio.create_task(monitor_online_streams(broadcaster_ids.values()))

    retry_delay = 1
    loop = asyncio.get_running_loop()

    while True:
        started_at = loop.time()

        try:
            async with AsyncExitStack() as connections:
                websocket = await connections.enter_async_context(
                    websockets.connect(
                        EVENTSUB_URL,
                        open_timeout=10,
                        close_timeout=5
                    )
                )

                while True:
                    raw_message = await websocket.recv()
                    message = json.loads(raw_message)
                    message_type = message["metadata"]["message_type"]

                    if message_type == "session_welcome":
                        session_id = message["payload"]["session"]["id"]

                        print("Connected!")
                        # print("Session:", session_id)

                        for broadcaster_username, broadcaster_id in broadcaster_ids.items():
                            await create_stream_chat_subscription(broadcaster_id, broadcaster_username, session_id)

                    elif message_type == "notification":
                        subscription_type = (message["metadata"]["subscription_type"])
                        event = message["payload"]["event"]
                        event_timestamp = message["metadata"]["message_timestamp"]

                        if subscription_type == "channel.chat.message":
                            # print(event["message"]["fragments"][0]["emote"], event["message"]["fragments"][0]["mention"])

                            # if event["message"]["fragments"][0]["emote"]:
                            #     print(event["message"]["text"])

                            chat_event = {
                                "event_id": event["message_id"],
                                "channel_id": event["broadcaster_user_id"],
                                "channel_name": event["broadcaster_user_name"],
                                "stream_id": ONLINE_STREAMS.get(event["broadcaster_user_id"], {}).get("stream_id"),
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

                            # channel_name = chat_event["channel_name"]
                            # username = chat_event["username"]
                            # text = chat_event["text"]

                            # print(f"[{channel_name}] - {username}: {text}")

                    elif message_type == "session_keepalive":
                        print("keepalive")
                    elif message_type == "session_reconnect":
                        reconnect_url = (message["payload"]["session"]["reconnect_url"])
                        old_websocket = websocket

                        print("Twitch requested reconnect, opening replacement connection...")

                        # Establishing new connection, meeting Twitch's 30-second handoff window requirement
                        async with asyncio.timeout(20):
                            new_websocket = await connections.enter_async_context(
                                websockets.connect(
                                    reconnect_url,
                                    open_timeout=10,
                                    close_timeout=5
                                )
                            )

                            raw_welcome_message = await new_websocket.recv()
                            welcome_message = json.loads(raw_welcome_message)

                            if welcome_message["metadata"]["message_type"] != "session_welcome":
                                raise RuntimeError("Expeceted session_welcome on replacement connection.")

                        websocket = new_websocket
                        session_id = welcome_message["payload"]["session"]["id"]

                        await old_websocket.close()
                        print("Reconnect completed.")
        except (ConnectionClosed, OSError, TimeoutError) as e:
            if loop.time() - started_at >= 60:
                retry_delay = 1

            print(f"EventSub WebSocket connection failed: {e}. Attempting to reconnect in {retry_delay} seconds.")

            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)

try:
    asyncio.run(main())
finally:
    producer.flush()
    query.stop()
    chat_message_query.stop()
    chat_messages_per_min_query.stop()
    chat_messages_every_five_seconds.stop()
    spark.stop()