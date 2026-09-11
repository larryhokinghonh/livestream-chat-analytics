import os
import psycopg

from dotenv import load_dotenv
from zoneinfo import ZoneInfo
from pyspark.sql.functions import col

load_dotenv()

def store_chat_messages(batch_df, batch_id):
  rows = (
    batch_df
    .select(
      "event_id",
      "channel_id",
      "channel_name",
      "stream_id",
      "user_id",
      "username",
      col("text").alias("text_message"),
      "event_time"
    )
    .collect()
  )

  if not rows:
    return

  with psycopg.connect(
    host="localhost",
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
  ) as conn:

    with conn.cursor() as cur:
      cur.executemany(
          """
          INSERT INTO chat_messages (
              event_id,
              channel_id,
              channel_name,
              stream_id,
              user_id,
              username,
              text_message,
              event_time
          )
          VALUES (
              %s, %s, %s, (SELECT stream_id FROM stream_sessions WHERE stream_id = %s), %s, %s, %s, %s
          )
          ON CONFLICT (event_id)
          DO NOTHING
          """,
          [
              (
                  row.event_id,
                  row.channel_id,
                  row.channel_name,
                  row.stream_id,
                  row.user_id,
                  row.username,
                  row.text_message,
                  row.event_time.replace(tzinfo=ZoneInfo("Asia/Singapore")),
              )
              for row in rows
          ]
      )

def store_chat_messages_per_min(batch_df, batch_id):
  rows = (
    batch_df
    .select(
      col("channel_id"),
      col("channel_name"),
      col("stream_id"),
      col("window.start").alias("start_window"),
      col("window.end").alias("end_window"),
      col("count").alias("message_count")
    )
    .collect()
  )

  if not rows:
    return

  with psycopg.connect(
    host="localhost",
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
  ) as conn:

    with conn.cursor() as cur:
      cur.executemany(
        """
        INSERT INTO messages_per_minute (
          channel_id,
          stream_id,
          channel_name,
          start_window,
          end_window,
          message_count
        ) 
        VALUES (%s, (SELECT stream_id FROM stream_sessions WHERE stream_id = %s), %s, %s, %s, %s)

        ON CONFLICT(channel_id, start_window)

        DO UPDATE SET
          end_window = EXCLUDED.end_window,
          message_count = EXCLUDED.message_count,
          updated_at = NOW()
        """,

        [
          (
            row.channel_id,
            row.stream_id,
            row.channel_name,
            row.start_window.replace(tzinfo=ZoneInfo("Asia/Singapore")),
            row.end_window.replace(tzinfo=ZoneInfo("Asia/Singapore")),
            row.message_count,
          )
          for row in rows
        ]
      )

def store_stream_sessions(stream_id, channel_id, channel_name, started_at):
  with psycopg.connect(
    host="localhost",
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
  ) as conn:

    with conn.cursor() as cur:
      cur.execute(
        """
        INSERT INTO stream_sessions (
          stream_id, channel_id, channel_name, started_at
        )
        VALUES
        (%s, %s, %s, %s)
        ON CONFLICT(stream_id) DO NOTHING
        """,
        (stream_id, channel_id, channel_name, started_at,)
      )

def update_stream_sessions(stream_id):
  with psycopg.connect(
    host="localhost",
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
  ) as conn:

    with conn.cursor() as cur:
      cur.execute(
        """
        UPDATE stream_sessions
        SET ended_at = NOW()
        WHERE stream_id = %s AND ended_at IS NULL
        """,
        (stream_id,)
      )