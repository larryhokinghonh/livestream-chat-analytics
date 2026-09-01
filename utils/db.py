import os
import psycopg

from dotenv import load_dotenv
from zoneinfo import ZoneInfo
from pyspark.sql.functions import col

load_dotenv()

def store_chat_messages(batch_df, batch_id):
  (
    batch_df
    .select(
      "event_id",
      "channel_id",
      "channel_name",
      "user_id",
      "username",
      col("text").alias("text_message"),
      "event_time"
    )
    .write
    .mode("append")
    .jdbc(
      url=os.getenv("JDBC_URL"),
      table="chat_messages",
      properties={
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": os.getenv("DB_DRIVER")
      }
    )
  )

def store_chat_messages_per_min(batch_df, batch_id):
  rows = (
    batch_df
    .select(
      col("channel_id"),
      col("channel_name"),
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
          channel_name,
          start_window,
          end_window,
          message_count
        ) 
        VALUES (%s, %s, %s, %s, %s)

        ON CONFLICT(channel_id, start_window)

        DO UPDATE SET
          end_window = EXCLUDED.end_window,
          message_count = EXCLUDED.message_count,
          updated_at = NOW()
        """,

        [
          (
            row.channel_id,
            row.channel_name,
            row.start_window.replace(tzinfo=ZoneInfo("Asia/Singapore")),
            row.end_window.replace(tzinfo=ZoneInfo("Asia/Singapore")),
            row.message_count,
          )
          for row in rows
        ]
      )