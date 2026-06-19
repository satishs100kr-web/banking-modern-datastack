# =============================================================================
#  kafka_to_minio.py  —  THE CONSUMER  (Kafka  -->  MinIO data lake)
#  WHAT IT DOES: listens to the 3 Kafka topics, keeps only the NEW row from each
#  change event, batches 50 rows, and writes them as a Parquet file into MinIO.
#
#  PIPELINE POSITION:
#     ... Kafka topics --> [THIS SCRIPT] --> MinIO (raw bucket, Parquet) --> Airflow ...
# =============================================================================

import boto3                              # AWS S3 client library — MinIO speaks the S3 API
from kafka import KafkaConsumer           # reads messages from Kafka
import json                              # decode the message bytes into a Python dict
import pandas as pd                      # turn a list of rows into a table (DataFrame)
from datetime import datetime           # to timestamp / date-partition the output files
import os                               # read env vars + delete the temp file
from dotenv import load_dotenv          # load this folder's .env (consumer/.env)

# Load consumer/.env -> KAFKA_BOOTSTRAP, MINIO_ENDPOINT, MINIO_* creds, MINIO_BUCKET.
load_dotenv()

# -----------------------------------------------------------------------------
# Create the Kafka consumer — the thing that reads the change events.
# -----------------------------------------------------------------------------
consumer = KafkaConsumer(
    # The 3 topics to read (one per table). Debezium created these.
    'banking_server.public.customers',
    'banking_server.public.accounts',
    'banking_server.public.transactions',
    # WHERE Kafka is. From the Mac we use host.docker.internal:29092 (set in .env).
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
    # On a brand-new consumer group, start from the OLDEST message so we capture history.
    auto_offset_reset='earliest',
    # Let Kafka auto-save our position (offset) so a restart resumes where we stopped.
    enable_auto_commit=True,
    # The consumer GROUP id. Kafka stores this group's offsets => restart-safe, no dups.
    group_id=os.getenv("KAFKA_GROUP"),
    # BUG FIX (learned the hard way): kafka-python 3.0.0 aborts any fetch frame >= 1MB
    # ("Invalid frame length"). Debezium messages embed the schema and are large, so we
    # cap each fetch well under 1MB. Without these two lines the consumer silently reads 0.
    max_partition_fetch_bytes=262144,   # 256 KB per partition fetch
    fetch_max_bytes=262144,             # 256 KB per overall fetch
    # Each message arrives as raw BYTES. This turns bytes -> JSON text -> Python dict.
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# -----------------------------------------------------------------------------
# Create the MinIO (S3) client — where we'll save the Parquet files.
# MinIO is S3-compatible, so the same boto3 code would work against real AWS S3.
# -----------------------------------------------------------------------------
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT"),          # e.g. http://localhost:9000
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),   # minioadmin
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
)

bucket = os.getenv("MINIO_BUCKET")   # the target bucket name, e.g. "raw"

# Make sure the bucket exists. list_buckets() returns all buckets; if ours isn't
# in that list, create it. (So the script works on a fresh MinIO with no setup.)
if bucket not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
    s3.create_bucket(Bucket=bucket)

# -----------------------------------------------------------------------------
# Helper: write one batch of rows for one table to MinIO as a Parquet file.
# -----------------------------------------------------------------------------
def write_to_minio(table_name, records):
    if not records:            # nothing to write -> stop early
        return
    df = pd.DataFrame(records)                     # list of dict rows -> a table
    date_str = datetime.now().strftime('%Y-%m-%d') # today's date, for partitioning
    file_path = f'{table_name}_{date_str}.parquet' # a LOCAL temp file name on disk
    # Write the DataFrame to a Parquet file (columnar, compressed, typed — ideal for analytics).
    df.to_parquet(file_path, engine='fastparquet', index=False)
    # The S3 "key" (path inside the bucket). We partition by date and add a unique time
    # so files never overwrite each other:  customers/date=2026-06-19/customers_153012.parquet
    s3_key = f'{table_name}/date={date_str}/{table_name}_{datetime.now().strftime("%H%M%S%f")}.parquet'
    s3.upload_file(file_path, bucket, s3_key)      # push the file up to MinIO
    os.remove(file_path)                           # delete the local temp copy
    print(f'✅ Uploaded {len(records)} records to s3://{bucket}/{s3_key}')

# -----------------------------------------------------------------------------
# Batching setup. We don't write one tiny file per message (the "small files
# problem"). Instead we collect rows in a per-topic buffer and flush every 50.
# -----------------------------------------------------------------------------
batch_size = 50
buffer = {                                  # one list per topic, holding waiting rows
    'banking_server.public.customers': [],
    'banking_server.public.accounts': [],
    'banking_server.public.transactions': []
}

print("✅ Connected to Kafka. Listening for messages...")  # prints once the consumer is ready

# -----------------------------------------------------------------------------
# The main loop. `for message in consumer:` blocks forever, yielding each new
# Kafka message as it arrives (this is why the script "sits and waits" — that's
# normal, healthy behaviour for a streaming consumer).
# -----------------------------------------------------------------------------
for message in consumer:
    topic = message.topic                  # which table this event is for
    event = message.value                  # the decoded Debezium event (a dict)
    payload = event.get("payload", {})     # Debezium wraps everything in "payload"
    record = payload.get("after")          # "after" = the row's NEW state (what we keep)

    if record:                             # ignore events with no new row (e.g. deletes)
        buffer[topic].append(record)       # add the row to that topic's buffer
        print(f"[{topic}] -> {record}")    # debug: show what we received

    # Once a topic's buffer reaches 50 rows, flush it to MinIO and reset the buffer.
    # topic.split('.')[-1] turns "banking_server.public.customers" -> "customers".
    if len(buffer[topic]) >= batch_size:
        write_to_minio(topic.split('.')[-1], buffer[topic])
        buffer[topic] = []
