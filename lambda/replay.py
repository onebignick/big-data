"""P5 replay producer: plays the bank's core system.

Reads the RAW Kaggle CSV from the landing zone, keeps one day's rows exactly as they are
(no cleaning, no features, duplicates included), sorts them by their timestamp and sends
each line to Kafka at a fixed rate. Key = sending account, so one account's rows stay in
order on one partition. The Kafka record timestamp (CreateTime) is the send time, which
the stream uses to measure send-to-flag latency.

Events:
  {"action": "create_topic", "partitions": 3}
  {"day": "2022/09/08", "rate": 900, "max_records": 600000}
"""
import os
import time

import boto3
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BUCKET, RAW_KEY = os.environ["BUCKET"], os.environ["RAW_KEY"]
BOOTSTRAP, TOPIC = os.environ["BOOTSTRAP"].split(","), os.environ["TOPIC"]


def create_topic(partitions):
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id="bda8-replay-admin")
    try:
        admin.create_topics([NewTopic(TOPIC, num_partitions=partitions, replication_factor=2,
                                      topic_configs={"retention.ms": str(24 * 3600 * 1000)})])
        created = True
    except TopicAlreadyExistsError:
        created = False
    meta = admin.describe_topics([TOPIC])
    admin.close()
    return {"topic": TOPIC, "created": created, "partitions": len(meta[0]["partitions"])}


def day_lines(day):
    body = boto3.client("s3").get_object(Bucket=BUCKET, Key=RAW_KEY)["Body"]
    prefix = day.encode()
    rows = [line for line in body.iter_lines() if line.startswith(prefix)]  # header never matches
    rows.sort(key=lambda b: b[:16])  # "yyyy/MM/dd HH:mm" sorts as text; stable within a minute
    return rows


def handler(event, _ctx):
    if event.get("action") == "create_topic":
        return create_topic(int(event.get("partitions", 3)))

    day, rate = event.get("day", "2022/09/08"), min(int(event.get("rate", 900)), 5000)
    rows = day_lines(day)[: int(event.get("max_records", 10**7))]
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, acks=1, linger_ms=20,
                             batch_size=256 * 1024, compression_type="gzip", retries=5)
    failed = [0]

    def on_err(_exc):
        failed[0] += 1

    t0 = time.time()
    for i, line in enumerate(rows):
        # key = "From Bank|Account" (2nd and 3rd raw fields)
        parts = line.split(b",", 3)
        producer.send(TOPIC, key=parts[1] + b"|" + parts[2], value=line).add_errback(on_err)
        if i % rate == rate - 1:  # pace: `rate` rows per wall-clock second
            ahead = t0 + (i + 1) / rate - time.time()
            if ahead > 0:
                time.sleep(ahead)
    producer.flush()
    producer.close()
    out = {"day": day, "sent": len(rows), "failed": failed[0], "secs": round(time.time() - t0, 1)}
    print("REPLAY_DONE", out)
    return out
