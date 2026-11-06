"""P5 stream: Kafka (raw CSV rows) -> Spark Structured Streaming on EMR.

Reads the RAW transaction lines straight off Kafka and does everything in the stream:
  1. clean: parse the CSV line with every field as a string (keeps zero-padded IDs), parse the
     timestamp with an explicit format, build the same txn_id as the batch job, drop duplicates;
  2. features: the same row features as the batch curate job, plus backward 24 h (model) and
     6 h (rules) counts per account from a rolling event-time history kept in the stream;
  3. BQ1 on the stream: running laundering rates by payment format, currency, cross-bank /
     cross-currency / self-transfer flag and hour -> results/bq1_stream/rates/run=RUN/;
  4. BQ2: BQ1 model score + rules mined by the batch graph job -> results/stream_scored/,
     results/flags/ and an SNS alert;
  5. archive: a second query writes every raw line unchanged into the lake's landing zone
     (landing/<dataset>/stream/dt=...), which is where batch training and graph mining read
     history from.

Only model parameters come from the lake: the trained model, its threshold, the per-currency
amount statistics of the training period, the graph job's flagged accounts, and the 24 h of
history before the replay starts (warm start, so the first rows have full windows).
Args: bucket run_id topic_arn bootstrap kafka_topic dataset replay_day [max_minutes] [idle_s]
"""
import json
import sys
import time
from datetime import date, timedelta

import boto3
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET, RUN, TOPIC, BOOTSTRAP, KTOPIC, DS, DAY = sys.argv[1:8]
MAX_MIN = float(sys.argv[8]) if len(sys.argv) > 8 else 25
IDLE_S = float(sys.argv[9]) if len(sys.argv) > 9 else 120
REGION = "ap-southeast-1"
H24, H6 = 24 * 3600, 6 * 3600
R = {"fan_out": 5, "fan_in": 5, "pattern_bonus": 0.15, "rule_bonus": 0.10}
RAW_COLS = ["ts_raw", "from_bank", "from_account", "to_bank", "to_account", "amount_received",
            "receiving_currency", "amount_paid", "payment_currency", "payment_format", "is_laundering"]
RATE_DIMS = ["payment_format", "payment_currency", "receiving_currency", "format_currency",
             "is_cross_bank", "is_cross_currency", "is_self_transfer", "hour_of_day"]

spark = SparkSession.builder.appName(f"bda8-stream-{RUN}").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "8")
sns = boto3.client("sns", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
LAKE = f"s3://{BUCKET}"

# ---- model parameters (lake) ----
model = PipelineModel.load(f"{LAKE}/models/bq1_rf/")
thr = spark.read.parquet(f"{LAKE}/results/bq1/test_scores/").approxQuantile("p", [0.995], 0.0001)[0]
flagged_accts = (
    spark.read.parquet(f"{LAKE}/results/bq2/account_flags/run=train/")
    .groupBy("account").agg(F.concat_ws("+", F.sort_array(F.collect_set("pattern"))).alias("patterns"))
    .cache()
)
curated = spark.read.parquet(f"{LAKE}/curated/{DS}/transactions/")
amount_stats = F.broadcast(
    curated.filter("split = 'train'").groupBy("payment_currency")
    .agg(F.mean("log_amount").alias("mu"), F.stddev("log_amount").alias("sd")).cache()
)
day0 = date.fromisoformat(DAY)
warm = (
    curated.filter(F.col("dt") == (day0 - timedelta(days=1)).isoformat())
    .select("txn_id", "from_key", "to_key", F.col("ts").cast("long").cast("double").alias("t"))
    .localCheckpoint()
)
print("STREAM_INIT", json.dumps({"threshold": thr, "flagged_accounts": flagged_accts.count(),
                                 "currencies": amount_stats.count(), "warm_history": warm.count()}))

state = {"history": warm, "last_data": None, "last_sns": 0.0, "last_rates": 0.0,
         "rows": 0, "dups": 0, "flags": 0, "rates": {}, "max_ts": None}


def clean(batch):
    """Raw Kafka records -> typed, de-duplicated rows with the batch job's row features."""
    ddl = ", ".join(f"{c} STRING" for c in RAW_COLS)
    raw = batch.select(F.col("value").cast("string").alias("line"),
                       (F.col("timestamp").cast("double")).alias("sent_at"))
    p = raw.select(F.from_csv("line", ddl).alias("r"), "sent_at").select("r.*", "sent_at")
    return (
        p.withColumn("txn_id", F.sha2(F.concat_ws("\u0001", *RAW_COLS), 256).substr(1, 20))
        .withColumn("ts", F.to_timestamp("ts_raw", "yyyy/MM/dd HH:mm"))
        .filter(F.col("ts").isNotNull())
        .dropDuplicates(["txn_id"])
        .withColumn("t", F.col("ts").cast("long").cast("double"))
        .withColumn("amount_paid", F.col("amount_paid").cast("double"))
        .withColumn("is_laundering", F.col("is_laundering").cast("int"))
        .withColumn("from_key", F.concat_ws("|", "from_bank", "from_account"))
        .withColumn("to_key", F.concat_ws("|", "to_bank", "to_account"))
        .withColumn("hour_of_day", F.hour("ts"))
        .withColumn("day_of_week", F.dayofweek("ts"))
        .withColumn("is_self_transfer", (F.col("from_key") == F.col("to_key")).cast("int"))
        .withColumn("is_cross_bank", (F.col("from_bank") != F.col("to_bank")).cast("int"))
        .withColumn("is_cross_currency", (F.col("receiving_currency") != F.col("payment_currency")).cast("int"))
        .withColumn("log_amount", F.log1p("amount_paid"))
        .withColumn("format_currency", F.concat_ws(" / ", "payment_format", "payment_currency"))
    )


def window_features(rows, hist):
    """Backward 24 h / 6 h counts per account over history + this batch (row itself included)."""
    b = rows.select("txn_id", "from_key", "to_key", "t")
    ev = hist.unionByName(b)
    e = ev.select(F.col("from_key").alias("e_from"), F.col("to_key").alias("e_to"), F.col("t").alias("e_t"))
    in6 = F.col("e_t") >= F.col("t") - H6
    out_side = (
        b.join(e, (F.col("e_from") == F.col("from_key")) & (F.col("e_t") >= F.col("t") - H24)
               & (F.col("e_t") <= F.col("t")))
        .groupBy("txn_id").agg(
            F.count(F.lit(1)).alias("acct_txn_count_24h"),
            F.countDistinct("e_to").alias("fan_out_count"),
            F.count(F.when(in6, 1)).alias("window_txn_count"),
            F.countDistinct(F.when(in6, F.col("e_to"))).alias("window_distinct_cp"))
    )
    in_side = (
        b.join(e, (F.col("e_to") == F.col("to_key")) & (F.col("e_t") >= F.col("t") - H24)
               & (F.col("e_t") <= F.col("t")))
        .groupBy("txn_id").agg(
            F.countDistinct("e_from").alias("fan_in_count"),
            F.countDistinct(F.when(in6, F.col("e_from"))).alias("window_distinct_in"))
    )
    return out_side.join(in_side, "txn_id", "full"), ev


def update_rates(rows, batch_ts):
    """BQ1 on the stream: add this batch's counts to the running per-group totals."""
    pairs = F.explode(F.array(*[F.struct(F.lit(d).alias("dim"), F.col(d).cast("string").alias("value"))
                                for d in RATE_DIMS]))
    for r in (rows.select(pairs.alias("p"), "is_laundering").select("p.*", "is_laundering")
              .groupBy("dim", "value").agg(F.count(F.lit(1)).alias("n"), F.sum("is_laundering").alias("l"))
              .collect()):
        n, l = state["rates"].get((r.dim, r.value), (0, 0))
        state["rates"][(r.dim, r.value)] = (n + r.n, l + (r.l or 0))
    state["max_ts"] = batch_ts


def write_rates():
    if not state["rates"]:
        return
    snap = [(d, v, n, l, 1e4 * l / n, state["max_ts"], time.time())
            for (d, v), (n, l) in state["rates"].items()]
    (spark.createDataFrame(snap, "dim string, value string, txns long, laundering long, "
                                 "rate_bp double, as_of_event_ts string, updated_at double")
     .coalesce(1).write.mode("overwrite").parquet(f"{LAKE}/results/bq1_stream/rates/run={RUN}/"))
    state["last_rates"] = time.time()


def process(batch, batch_id):
    t_start = time.time()
    hist = state["history"]
    cleaned = clean(batch).cache()
    n_in = cleaned.count()
    if n_in == 0:
        cleaned.unpersist()
        return
    state["last_data"] = time.time()
    # replays and duplicates already seen in an earlier batch
    rows = cleaned.join(hist.select("txn_id"), "txn_id", "left_anti") \
        .withColumn("received_at", F.lit(t_start)).cache()
    n = rows.count()
    state["dups"] += n_in - n

    win, ev = window_features(rows, hist)
    feats = (
        rows.join(win, "txn_id", "left")
        .join(amount_stats, "payment_currency", "left")
        .withColumn("amount_z", F.coalesce(F.when(F.col("sd") > 0, (F.col("log_amount") - F.col("mu")) / F.col("sd")),
                                           F.lit(0.0)))
        .withColumn("log_txn_24h", F.log1p("acct_txn_count_24h"))
        .withColumn("log_fan_out", F.log1p("fan_out_count"))
        .withColumn("log_fan_in", F.log1p("fan_in_count"))
        .join(F.broadcast(flagged_accts.withColumnRenamed("account", "from_key")
                          .withColumnRenamed("patterns", "from_patterns")), "from_key", "left")
        .join(F.broadcast(flagged_accts.withColumnRenamed("account", "to_key")
                          .withColumnRenamed("patterns", "to_patterns")), "to_key", "left")
    )
    scored = model.transform(feats).withColumn("model_score", vector_to_array("probability")[1])
    hit_pattern = F.col("from_patterns").isNotNull() | F.col("to_patterns").isNotNull()
    hit_out = F.col("window_distinct_cp") >= R["fan_out"]
    hit_in = F.col("window_distinct_in") >= R["fan_in"]
    out = (
        scored.withColumn("pattern_flag", hit_pattern.cast("int"))
        .withColumn("risk_score", F.col("model_score")
                    + F.when(hit_pattern, R["pattern_bonus"]).otherwise(0.0)
                    + F.when(hit_out, R["rule_bonus"]).otherwise(0.0)
                    + F.when(hit_in, R["rule_bonus"]).otherwise(0.0))
        .withColumn("matched_rule", F.concat_ws(",",
            F.when(F.col("model_score") >= thr, F.lit("model")),
            F.when(hit_pattern, F.concat(F.lit("pattern:"), F.coalesce("from_patterns", "to_patterns"))),
            F.when(hit_out, F.lit("window_fan_out")), F.when(hit_in, F.lit("window_fan_in"))))
        .withColumn("flagged", (F.col("risk_score") >= thr).cast("int"))
        .withColumn("scored_at", F.lit(time.time()))
        .withColumn("latency_s", F.col("scored_at") - F.col("sent_at"))
        .withColumn("batch_id", F.lit(batch_id))
        .select("txn_id", F.date_format("ts", "yyyy-MM-dd HH:mm:ss").alias("ts"), "t", "from_key", "to_key",
                "amount_paid", "payment_currency", "payment_format", "is_laundering", "acct_txn_count_24h",
                "fan_out_count", "fan_in_count", "amount_z", "window_txn_count", "window_distinct_cp",
                "window_distinct_in", "pattern_flag", "model_score", "risk_score", "matched_rule", "flagged",
                "sent_at", "received_at", "scored_at", "latency_s", "batch_id")
        .cache()
    )
    out.write.mode("append").parquet(f"{LAKE}/results/stream_scored/run={RUN}/")
    flags = out.filter("flagged = 1").cache()
    nf = flags.count()
    if nf:
        flags.write.mode("append").parquet(f"{LAKE}/results/flags/run={RUN}/")
    state["rows"] += n
    state["flags"] += nf

    if nf and time.time() - state["last_sns"] >= 60:  # at most one alert a minute
        top = flags.orderBy(F.desc("risk_score")).limit(5).collect()
        lines = [f"{r.txn_id}  {r.from_key} -> {r.to_key}  {r.amount_paid:,.2f} {r.payment_currency}"
                 f"  risk={r.risk_score:.3f}  [{r.matched_rule}]" for r in top]
        sns.publish(TopicArn=TOPIC, Subject=f"AML stream: {nf} flagged in batch {batch_id}"[:100],
                    Message=f"Run {RUN}, batch {batch_id}: {nf} of {n} transactions flagged.\n\n" + "\n".join(lines))
        state["last_sns"] = time.time()

    max_t, max_ts = rows.agg(F.max("t"), F.date_format(F.max("ts"), "yyyy-MM-dd HH:mm")).first()
    update_rates(rows, max_ts)
    if time.time() - state["last_rates"] >= 60:
        write_rates()

    # keep the last 24 h of event time as history (txn_id doubles as the de-dup set)
    new_hist = ev.filter(F.col("t") > max_t - H24).localCheckpoint()
    hist.unpersist()
    state["history"] = new_hist
    for d in (flags, out, rows, cleaned):
        d.unpersist()
    print("STREAM_BATCH", json.dumps({"batch": batch_id, "rows": n, "dups": n_in - n, "flags": nf,
                                      "secs": round(time.time() - t_start, 2)}))


def kafka():
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("subscribe", KTOPIC)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load())


score_q = (kafka().writeStream.queryName("score").foreachBatch(process)
           .option("checkpointLocation", f"{LAKE}/checkpoints/stream/run={RUN}/score/")
           .trigger(processingTime="10 seconds").start())
archive_q = (kafka()
             .select(F.col("value").cast("string").alias("value"),
                     F.regexp_replace(F.substring(F.col("value").cast("string"), 1, 10), "/", "-").alias("dt"))
             .writeStream.queryName("archive").format("text").partitionBy("dt")
             .option("path", f"{LAKE}/landing/{DS}/stream/")
             .option("checkpointLocation", f"{LAKE}/checkpoints/stream/run={RUN}/archive/")
             .trigger(processingTime="60 seconds").start())

# Tell the orchestrator both consumers are live, so the replay starts after "latest" is pinned.
time.sleep(30)
s3.put_object(Bucket=BUCKET, Key=f"results/stream_ready/run={RUN}", Body=b"ready")
t0 = time.time()
while score_q.isActive:
    score_q.awaitTermination(15)
    now = time.time()
    if now - t0 > MAX_MIN * 60 or (state["last_data"] and now - state["last_data"] > IDLE_S):
        break
archive_q.processAllAvailable()
for q in (score_q, archive_q):
    q.stop()
write_rates()
print("STREAM_DONE", json.dumps({"rows": state["rows"], "dups": state["dups"], "flags": state["flags"]}))
