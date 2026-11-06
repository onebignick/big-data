"""P2 curate job: landing CSV -> cleaned, feature-engineered Parquet partitioned by dt.

Deck slide 10. Every windowed feature looks backward only (no future rows), and the
amount z-score uses train-period statistics only, so nothing leaks from the test period.
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "bucket", "dataset", "train_end", "test_end"]
)
BUCKET, DS = args["bucket"], args["dataset"]
TRAIN_END, TEST_END = args["train_end"], args["test_end"]  # inclusive, yyyy-MM-dd

sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
job = Job(glue)
job.init(args["JOB_NAME"], args)

DAY = 86400
src = f"s3://{BUCKET}/landing/{DS}/transactions/"
dst = f"s3://{BUCKET}/curated/{DS}/transactions/"

# Read everything as strings: bank/account IDs keep their zero padding.
cols = [
    "ts_raw", "from_bank", "from_account", "to_bank", "to_account",
    "amount_received", "receiving_currency", "amount_paid", "payment_currency",
    "payment_format", "is_laundering",
]
raw = (
    spark.read.option("header", True).option("inferSchema", False).csv(src)
    .toDF(*cols)  # raw header has two columns named "Account"
)
n_raw = raw.count()

df = (
    raw.dropDuplicates()
    .withColumn("ts", F.to_timestamp("ts_raw", "yyyy/MM/dd HH:mm"))
    .withColumn("amount_received", F.col("amount_received").cast("double"))
    .withColumn("amount_paid", F.col("amount_paid").cast("double"))
    .withColumn("is_laundering", F.col("is_laundering").cast("int"))
    .withColumn("from_key", F.concat_ws("|", "from_bank", "from_account"))
    .withColumn("to_key", F.concat_ws("|", "to_bank", "to_account"))
    .withColumn("epoch", F.col("ts").cast("long"))
    .withColumn("dt", F.date_format("ts", "yyyy-MM-dd"))
)
n_dedup = df.count()
n_bad_ts = df.filter(F.col("ts").isNull()).count()
df = df.filter(F.col("ts").isNotNull())

# Stable transaction id (no id in the source): hash of all raw fields.
df = df.withColumn("txn_id", F.sha2(F.concat_ws("\u0001", *cols), 256).substr(1, 20))

# Row-level features.
df = (
    df.withColumn("hour_of_day", F.hour("ts"))
    .withColumn("day_of_week", F.dayofweek("ts"))  # 1 = Sunday
    .withColumn("is_self_transfer", (F.col("from_key") == F.col("to_key")).cast("int"))
    .withColumn("is_cross_bank", (F.col("from_bank") != F.col("to_bank")).cast("int"))
    .withColumn(
        "is_cross_currency",
        (F.col("receiving_currency") != F.col("payment_currency")).cast("int"),
    )
    .withColumn("log_amount", F.log1p("amount_paid"))
    .withColumn(
        "split",
        F.when(F.col("dt") <= TRAIN_END, "train")
        .when(F.col("dt") <= TEST_END, "test")
        .otherwise("tail"),
    )
)

# amount_z: z-score of log amount within payment currency, train-period stats only.
stats = (
    df.filter(F.col("split") == "train")
    .groupBy("payment_currency")
    .agg(F.mean("log_amount").alias("mu"), F.stddev("log_amount").alias("sd"))
)
df = (
    df.join(F.broadcast(stats), "payment_currency", "left")
    .withColumn(
        "amount_z",
        F.when(F.col("sd") > 0, (F.col("log_amount") - F.col("mu")) / F.col("sd")),
    )
    .drop("mu", "sd")
)

# Backward-looking 24h windows (rows in the same minute count as concurrent).
w_from = Window.partitionBy("from_key").orderBy("epoch").rangeBetween(-DAY, 0)
w_to = Window.partitionBy("to_key").orderBy("epoch").rangeBetween(-DAY, 0)
df = (
    df.withColumn("acct_txn_count_24h", F.count(F.lit(1)).over(w_from))
    .withColumn("fan_out_count", F.size(F.collect_set("to_key").over(w_from)))
    .withColumn("fan_in_count", F.size(F.collect_set("from_key").over(w_to)))
)

out_cols = [
    "txn_id", "ts", "from_bank", "from_account", "to_bank", "to_account",
    "from_key", "to_key", "amount_received", "receiving_currency", "amount_paid",
    "payment_currency", "payment_format", "is_laundering", "hour_of_day",
    "day_of_week", "is_self_transfer", "is_cross_bank", "is_cross_currency",
    "log_amount", "amount_z", "acct_txn_count_24h", "fan_out_count",
    "fan_in_count", "split", "dt",
]
(
    df.select(*out_cols)
    .repartition("dt")
    .sortWithinPartitions("ts")
    .write.mode("overwrite")
    .partitionBy("dt")
    .parquet(dst)
)

n_out = spark.read.parquet(dst).count()
print(f"CURATE_STATS raw={n_raw} dedup={n_dedup} bad_ts={n_bad_ts} out={n_out}")
job.commit()
