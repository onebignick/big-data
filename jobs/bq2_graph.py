"""P4 / BQ2 batch path: mine laundering shapes from the account-to-account graph with GraphFrames.

Two runs of the same detectors:
  * "train" (01-07 Sep): per-account pattern flags the stream job may use (no test leakage).
  * "full"  (all days):  pattern frequencies, validated against the injected *_Patterns.txt.
Hub accounts (very high degree) are excluded from motif search: they are legitimate
bank-like accounts and make motif joins explode.
Outputs (results/bq2/): account_flags/run=..., pattern_frequency/, rules/.
"""
import re
import sys

from graphframes import GraphFrame
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

BUCKET, DS = sys.argv[1], sys.argv[2]
DAY = 86400
RULES = {  # thresholds, also written out for the stream job
    "hub_degree": 50,       # distinct counterparties over the run; above = hub, excluded from motifs
    "fan_out_day": 5,       # distinct receivers from one account within 1 day
    "fan_in_day": 5,        # distinct senders into one account within 1 day
    "gs_min_side": 3,       # gather-scatter: >= 3 senders in and >= 3 receivers out in 1 day
    "sg_min_mid": 3,        # scatter-gather: a -> >= 3 distinct middles -> c within 4 days
    "sg_window_days": 4,
    "cycle_window_days": 7, # cycles of length 2-4 with increasing timestamps within 7 days
}

spark = SparkSession.builder.appName("bda8-bq2-graph").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.sparkContext.setCheckpointDir(f"s3://{BUCKET}/tmp/graph-ckpt/")
cur = spark.read.parquet(f"s3://{BUCKET}/curated/{DS}/transactions/")


def detect(txns, run):
    e_all = (
        txns.filter("is_self_transfer = 0")
        .select(F.col("from_key").alias("src"), F.col("to_key").alias("dst"),
                F.col("ts").cast("long").alias("t"), F.to_date("ts").alias("d"))
    )
    # Degree over the run -> hubs.
    deg = (
        e_all.select(F.col("src").alias("id"), F.col("dst").alias("cp"))
        .unionByName(e_all.select(F.col("dst").alias("id"), F.col("src").alias("cp")))
        .groupBy("id").agg(F.countDistinct("cp").alias("degree"))
    )
    hubs = deg.filter(F.col("degree") > RULES["hub_degree"]).select("id")
    e = (
        e_all.join(hubs.withColumnRenamed("id", "src"), "src", "left_anti")
        .join(hubs.withColumnRenamed("id", "dst"), "dst", "left_anti")
        .groupBy("src", "dst").agg(F.min("t").alias("t"), F.min("d").alias("d"))  # first transfer per pair
        .checkpoint()
    )
    v = e.select(F.col("src").alias("id")).union(e.select(F.col("dst").alias("id"))).distinct()
    g = GraphFrame(v, e)
    flags = []

    # Fan-out / fan-in: distinct counterparties per account per day.
    fo = e.groupBy("src", "d").agg(F.countDistinct("dst").alias("n")) \
        .filter(F.col("n") >= RULES["fan_out_day"])
    flags.append(fo.select(F.col("src").alias("account"), F.lit("FAN-OUT").alias("pattern")))
    fi = e.groupBy("dst", "d").agg(F.countDistinct("src").alias("n")) \
        .filter(F.col("n") >= RULES["fan_in_day"])
    flags.append(fi.select(F.col("dst").alias("account"), F.lit("FAN-IN").alias("pattern")))

    # Gather-scatter: many in, then many out, same day, through one account.
    ins = e.groupBy(F.col("dst").alias("account"), "d").agg(
        F.countDistinct("src").alias("n_in"), F.min("t").alias("first_in"))
    outs = e.groupBy(F.col("src").alias("account"), "d").agg(
        F.countDistinct("dst").alias("n_out"), F.max("t").alias("last_out"))
    gs = ins.join(outs, ["account", "d"]).filter(
        (F.col("n_in") >= RULES["gs_min_side"]) & (F.col("n_out") >= RULES["gs_min_side"])
        & (F.col("last_out") >= F.col("first_in")))
    flags.append(gs.select("account", F.lit("GATHER-SCATTER").alias("pattern")))

    # Scatter-gather: a -> b_i -> c with >= N distinct b_i, time-ordered, within W days.
    sg = (
        g.find("(a)-[e1]->(b); (b)-[e2]->(c)")
        .filter("a.id != c.id AND e2.t >= e1.t")
        .filter(F.col("e2.t") - F.col("e1.t") <= RULES["sg_window_days"] * DAY)
        .groupBy(F.col("a.id").alias("a"), F.col("c.id").alias("c"))
        .agg(F.countDistinct("b.id").alias("mids"))
        .filter(F.col("mids") >= RULES["sg_min_mid"])
    )
    flags.append(sg.select(F.col("a").alias("account"), F.lit("SCATTER-GATHER").alias("pattern")))
    flags.append(sg.select(F.col("c").alias("account"), F.lit("SCATTER-GATHER").alias("pattern")))

    # Cycles of length 2-4 with increasing timestamps.
    W = RULES["cycle_window_days"] * DAY
    c2 = g.find("(a)-[e1]->(b); (b)-[e2]->(a)").filter("e2.t >= e1.t") \
        .filter(F.col("e2.t") - F.col("e1.t") <= W).select(F.array("a.id", "b.id").alias("members"))
    c3 = g.find("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)") \
        .filter("a.id != c.id AND e2.t >= e1.t AND e3.t >= e2.t") \
        .filter(F.col("e3.t") - F.col("e1.t") <= W).select(F.array("a.id", "b.id", "c.id").alias("members"))
    c4 = g.find("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(x); (x)-[e4]->(a)") \
        .filter("a.id != c.id AND b.id != x.id AND e2.t >= e1.t AND e3.t >= e2.t AND e4.t >= e3.t") \
        .filter(F.col("e4.t") - F.col("e1.t") <= W) \
        .select(F.array("a.id", "b.id", "c.id", "x.id").alias("members"))
    cyc = c2.unionByName(c3).unionByName(c4)
    flags.append(cyc.select(F.explode("members").alias("account"), F.lit("CYCLE").alias("pattern")))

    out = flags[0]
    for f in flags[1:]:
        out = out.unionByName(f)
    out = out.distinct().withColumn("run", F.lit(run))
    out.write.mode("overwrite").parquet(f"s3://{BUCKET}/results/bq2/account_flags/run={run}/")
    return spark.read.parquet(f"s3://{BUCKET}/results/bq2/account_flags/run={run}/"), hubs


# ---- Ground truth from the Patterns file --------------------------------------------
text = spark.sparkContext.wholeTextFiles(f"s3://{BUCKET}/landing/{DS}/patterns/").values().collect()[0]
attempts, cur_type, cur_accts = [], None, set()
for line in text.splitlines():
    m = re.match(r"BEGIN LAUNDERING ATTEMPT - ([A-Z\-]+)", line)
    if m:
        cur_type, cur_accts = m.group(1), set()
    elif line.startswith("END LAUNDERING ATTEMPT"):
        attempts.append((len(attempts), cur_type, sorted(cur_accts)))
        cur_type = None
    elif cur_type and line.strip():
        p = line.split(",")
        cur_accts.update([f"{p[1]}|{p[2]}", f"{p[3]}|{p[4]}"])
truth = spark.createDataFrame(attempts, "attempt int, pattern string, accounts array<string>") \
    .select("attempt", "pattern", F.explode("accounts").alias("account"))

# ---- Runs ----------------------------------------------------------------------------
detect(cur.filter("split = 'train'"), "train")
flags_full, hubs_full = detect(cur, "full")

# Validation: an attempt counts as detected if any of its accounts carries a flag of the
# same pattern type ("by_type"), or any flag at all ("any").
j = truth.join(flags_full.select("account", F.col("pattern").alias("flag")), "account", "left")
per_attempt = j.groupBy("attempt", "pattern").agg(
    F.max(F.when(F.col("flag") == F.col("pattern"), 1).otherwise(0)).alias("hit_type"),
    F.max(F.when(F.col("flag").isNotNull(), 1).otherwise(0)).alias("hit_any"),
)
truth_freq = per_attempt.groupBy("pattern").agg(
    F.count("*").alias("attempts"), F.sum("hit_type").alias("detected_same_type"),
    F.sum("hit_any").alias("detected_any_flag"),
).withColumn("recall_same_type", F.col("detected_same_type") / F.col("attempts")) \
 .withColumn("recall_any_flag", F.col("detected_any_flag") / F.col("attempts"))

# Precision proxy: share of flagged accounts that take part in any labelled laundering txn.
laund_accts = cur.filter("is_laundering = 1").select(F.col("from_key").alias("account")) \
    .union(cur.filter("is_laundering = 1").select(F.col("to_key").alias("account"))).distinct() \
    .withColumn("is_laund_acct", F.lit(1))
mined = flags_full.join(laund_accts, "account", "left").groupBy("pattern").agg(
    F.countDistinct("account").alias("flagged_accounts"),
    F.countDistinct(F.when(F.col("is_laund_acct") == 1, F.col("account"))).alias("flagged_laundering_accounts"),
).withColumn("account_precision", F.col("flagged_laundering_accounts") / F.col("flagged_accounts"))

freq = truth_freq.join(mined, "pattern", "full_outer").orderBy(F.desc_nulls_last("attempts"))
freq.coalesce(1).write.mode("overwrite").parquet(f"s3://{BUCKET}/results/bq2/pattern_frequency/")
spark.createDataFrame([(k, float(v)) for k, v in RULES.items()], "rule string, value double") \
    .coalesce(1).write.mode("overwrite").parquet(f"s3://{BUCKET}/results/bq2/rules/")
print("BQ2_HUBS", hubs_full.count())
for r in freq.collect():
    print("BQ2_FREQ", r.asDict())
