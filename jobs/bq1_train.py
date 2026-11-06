"""P3 / BQ1: tree models ranking which transaction characteristics relate to laundering.

Train on the train split (01-07 Sep), evaluate on the later test split (08-10 Sep).
Class imbalance: keep all positives, undersample negatives to NEG_FRAC, then weight
positives by the remaining imbalance. Metrics are PR-AUC / precision / recall, never accuracy.
Outputs (results/bq1/): metrics, feature_importance, test_scores; model -> models/bq1_rf/.
"""
import json
import sys

from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET, DS = sys.argv[1], sys.argv[2]
NEG_FRAC, SEEDS = 0.10, [7, 42, 2024]

spark = SparkSession.builder.appName("bda8-bq1-train").getOrCreate()
cur = spark.read.parquet(f"s3://{BUCKET}/curated/{DS}/transactions/")

df = (
    cur.filter(F.col("split") != "tail")
    .withColumn("log_txn_24h", F.log1p("acct_txn_count_24h"))
    .withColumn("log_fan_out", F.log1p("fan_out_count"))
    .withColumn("log_fan_in", F.log1p("fan_in_count"))
    .withColumn("amount_z", F.coalesce("amount_z", F.lit(0.0)))
    .withColumn("label", F.col("is_laundering").cast("double"))
)

CATS = ["payment_format", "payment_currency", "receiving_currency"]
NUMS = [
    "hour_of_day", "day_of_week", "is_cross_bank", "is_cross_currency",
    "is_self_transfer", "amount_z", "log_amount", "log_txn_24h", "log_fan_out", "log_fan_in",
]
FEATURES = [f"{c}_idx" for c in CATS] + NUMS
FEATURE_NAMES = CATS + NUMS  # importances reported against readable names

train_all = df.filter(F.col("split") == "train")
test = df.filter(F.col("split") == "test").cache()
pos = train_all.filter("label = 1")
neg = train_all.filter("label = 0").sample(False, NEG_FRAC, seed=1)
n_pos, n_neg = pos.count(), neg.count()
w_pos = n_neg / n_pos
train = pos.unionByName(neg).withColumn(
    "weight", F.when(F.col("label") == 1, F.lit(w_pos)).otherwise(F.lit(1.0))
).cache()
base_rate = test.filter("label = 1").count() / test.count()

indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in CATS]
assembler = VectorAssembler(inputCols=FEATURES, outputCol="features")
pr_auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR")
roc_auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")


def evaluate(scored):
    s = scored.withColumn("p", vector_to_array("probability")[1]).cache()
    out = {"pr_auc": pr_auc.evaluate(s), "roc_auc": roc_auc.evaluate(s)}
    n_pos_test = s.filter("label = 1").count()
    for k in [0.001, 0.005, 0.01]:  # flag the top k share of test transactions
        thr = s.approxQuantile("p", [1 - k], 0.0001)[0]
        flagged = s.filter(F.col("p") >= thr)
        tp = flagged.filter("label = 1").count()
        nf = flagged.count()
        out[f"top{k:g}"] = {
            "threshold": thr, "flagged": nf, "precision": tp / nf if nf else 0.0,
            "recall": tp / n_pos_test,
        }
    return out, s


results, importances = [], []
best = None
for name, make in [
    ("rf", lambda seed: RandomForestClassifier(numTrees=100, maxDepth=10, maxBins=64,
                                               subsamplingRate=0.8, seed=seed)),
    ("gbt", lambda seed: GBTClassifier(maxIter=60, maxDepth=5, maxBins=64, stepSize=0.1, seed=seed)),
]:
    for seed in SEEDS:
        clf = make(seed).setLabelCol("label").setFeaturesCol("features").setWeightCol("weight")
        model = Pipeline(stages=indexers + [assembler, clf]).fit(train)
        metrics, scored = evaluate(model.transform(test))
        metrics.update({"model": name, "seed": seed})
        results.append(metrics)
        imp = model.stages[-1].featureImportances.toArray().tolist()
        for rank, (f, v) in enumerate(sorted(zip(FEATURE_NAMES, imp), key=lambda x: -x[1]), 1):
            importances.append((name, seed, f, float(v), rank))
        print("BQ1", json.dumps(metrics))
        if name == "rf" and (best is None or metrics["pr_auc"] > best[0]):
            best = (metrics["pr_auc"], model, scored)

summary = {
    "dataset": DS, "train_pos": n_pos, "train_neg_sampled": n_neg, "neg_frac": NEG_FRAC,
    "pos_weight": w_pos, "test_rows": test.count(), "test_base_rate": base_rate,
    "baseline_pr_auc": base_rate, "runs": results,
}
out = f"s3://{BUCKET}/results/bq1"
spark.createDataFrame(
    [(r["model"], r["seed"], r["pr_auc"], r["roc_auc"],
      r["top0.001"]["precision"], r["top0.001"]["recall"],
      r["top0.005"]["precision"], r["top0.005"]["recall"],
      r["top0.01"]["precision"], r["top0.01"]["recall"], base_rate) for r in results],
    "model string, seed int, pr_auc double, roc_auc double, p_at_0_1pct double, r_at_0_1pct double,"
    " p_at_0_5pct double, r_at_0_5pct double, p_at_1pct double, r_at_1pct double, base_rate double",
).coalesce(1).write.mode("overwrite").parquet(f"{out}/metrics/")
spark.createDataFrame(importances, "model string, seed int, feature string, importance double, rank int") \
    .coalesce(1).write.mode("overwrite").parquet(f"{out}/feature_importance/")

_, model, scored = best
model.write().overwrite().save(f"s3://{BUCKET}/models/bq1_rf/")
scored.select("txn_id", "dt", "label", "p").write.mode("overwrite").parquet(f"{out}/test_scores/")
print("BQ1_SUMMARY", json.dumps({k: v for k, v in summary.items() if k != "runs"}))
