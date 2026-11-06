# big-data: AML pipeline on AWS (BDA Group 8)

IBM Transactions for Anti-Money Laundering (Kaggle, HI-Small) on AWS, provisioned with Terraform
in `ap-southeast-1`.

- **BQ1:** which transaction characteristics are most associated with laundering?
- **BQ2:** can suspicious activity be flagged in near real time?

## Architecture

```
                     ┌─────────── Kafka (Amazon MSK, topic aml-raw-txn) ◄── Lambda replay: RAW CSV rows in time order
                     │
                     ▼
        Spark Structured Streaming on EMR (jobs/stream.py)   ── reads raw rows straight off Kafka
          ├─ clean in-stream (string IDs, explicit ts format, de-dup) + features (24 h / 6 h account windows)
          ├─ BQ1 live: running laundering rates by format / currency / flags / hour  → results/bq1_stream/rates/
          ├─ BQ2: BQ1 model score + graph rules → results/stream_scored/, results/flags/, SNS alert
          └─ archive every raw row unchanged → landing/hi-small/stream/dt=…   (into the lake)

S3 data lake (bda8-aml-lake-<account>)
  landing/  raw Kaggle CSV + Patterns file (+ the stream's raw archive)
  curated/  Glue ETL (jobs/curate.py): cleaned rows + features, Parquet partitioned by dt
  results/  model metrics, feature importance, pattern tables, flags (Athena via Glue Data Catalog)

Batch (transient EMR clusters, bin/emr-run.sh)
  BQ1  jobs/bq1_train.py  Random Forest + GBT (MLlib), time split, PR-AUC
  BQ2  jobs/bq2_graph.py  GraphFrames pattern mining → rules + flagged accounts used by the stream

Serving: Athena views (sql/bq1_views.sql) + Streamlit (app/), hosted on EC2 behind an IP allowlist + password
```

### Where the BQ2 graph job's data comes from

| Step | Location | Produced by |
|---|---|---|
| Raw transactions | `s3://…/landing/hi-small/transactions/HI-Small_Trans.csv` | Kaggle download |
| Cleaned edges | `s3://…/curated/hi-small/transactions/dt=YYYY-MM-DD/` (Parquet) | Glue job `bda8-aml-curate` |
| Graph | vertices = accounts (`bank\|account`), edges = transactions; **train period 1–7 Sep 2022 (3.73M edges)** for the rules; the full period for evaluation | `jobs/bq2_graph.py` on EMR |
| Ground truth | `s3://…/landing/hi-small/patterns/HI-Small_Patterns.txt` (370 injected attempts) | Kaggle download |
| Output | `results/bq2/account_flags/run=train/` → loaded by the stream as `pattern_flag`; `results/bq2/pattern_frequency/` | `jobs/bq2_graph.py` |

## Layout

| Path | What |
|---|---|
| `p0_guardrails.tf` | $50/month budget with email alerts |
| `p1_lake.tf` | bucket, Glue database, landing table, Athena workgroup (10 GiB scan cap), Glue role |
| `p2_curate.tf` | Glue ETL job + crawler |
| `p3_emr.tf` | EMR roles, job/bootstrap upload |
| `p5_stream.tf` | MSK Kafka, replay Lambda, SNS, raw-archive table |
| `p6_serve.tf`, `p6_host.tf` | results crawler; Streamlit host |
| `jobs/` | Spark / Glue jobs |
| `steps/` | EMR step definitions |
| `bin/` | `emr-run.sh` (transient cluster), `run-stream.sh` (one replay), `build-lambda.sh`, `athena-sql.sh` |
| `iam/friends-readonly.json` | read-only console policy for group members |

## Run

```bash
cp terraform.tfvars.example terraform.tfvars   # fill in
bin/build-lambda.sh                            # vendors kafka-python for the replay Lambda
terraform init && terraform apply
# upload HI-Small_Trans.csv / HI-Small_Patterns.txt to landing/hi-small/{transactions,patterns}/
aws glue start-job-run --job-name bda8-aml-curate        # P2
bin/emr-run.sh batch steps/batch.json                   # P3 (BQ1 models) + P4 (BQ2 graph)
bin/athena-sql.sh sql/bq1_views.sql                     # BQ1 rate views
bin/run-stream.sh 2022-09-08 900                        # P5: replay one day through Kafka
aws glue start-crawler --name bda8-aml-results          # P6: catalog results for Athena / Streamlit
```

Tear down: `terraform destroy` (empty the bucket first). Kafka costs ~$0.09/h while it exists.
