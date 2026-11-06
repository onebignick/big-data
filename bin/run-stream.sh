#!/usr/bin/env bash
# P5: create the Kafka topic, start the streaming cluster, start the raw replay once both
# consumers are live, then wait for the cluster to finish. Usage: bin/run-stream.sh [day] [rate]
set -euo pipefail
cd "$(dirname "$0")/.."
export AWS_PAGER=""
DAY=${1:-2022-09-08}; RATE=${2:-900}
P="--profile default --region ap-southeast-1"
BUCKET=$(terraform output -raw lake_bucket); TOPIC=$(terraform output -raw sns_topic)
BOOT=$(terraform output -raw kafka_bootstrap); KTOPIC=$(terraform output -raw kafka_topic)
RUN=$(date -u +%Y%m%dT%H%M%S)
mkdir -p .build
invoke() { aws lambda invoke --function-name bda8-aml-replay --cli-binary-format raw-in-base64-out --payload "$1" $P "$2"; }
echo "[$RUN] ensuring Kafka topic $KTOPIC"
invoke '{"action": "create_topic", "partitions": 3}' .build/topic-$RUN.json >/dev/null && cat .build/topic-$RUN.json; echo
mkdir -p .build
sed -e "s/__BUCKET__/$BUCKET/g" -e "s/__RUN__/$RUN/g" -e "s|__TOPIC__|$TOPIC|g" -e "s|__BOOTSTRAP__|$BOOT|g" \
    -e "s/__KTOPIC__/$KTOPIC/g" -e "s/__DAY__/$DAY/g" steps/stream.json > .build/stream-$RUN.json
CID=$(bin/emr-run.sh "stream-$RUN" .build/stream-$RUN.json)
echo "[$RUN] cluster $CID; waiting for the consumers"
until aws s3api head-object --bucket "$BUCKET" --key "results/stream_ready/run=$RUN" $P >/dev/null 2>&1; do
  st=$(aws emr describe-cluster --cluster-id $CID $P --query Cluster.Status.State --output text)
  case $st in TERMINATED*) echo "cluster ended before the consumer was ready"; exit 1;; esac; sleep 15; done
echo "[$RUN] consumers live; replaying raw rows of $DAY at $RATE rows/s"
aws lambda invoke --function-name bda8-aml-replay --invocation-type Event --cli-binary-format raw-in-base64-out \
  --payload "{\"day\": \"${DAY//-//}\", \"rate\": $RATE}" $P /dev/null >/dev/null
aws emr wait cluster-terminated --cluster-id $CID $P || true
echo "[$RUN] done: $(aws emr list-steps --cluster-id $CID $P --query 'Steps[0].Status.State' --output text)"
echo "RUN=$RUN CLUSTER=$CID"
