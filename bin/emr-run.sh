#!/usr/bin/env bash
# Launch a transient EMR cluster that runs the given steps file and terminates itself.
# Usage: bin/emr-run.sh <name> <steps.json> [--keep-alive]
# 1 x m5.xlarge primary (on-demand) + 2 x m5.xlarge core (spot), EMR 7.2 / Spark 3.5.
set -euo pipefail
cd "$(dirname "$0")/.."
NAME=$1; STEPS=$2; MODE=${3:-}
P="--profile default --region ap-southeast-1"
BUCKET=$(terraform output -raw lake_bucket)
SUBNET=$(terraform output -raw emr_subnet)
STEPS_JSON=$(sed "s/__BUCKET__/$BUCKET/g" "$STEPS")
TERM_FLAG="--auto-terminate"
[ "$MODE" = "--keep-alive" ] && TERM_FLAG="--no-auto-terminate --auto-termination-policy IdleTimeout=3600"
aws emr create-cluster $P \
  --name "bda8-aml-$NAME" \
  --release-label emr-7.2.0 \
  --applications Name=Spark \
  --service-role bda8-aml-emr-service \
  --ec2-attributes "InstanceProfile=bda8-aml-emr-ec2,SubnetId=$SUBNET" \
  --instance-groups \
     InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge \
     InstanceGroupType=CORE,InstanceCount=2,InstanceType=m5.xlarge,BidPrice=OnDemandPrice \
  --configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"}}]' \
  --log-uri "s3://$BUCKET/logs/emr/" \
  --tags project=bda8-aml owner=onebignick \
  --bootstrap-actions "Path=s3://$BUCKET/code/bootstrap/python-deps.sh,Name=python-deps" \
  --steps "$STEPS_JSON" \
  $TERM_FLAG \
  --query ClusterId --output text
