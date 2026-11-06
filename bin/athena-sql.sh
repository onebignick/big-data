#!/usr/bin/env bash
# Run each ';;'-separated statement of a SQL file in the bda8-aml workgroup.
set -euo pipefail
P="--profile default --region ap-southeast-1"
python3 -c "import sys;[print(s.strip().rstrip(';')+'\0',end='') for s in open(sys.argv[1]).read().split(';;') if s.strip()]" "$1" |
while IFS= read -r -d '' stmt; do
  q=$(aws athena start-query-execution --work-group bda8-aml --query-string "$stmt" $P --query QueryExecutionId --output text)
  while :; do s=$(aws athena get-query-execution --query-execution-id $q $P --query QueryExecution.Status.State --output text)
    case $s in SUCCEEDED) echo "ok: $(echo "$stmt" | grep -o 'v_[a-z_]*' | head -1)"; break;;
      FAILED|CANCELLED) echo "FAILED: $(aws athena get-query-execution --query-execution-id $q $P --query QueryExecution.Status.StateChangeReason --output text)"; exit 1;; esac; sleep 1; done
done
