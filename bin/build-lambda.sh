#!/usr/bin/env bash
# Package the replay Lambda: replay.py + vendored kafka-python (pure Python) for python3.12.
set -euo pipefail
cd "$(dirname "$0")/.."
rm -rf .build/replay-pkg && mkdir -p .build/replay-pkg
"${PIP:-app/.venv/bin/python}" -m pip install --quiet --target .build/replay-pkg --python-version 3.12 --only-binary=:all: \
  --implementation cp --platform manylinux2014_x86_64 "kafka-python==2.2.15"
cp lambda/replay.py .build/replay-pkg/
find .build/replay-pkg -name '__pycache__' -prune -exec rm -rf {} +
echo "built .build/replay-pkg ($(du -sh .build/replay-pkg | cut -f1))"
