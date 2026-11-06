#!/bin/bash
# EMR bootstrap: pyspark.ml (and GraphFrames' Python API) need numpy (and pyspark.ml.functions needs pandas/pyarrow); EMR 7 ships none of them.
set -euxo pipefail
sudo python3 -m pip install --quiet "numpy<2" "pandas<2.3" "pyarrow<17" boto3
