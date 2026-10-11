#!/bin/bash

[ $# -ne 5 ] && echo "Usage: $0 <TPS> <STEP> <ACCELERATION> <DURATION> <SCRIPT>" && exit 1

TPS="$1"
STEP="$2"
ACCELERATION="$3"
DURATION="$4"
SCRIPT="$5"

GOGC=50 \
OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION="base2_exponential_bucket_histogram" \
OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE="cumulative" \
K6_OTEL_METRIC_PREFIX="k6_" \
K6_OTEL_EXPORTER_TYPE="http" \
K6_OTEL_HTTP_EXPORTER_ENDPOINT="1:8429" \
K6_OTEL_HTTP_EXPORTER_URL_PATH="/opentelemetry/v1/metrics" \
K6_OTEL_HTTP_EXPORTER_INSECURE="true" \
./k6 run \
  -e MY_TPS="$TPS" \
  -e MY_STEP="$STEP" \
  -e MY_ACCELERATION="$ACCELERATION" \
  -e MY_DURATION="$DURATION" \
  -e MY_IP="$(hostname -I)" \
  -o experimental-opentelemetry \
  --summary-mode=disabled \
  --no-thresholds \
  --address localhost:0 \
  "/home/$USER/Genk6/js/$SCRIPT/generator.js" &
