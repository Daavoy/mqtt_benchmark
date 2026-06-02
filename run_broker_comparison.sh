#!/usr/bin/env bash
# Run a benchmark across all supported brokers for one payload size.
# Results land in results/<payload_size>[_<suffix>]/ and logs in
# subscriber/logs/aut0_<broker>_<size>[_<suffix>]/qos0/.
#
# Usage:
#   ./run_broker_comparison.sh                      # default: 1kb, 10 execs
#   ./run_broker_comparison.sh 10kb                 # override payload size
#   ./run_broker_comparison.sh 1kb 3                # payload size + numexecs
#   BROKERS="hivemq emqx" ./run_broker_comparison.sh 1kb 1
#   SUFFIX=test ./run_broker_comparison.sh 1kb 1    # isolated test run

set -euo pipefail

PAYLOAD="${1:-1kb}"
NUMEXECS="${2:-10}"
BROKERS="${BROKERS:-hivemq emqx mosquitto nanomq rabbitmq}"
SUFFIX="${SUFFIX:-}"
SLEEP_BETWEEN="${SLEEP_BETWEEN:-60}"

# Results directory — suffixed when SUFFIX is set so test runs don't pollute production data.
if [ -n "$SUFFIX" ]; then
    RESULTS_DIR="results/${PAYLOAD}_${SUFFIX}"
else
    RESULTS_DIR="results/${PAYLOAD}"
fi

mkdir -p "$RESULTS_DIR"

# Fix ownership on all directories that Docker containers write to.
# On a fresh clone these don't exist yet; on re-runs Docker may have created
# them as root, making subsequent non-root writes fail.
for dir in \
    monitoring/prometheus_data \
    publisher/data/synthetic \
    publisher/logs \
    subscriber/logs \
    results; do
    mkdir -p "$dir"
done
chmod -R 777 monitoring/prometheus_data publisher/data/synthetic \
    publisher/logs subscriber/logs results \
    2>/dev/null || \
sudo chmod -R 777 monitoring/prometheus_data publisher/data/synthetic \
    publisher/logs subscriber/logs results

echo "============================================================"
echo "  Broker comparison run"
echo "  Payload       : ${PAYLOAD}"
echo "  Execs         : ${NUMEXECS}"
echo "  Sleep between : ${SLEEP_BETWEEN}s"
echo "  Brokers       : ${BROKERS}"
echo "  Results       : ${RESULTS_DIR}/"
if [ -n "$SUFFIX" ]; then
    echo "  Suffix        : ${SUFFIX}  (isolated from production data)"
fi
echo "============================================================"
echo ""

# ── Ensure monitoring stack is running before the first broker starts ────────
echo "Checking monitoring stack (Prometheus)..."
if ! docker compose -f docker-compose-monitoring.yml ps --services --filter status=running 2>/dev/null | grep -q prometheus; then
    echo "  Prometheus not running — starting monitoring stack..."
    docker compose -f docker-compose-monitoring.yml up -d
    echo "  Waiting 15 s for Prometheus to become ready..."
    sleep 15
fi

# Confirm Prometheus API is reachable before committing to a full run.
if ! curl -sf http://localhost:9090/-/ready > /dev/null 2>&1; then
    echo "  [warn] Prometheus API not ready at localhost:9090."
    echo "         Prometheus metrics will be missing from CSVs."
fi
echo ""

FAILED=()

for broker in $BROKERS; do
    echo "------------------------------------------------------------"
    echo "  Starting broker: ${broker}  (payload=${PAYLOAD}, execs=${NUMEXECS})"
    echo "------------------------------------------------------------"

    SUFFIX_ARG=()
    if [ -n "$SUFFIX" ]; then
        SUFFIX_ARG=(--run-suffix "$SUFFIX")
    fi

    python3 run_load_updated.py \
        --broker "$broker" \
        --payload-size "$PAYLOAD" \
        --qos 0 \
        --numexecs "$NUMEXECS" \
        --sleep-between "$SLEEP_BETWEEN" \
        --stats "${RESULTS_DIR}/${broker}_${PAYLOAD}_qos0.csv" \
        "${SUFFIX_ARG[@]}" \
        && echo "  [OK] ${broker}" \
        || { echo "  [FAIL] ${broker}"; FAILED+=("$broker"); }

    echo ""
done

echo "============================================================"
echo "  Done. Results in: ${RESULTS_DIR}/"
echo ""
echo "  Add to notebook auts list:"
for broker in $BROKERS; do
    if [ -n "$SUFFIX" ]; then
        echo "    'aut0_${broker}_${PAYLOAD}_${SUFFIX}'"
    else
        echo "    'aut0_${broker}_${PAYLOAD}'"
    fi
done
echo ""
if [ ${#FAILED[@]} -gt 0 ]; then
    echo "  FAILED brokers: ${FAILED[*]}"
else
    echo "  All brokers completed successfully."
fi
echo "============================================================"
