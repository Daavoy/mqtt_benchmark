#!/usr/bin/env bash
# Run a single-execution benchmark across all supported brokers for one payload size.
# Results land in results/<payload_size>/ and logs in subscriber/logs/aut0_<broker>_<size>/qos0/.
# Add the resulting aut labels to data_analysis/data_analysis copy.ipynb → auts list.
#
# Usage:
#   ./run_broker_comparison.sh              # uses default payload size (1kb)
#   ./run_broker_comparison.sh 10kb         # override payload size
#   ./run_broker_comparison.sh 1kb 3        # payload size + numexecs (default 1)
#   BROKERS="hivemq emqx" ./run_broker_comparison.sh 1kb   # run subset of brokers

set -euo pipefail

PAYLOAD="${1:-1kb}"
NUMEXECS="${2:-1}"
BROKERS="${BROKERS:-hivemq emqx mosquitto nanomq rabbitmq}"
RESULTS_DIR="results/${PAYLOAD}"

mkdir -p "$RESULTS_DIR"

echo "============================================================"
echo "  Broker comparison run"
echo "  Payload : ${PAYLOAD}"
echo "  Execs   : ${NUMEXECS}"
echo "  Brokers : ${BROKERS}"
echo "  Results : ${RESULTS_DIR}/"
echo "============================================================"
echo ""

FAILED=()

for broker in $BROKERS; do
    echo "------------------------------------------------------------"
    echo "  Starting broker: ${broker}  (payload=${PAYLOAD}, execs=${NUMEXECS})"
    echo "------------------------------------------------------------"

    python3 run_load_updated.py \
        --broker "$broker" \
        --payload-size "$PAYLOAD" \
        --qos 0 \
        --numexecs "$NUMEXECS" \
        --sleep-between 5 \
        --stats "${RESULTS_DIR}/${broker}_${PAYLOAD}_qos0.csv" \
        && echo "  [OK] ${broker}" \
        || { echo "  [FAIL] ${broker}"; FAILED+=("$broker"); }

    echo ""
done

echo "============================================================"
echo "  Done. Results in: ${RESULTS_DIR}/"
echo ""
echo "  Add to notebook auts list:"
for broker in $BROKERS; do
    echo "    'aut0_${broker}_${PAYLOAD}'"
done
echo ""
if [ ${#FAILED[@]} -gt 0 ]; then
    echo "  FAILED brokers: ${FAILED[*]}"
else
    echo "  All brokers completed successfully."
fi
echo "============================================================"
