#!/bin/sh
# usage: measure.sh <phase> <project_dir> <out_dir> <times>
# Runs StockWebSocketServiceConcurrencyTest <times> times and appends [RESULT] lines to CSV + raw log.
PHASE=$1; DIR=$2; OUT=$3; N=$4
mkdir -p "$OUT"; CSV=$OUT/$PHASE.csv; LOG=$OUT/$PHASE.log
echo "phase,run,timestamp,scenario,repeats,failed_runs,detail" > "$CSV"
: > "$LOG"
for i in $(seq 1 "$N"); do
  ( cd "$DIR" && sh ./gradlew test --tests '*StockWebSocketServiceConcurrencyTest' --rerun-tasks -q >/dev/null 2>&1 )
  TS=$(date '+%Y-%m-%dT%H:%M:%S%z')
  grep -ho '\[RESULT\][^<]*' "$DIR"/build/test-results/test/TEST-*Concurrency*.xml | sed 's/[[:space:]]*$//' | while read -r line; do
    echo "run=$i $TS $line" >> "$LOG"
    sc=$(echo "$line" | sed -E 's/.*scenario=([^ ]+).*/\1/')
    rp=$(echo "$line" | sed -E 's/.* runs=([0-9]+).*/\1/')
    fr=$(echo "$line" | sed -E 's/.*failedRuns=([0-9]+).*/\1/')
    dt=$(echo "$line" | sed -E 's/.*failedRuns=[0-9]+ ?//')
    echo "$PHASE,$i,$TS,$sc,$rp,$fr,\"$dt\"" >> "$CSV"
  done
done
