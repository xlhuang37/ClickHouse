#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LOG="$CLICKHOUSE_TMP/benchmark-open-mode-$CLICKHOUSE_DATABASE.log"
TIMEOUTS="--connect_timeout 60 --handshake_timeout_ms 60000"

echo "iterations"
query_id_prefix="${CLICKHOUSE_DATABASE}_open_mode"
# shellcheck disable=SC2086
$CLICKHOUSE_BENCHMARK $TIMEOUTS --mode open --rate 200 --connections 4 --iterations 30 --delay 0 --query_id_prefix "$query_id_prefix" \
    <<< $'SELECT 1\nSELECT 2' 1>/dev/null 2>"$LOG"
grep -oF 'Queries executed: 30 ' "$LOG" || cat "$LOG" >&2
grep -oF 'Dispatched 30 of 30 arrivals' "$LOG" || cat "$LOG" >&2
grep -oF 'Response time (from the arrival until the result is received):' "$LOG"
grep -oF 'Queue wait (from the arrival until the query is sent):' "$LOG"

# Each arrival picks one of the two queries with equal probability, so both of them are executed.
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT count(), countIf(query = 'SELECT 1') > 0, countIf(query = 'SELECT 2') > 0
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
        AND type = 'QueryFinish' AND query_id LIKE '${query_id_prefix}%'"

echo "timelimit"
# shellcheck disable=SC2086
$CLICKHOUSE_BENCHMARK $TIMEOUTS --mode open --rate 50 --connections 2 --timelimit 1 --delay 0 \
    <<< 'SELECT 1' 1>/dev/null 2>"$LOG"
# The number of arrivals within the time limit is random, but all of them are dispatched and executed.
grep -oE 'Dispatched [0-9]+ of [0-9]+ arrivals' "$LOG" | awk '{ print ($2 == $4 && $2 > 0) }'
grep -c 'Queries executed' "$LOG" || cat "$LOG" >&2

echo "errors"
$CLICKHOUSE_BENCHMARK --mode foo <<< 'SELECT 1' |& grep -c "Unknown value for --mode: 'foo'"
$CLICKHOUSE_BENCHMARK --mode open --iterations 1 <<< 'SELECT 1' |& grep -c "Option --rate is required for --mode=open"
$CLICKHOUSE_BENCHMARK --mode open --rate 0 --iterations 1 <<< 'SELECT 1' |& grep -c "Option --rate must be a positive number"
$CLICKHOUSE_BENCHMARK --mode open --rate 10 --connections 0 --iterations 1 <<< 'SELECT 1' |& grep -c "Option --connections must be positive"
$CLICKHOUSE_BENCHMARK --mode open --rate 10 <<< 'SELECT 1' |& grep -c "requires --iterations or --timelimit"
$CLICKHOUSE_BENCHMARK --mode open --rate 10 --iterations 1 -c 4 <<< 'SELECT 1' |& grep -c "are not supported with --mode=open"
$CLICKHOUSE_BENCHMARK --mode open --rate 10 --iterations 1 --max_concurrency 4 <<< 'SELECT 1' |& grep -c "are not supported with --mode=open"
$CLICKHOUSE_BENCHMARK --rate 10 --iterations 1 <<< 'SELECT 1' |& grep -c "are only supported with --mode=open"
$CLICKHOUSE_BENCHMARK --connections 4 --iterations 1 <<< 'SELECT 1' |& grep -c "are only supported with --mode=open"
$CLICKHOUSE_BENCHMARK --mode open --rate 0.0001 --timelimit 0.001 <<< 'SELECT 1' |& grep -c "No queries arrive within the time limit"

rm -f "$LOG"
