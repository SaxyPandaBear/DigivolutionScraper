#!/bin/bash
# Entrypoint for a cron-launched, run-to-completion container (e.g. Railway's
# Cron Schedule + Custom Start Command), as an alternative to keeping
# `airflow standalone` running indefinitely.
#
# Both the scheduler AND the api-server are required here, even though
# nothing external needs to reach this container: in Airflow 3's Task
# Execution API architecture, each task runs in a supervisor process that
# talks to the api-server over HTTP to fetch context and report state - the
# scheduler cannot execute tasks on its own. (Confirmed the hard way: a
# scheduler-only version of this script left every task retrying against
# `httpx.ConnectError` with no api-server to answer it.)
#
# Runs a one-shot DAG parse, starts both required components, triggers the
# DAG via the CLI (never the UI/REST trigger path, which stamps an explicit
# logical_date and can produce a run with zero schedulable task instances if
# it predates the DAG's start_date), waits for a terminal state, then exits
# with a matching status code so Railway's cron run reporting reflects
# success/failure correctly.
set -euo pipefail

DAG_ID="digivolution_scraper"
MAX_WAIT_SECONDS=1800 # generous margin over the ~2.5min observed run time
POLL_INTERVAL_SECONDS=10

airflow db migrate

echo "Parsing DAGs..."
airflow dag-processor --num-runs 1

airflow api-server &
API_SERVER_PID=$!
airflow scheduler &
SCHEDULER_PID=$!
cleanup() {
    kill "$API_SERVER_PID" "$SCHEDULER_PID" 2>/dev/null || true
    wait "$API_SERVER_PID" "$SCHEDULER_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "Waiting for the api-server to accept requests..."
for _ in $(seq 1 30); do
    curl -sf "http://localhost:8080/api/v2/monitor/health" >/dev/null 2>&1 && break
    sleep 2
done

echo "Waiting for the DAG to be registered..."
for _ in $(seq 1 30); do
    airflow dags list 2>/dev/null | grep -q "^${DAG_ID} " && break
    sleep 2
done

# a deterministic, explicit run_id so the poll below can query this exact
# run - `dags list-runs | tail -1` is not reliable once the DAG has more than
# one row of history, which a recurring cron job always will after its first
# invocation
RUN_ID="cron__$(date -u +%Y-%m-%dT%H:%M:%S)"

echo "Triggering ${DAG_ID} (run_id=${RUN_ID})..."
airflow dags trigger --run-id "$RUN_ID" "$DAG_ID"

echo "Waiting for the run to finish..."
elapsed=0
state="queued"
while [[ "$state" != "success" && "$state" != "failed" ]]; do
    if (( elapsed >= MAX_WAIT_SECONDS )); then
        echo "Timed out after ${MAX_WAIT_SECONDS}s waiting for ${DAG_ID} (last state: $state)"
        exit 1
    fi
    sleep "$POLL_INTERVAL_SECONDS"
    elapsed=$((elapsed + POLL_INTERVAL_SECONDS))
    # airflow's CLI prints setup/plugin noise to stdout ahead of the actual
    # result, so only the last line is the real state
    state=$(airflow dags state "$DAG_ID" "$RUN_ID" 2>/dev/null | tail -1)
    echo "  [${elapsed}s] state=$state"
done

echo "Run finished: $state"
[[ "$state" == "success" ]]
