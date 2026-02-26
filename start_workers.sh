#!/bin/bash
# ---------------------------------------------------------------------------
# start_workers.sh  [num_workers] [conda_env]
#
# Usage:
#   ./start_workers.sh              # 1 worker, env = $CONDA_ENV or 'air_deid'
#   ./start_workers.sh 4            # 4 workers, default env
#   ./start_workers.sh 4 my_env     # 4 workers, conda env named 'my_env'
#   ./start_workers.sh 8 air_deid   # 8 workers  ← for heavy parallel workloads
#
# What it does:
#   1. Kills any existing deid screen sessions + frees port 8000
#   2. Starts the Django server in a screen session (SRV_deid)
#   3. Starts N workers in separate screen sessions (WRK_deid_1 … WRK_deid_N)
#
# For parallel large-table processing, set num_workers ≥ PARALLEL_TASKS_COUNT
# from run.ipynb (default 4) so all ranged sub-tasks run simultaneously.
# ---------------------------------------------------------------------------

n=${1:-1}
VENV_PATH=${2:-${CONDA_ENV:-air_deid}}

# ---------------------------------------------------------------------------
# Derive paths automatically — no hardcoded user directories.
# ---------------------------------------------------------------------------
# Resolve the directory that contains this script (works even if called from
# a different working directory).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# The Django package root sits one level below the repo root (in deIdentification/).
PROJECT_DIR="$SCRIPT_DIR/deIdentification"

echo "PROJECT_DIR : $PROJECT_DIR"
echo "VENV_PATH   : $VENV_PATH"
echo "Workers     : $n"
echo ""

# ---------------------------------------------------------------------------
# Phase 1: Clean up any leftover sessions and free the server port
# ---------------------------------------------------------------------------
echo "--- Phase 1: Cleaning sessions & ports ---"
screen -ls | grep "deid_" | awk '{print $1}' | xargs -I{} screen -X -S {} quit 2>/dev/null
lsof -ti:8000 | xargs kill -9 2>/dev/null
sleep 2

# ---------------------------------------------------------------------------
# Phase 2: Start the Django development server
# ---------------------------------------------------------------------------
echo "--- Phase 2: Starting server ---"
screen -dmS "SRV_deid" bash -lc "conda activate '$VENV_PATH' && cd '$PROJECT_DIR' && python manage.py runserver > server_debug.log 2>&1"
sleep 3

# ---------------------------------------------------------------------------
# Phase 3: Start N workers
# Each worker independently picks up pending tasks from PostgreSQL using
# SELECT FOR UPDATE SKIP LOCKED so they never process the same task twice.
#
# Rule of thumb:
#   - Set n = PARALLEL_TASKS_COUNT (from run.ipynb) to fully utilise parallel
#     large-table splits.
#   - For 1000 small tables (single task each) + 100 large tables (4 tasks
#     each), running 8-16 workers gives good throughput.
# ---------------------------------------------------------------------------
echo "--- Phase 3: Starting $n worker(s) ---"
for ((i = 1; i <= n; i++)); do
  screen -dmS "WRK_deid_$i" bash -lc "conda activate '$VENV_PATH' && cd '$PROJECT_DIR' && python manage.py start_worker"
  echo "  Started WRK_deid_$i"
done

echo ""
echo "--- Active sessions ---"
screen -ls | grep "deid"
