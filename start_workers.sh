#!/bin/bash
n=${1:-1}
PROJECT_DIR="/Users/ndaidcnd/Desktop/deidentification/deIdentification"
VENV_PATH="air_deid"

echo "--- Phase 1: Cleaning Sessions & Ports ---"
screen -ls | grep "deid_" | awk '{print $1}' | xargs -I{} screen -X -S {} quit
lsof -ti:8000 | xargs kill -9 2>/dev/null 
sleep 2

echo "--- Phase 2: Starting Server ---"
# Logs output to 'server_debug.log' so we can read it if it crashes
screen -dmS "SRV_deid" bash -lc "conda activate '$VENV_PATH' && cd '$PROJECT_DIR' && python manage.py runserver > server_debug.log 2>&1"
sleep 3

echo "--- Phase 3: Starting $n Workers ---"
for ((i = 1; i <= n; i++)); do
  screen -dmS "WRK_deid_$i" bash -lc "conda activate '$VENV_PATH' && cd '$PROJECT_DIR' && python manage.py start_worker"
done

echo "Check: screen -ls"
screen -ls | grep "deid"