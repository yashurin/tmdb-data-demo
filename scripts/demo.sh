#!/usr/bin/env bash
# Start the local platform and print the 15-minute walkthrough commands.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ ! -f .env ]]; then
  echo "Copy .env.example to .env and set TMDB_API_KEY, then re-run."
  echo "  cp .env.example .env"
  exit 1
fi

if grep -Eq '^TMDB_API_KEY=(replace-me)?[[:space:]]*$' .env; then
  echo "Set a real TMDB_API_KEY in .env before triggering bronze."
  echo "Compose will still start; extract will fail until the key is set."
fi

echo "Starting stack (first run builds the Airflow image)..."
docker compose up -d --build

echo "Waiting for Airflow webserver health on :8080 ..."
ok=0
for _ in $(seq 1 90); do
  if curl -sf http://localhost:8080/health >/dev/null 2>&1; then
    ok=1
    break
  fi
  sleep 4
done

if [[ "$ok" -ne 1 ]]; then
  echo "Airflow did not become healthy in time. Check: docker compose logs airflow-webserver airflow-init"
  exit 1
fi

cat <<'EOF'

Airflow is up:  http://localhost:8080  (admin / admin unless you changed .env)
MinIO console:  http://localhost:9001  (minioadmin / minioadmin)
Metabase:       http://localhost:3000  (create admin, then connect to host=postgres db=warehouse)
Postgres:       localhost:5432         (db warehouse, user from .env)

Trigger bronze (silver + gold follow via Datasets):

  docker compose exec airflow-scheduler airflow dags trigger dag_tmdb_bronze

Or: make trigger-bronze

Then wait for dag_tmdb_gold success and run a query from analytics/example_questions.sql in Metabase.
EOF
