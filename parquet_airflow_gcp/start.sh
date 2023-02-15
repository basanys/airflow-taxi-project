set -e
docker compose build
docker compose up airflow-init
docker compose up
