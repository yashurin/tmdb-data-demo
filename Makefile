.PHONY: up down down-v logs ps trigger-bronze unpause test config demo

up:
	docker compose up -d --build

down:
	docker compose down

down-v:
	docker compose down -v

logs:
	docker compose logs -f --tail=200

ps:
	docker compose ps

trigger-bronze:
	docker compose exec airflow-scheduler airflow dags trigger dag_tmdb_bronze

unpause:
	docker compose exec airflow-scheduler airflow dags unpause dag_tmdb_bronze
	docker compose exec airflow-scheduler airflow dags unpause dag_tmdb_silver
	docker compose exec airflow-scheduler airflow dags unpause dag_tmdb_gold

test:
	PYTHONPATH=src python3 -m pytest tests -q

config:
	docker compose config >/dev/null && echo "compose config OK"

demo:
	./scripts/demo.sh
