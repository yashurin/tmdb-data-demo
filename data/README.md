# data/

This folder is notes only. Demo data is not stored in git.

| Layer | Where it lives |
|---|---|
| Bronze files | MinIO bucket `tmdb-bronze` (`s3://tmdb-bronze/tmdb/...`) |
| Bronze catalog | Postgres `warehouse.bronze.ingest_objects`, `warehouse.bronze.raw_payloads` |
| Silver tables | Postgres `warehouse.silver.*` (dbt) |
| Gold marts | Postgres `warehouse.gold.*` (dbt) |

Optional MinIO bucket `tmdb-silver` is created for later file exports; v1 gold/silver live in Postgres.
