#!/bin/bash
# Creates Airflow metadata DB + warehouse DB with medallion schemas.
# Runs only on first Postgres volume init.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
SELECT 'CREATE DATABASE warehouse'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'warehouse')\gexec
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "warehouse" <<SQL
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
GRANT ALL ON SCHEMA bronze TO ${POSTGRES_USER};
GRANT ALL ON SCHEMA silver TO ${POSTGRES_USER};
GRANT ALL ON SCHEMA gold TO ${POSTGRES_USER};
ALTER DATABASE warehouse SET search_path TO gold, silver, bronze, public;
SQL
