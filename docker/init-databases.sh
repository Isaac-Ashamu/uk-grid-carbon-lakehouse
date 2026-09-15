#!/usr/bin/env bash
# Runs once on first Postgres start: one instance, two databases
# (Airflow metadata and the gridcarbon serving schema).
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
    CREATE DATABASE airflow;
    CREATE DATABASE gridcarbon;
SQL
