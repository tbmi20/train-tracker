#!/bin/bash
# Runs once on first container boot (docker-entrypoint-initdb.d), after the
# primary POSTGRES_DB has been created. Adds a second database for Airflow's
# own metadata so one Postgres container can serve both the app and Airflow.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE airflow OWNER "$POSTGRES_USER";
EOSQL
