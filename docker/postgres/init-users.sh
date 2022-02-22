#!/bin/sh

set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

    CREATE ROLE hive2 WITH LOGIN PASSWORD 'hive2';
    CREATE DATABASE "metastore2_db" OWNER = hive2;
    GRANT ALL PRIVILEGES ON DATABASE "metastore2_db" TO hive2;

EOSQL
