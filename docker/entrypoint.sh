#!/usr/bin/env bash

set -e

sed '/^$/d' /opt/project/onetl_local.default.env | sed '/^#/d' | sed 's/^/export /' > ./env
source ./env

/wait-for-it.sh -h "${ONETL_PG_CONN_HOST}" -p "${ONETL_PG_CONN_HOST}" -t 60
/wait-for-it.sh -h "${ONETL_MYSQL_CONN_HOST}" -p "${ONETL_MYSQL_CONN_PORT}" -t 60
/wait-for-it.sh -h "${ONETL_CH_CONN_HOST}" -p "${ONETL_CH_CONN_PORT}" -t 60
/wait-for-it.sh -h "${ONETL_MSSQL_CONN_HOST}" -p "${ONETL_MSSQL_CONN_PORT}" -t 60
/wait-for-it.sh -h "${ONETL_GP_CONN_HOST}" -p "${ONETL_GP_CONN_PORT}" -t 60
/wait-for-it.sh -h "${ONETL_ORA_CONN_HOST}" -p "${ONETL_ORA_CONN_PORT}" -t 60
/wait-for-it.sh -h "${ONETL_HIVE_CONN_HOST}" -p "${ONETL_HIVE_CONN_PORT}" -t 60
/wait-for-it.sh -h "${ONETL_MINIO_HOST}" -p "${ONETL_MINIO_PORT}" -t 60
/wait-for-it.sh -h "${ONETL_WEBDAV_HOST}" -p "${ONETL_WEBDAV_PORT}" -t 60
/wait-for-it.sh -h "${ONETL_MONGO_HOST}" -p "${ONETL_MONGO_PORT}" -t 60
python3 -m pip install -e tests/libs/dummy

exec "$@"
