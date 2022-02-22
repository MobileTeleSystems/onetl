#!/usr/bin/env bash

set -e

sed '/^$/d' /opt/project/onetl_local.default.env | sed '/^#/d' | sed 's/^/export /' > ./env
source ./env

/wait-for-it.sh -h "${ONETL_HIVE_CONN_HOST}" -p "${ONETL_HIVE_CONN_PORT}" -t 0
/wait-for-it.sh -h "${ONETL_MYSQL_CONN_HOST}" -p "${ONETL_MYSQL_CONN_PORT}" -t 0
/wait-for-it.sh -h "${ONETL_CH_CONN_HOST}" -p "${ONETL_CH_CONN_PORT}" -t 0
/wait-for-it.sh -h "${ONETL_MSSQL_CONN_HOST}" -p "${ONETL_MSSQL_CONN_PORT}" -t 0
/wait-for-it.sh -h "${ONETL_ORA_CONN_HOST}" -p "${ONETL_ORA_CONN_PORT}" -t 0
/wait-for-it.sh -h "${ONETL_ATLAS_CONN_HOST}" -p "${ONETL_ATLAS_CONN_PORT}" -t 0
atlas_bootstrap --url "http://${ONETL_ATLAS_CONN_HOST}:${ONETL_ATLAS_CONN_PORT}" --username "${ONETL_ATLAS_CONN_USER}" --password "${ONETL_ATLAS_CONN_PASSWORD}" --validate

exec "$@"
