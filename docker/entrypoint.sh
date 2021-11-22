#!/usr/bin/env bash


./wait-for-it.sh -h "${ONETL_HIVE_CONN_HOST}" -p "${ONETL_HIVE_CONN_PORT}" -t 0;

exec "$@"
