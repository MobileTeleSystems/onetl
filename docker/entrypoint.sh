#!/usr/bin/env bash


# TODO: remove later, if not needed
# The entrypoint Defaults
: "${WAIT:=0}"

# Wait for DB or sibling containers (like, a webserver waiting for scheduler...)
while [ ${WAIT} -ne 0 ]; do
    echo "System is waiting for smth to start; $((WAIT-=1)) remaining attempts" >&2
    sleep 1
done

# Mount local files (tests, etc) into /opt/project, so the directory will be
# consistently cleaned up across test runs.
if [ -d /opt/project ]; then
    echo "========================/opt/project Cleanup========================"
    find /opt/project -type f -name '*.py[co]' -delete -o -type d -name __pycache__ -delete
fi

exec "$@"
