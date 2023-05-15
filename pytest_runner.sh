#!/usr/bin/env bash

_term() {
    echo "Caught SIGTERM signal!"
    exit 255
}

trap _term SIGTERM SIGINT

if [[ "x$CI" == "xtrue" ]]; then
    root_path=$(dirname $(realpath $0))
    python_version=$(python -c 'import sys; print("{0}.{1}".format(*sys.version_info))')
    coverage run --rcfile=tests/.coveragerc -m pytest --junitxml=$root_path/reports/junit/python${python_version}.xml "$@"
else
    pytest "$@"
fi

ret=$?
if [[ "x$ret" == "x5" ]]; then
    echo "No tests collected. Exiting with 0 (instead of 5)."
    exit 0
fi
exit "$ret"
