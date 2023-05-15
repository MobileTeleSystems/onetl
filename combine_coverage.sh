#!/usr/bin/env bash

root_path=$(dirname $(realpath $0))
coverage combine --rcfile=tests/.coveragerc
coverage xml --rcfile=tests/.coveragerc -o $root_path/reports/coverage.xml -i
