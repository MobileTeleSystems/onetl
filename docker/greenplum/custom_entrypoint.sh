#!/bin/bash

set -e

function run_custom_scripts {
    SCRIPTS_ROOT="${1}";

    # Check whether parameter has been passed on
    if [ -z "${SCRIPTS_ROOT}" ]; then
        echo "No SCRIPTS_ROOT passed on, no scripts will be run.";
        return;
    fi;

    # Execute custom provided files (only if directory exists and has files in it)
    if [ -d "${SCRIPTS_ROOT}" ] && [ -n "$(ls -A "${SCRIPTS_ROOT}")" ]; then
        echo -e "\nENTRYPOINT: Executing user-defined scripts..."
        run_custom_scripts_recursive "${SCRIPTS_ROOT}"
        echo -e "ENTRYPOINT: DONE: Executing user-defined scripts.\n"
    fi;
}

function run_custom_scripts_recursive {
    local f;
    for f in "${1}"/*; do
        case "${f}" in
            *.sh)
                if [ -x "${f}" ]; then
                    echo -e "\nENTRYPOINT: running ${f} ..."; run_script_as_gpadmin "${f}"; echo "ENTRYPOINT: DONE: running ${f}"
                else
                    echo -e "\nENTRYPOINT: sourcing ${f} ..."; run_command_as_gpadmin "${f}" echo "ENTRYPOINT: DONE: sourcing ${f}"
                fi;
                ;;

            *.sql)
                echo -e "\nENTRYPOINT: running ${f} ..."; run_command_as_gpadmin psql -f "${f}"; echo "ENTRYPOINT: DONE: running ${f}"
                ;;

            *)
                if [ -d "${f}" ]; then
                    echo -e "\nENTRYPOINT: descending into ${f} ..."; run_custom_scripts_recursive "${f}"; echo "ENTRYPOINT: DONE: descending into ${f}"
                else
                    echo -e "\nENTRYPOINT: ignoring ${f}"
                fi;
                ;;
        esac
        echo "";
    done
}

function run_script_as_gpadmin() {
    su -w POSTGRESQL_DATABASE -w POSTGRESQL_USERNAME -w POSTGRESQL_PASSWORD - gpadmin "${1}"
}

function run_command_as_gpadmin() {
    su -w POSTGRESQL_DATABASE -w POSTGRESQL_USERNAME -w POSTGRESQL_PASSWORD - gpadmin bash -c "${1}"
}


function start_gpadmin() {
    /etc/init.d/ssh start;
    sleep 1;
    run_command_as_gpadmin "gpstart -a"
}

function stop_gpadmin() {
    run_command_as_gpadmin "gpstop -a -M fast"
}

function output_logs() {
    tail -f `ls /data/{master,coordinator}/gpsne-1/{pg_log,log}/gpdb-* | tail -n1` &
}

function main() {
    start_gpadmin
    trap "stop_gpadmin" INT TERM
    output_logs

    run_custom_scripts "/container-entrypoint-initdb.d"

    # required by trap
    while [ "$END" == '' ]; do
        sleep 1
    done
}

main
