#!/usr/bin/env bash

# allow create files and directories
mkdir -p /share/folder
chmod 0777 /share/folder
/entrypoint.sh -u "1000:1000:onetl:onetl:awd123fd1" -s "SmbShare:/share/folder:rw:onetl"
