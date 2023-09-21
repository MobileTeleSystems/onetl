#!/usr/bin/env bash

# allow create files and directories
chmod 777 /share/folder
/entrypoint.sh -u "1000:1000:onetl:onetl:awd123fd1" -s "SmbShare:/share/folder:rw:onetl"
