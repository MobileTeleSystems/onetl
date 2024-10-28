# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
import subprocess
from logging import getLogger
from threading import Lock

from onetl._util.file import is_file_readable

log = getLogger(__name__)
_kinit_lock = Lock()


def kinit_keytab(user: str, keytab: str | os.PathLike) -> None:
    path = is_file_readable(keytab)

    with _kinit_lock:
        cmd = ["kinit", user, "-k", "-t", os.fspath(path)]
        log.info("|onETL| Executing kerberos auth command: %s", " ".join(cmd))
        subprocess.check_call(cmd)


def kinit_password(user: str, password: str) -> None:
    cmd = ["kinit", user]
    log.info("|onETL| Executing kerberos auth command: %s", " ".join(cmd))

    with _kinit_lock:
        with subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            # do not show user 'Please enter password' banner
            stdout=subprocess.PIPE,
            # do not capture stderr, immediately show all errors to user
        ) as proc:
            proc.communicate(password.encode("utf-8"))
            exit_code = proc.poll()
            if exit_code:
                raise subprocess.CalledProcessError(exit_code, cmd)


def kinit(user: str, keytab: os.PathLike | None = None, password: str | None = None) -> None:
    if keytab:
        kinit_keytab(user, keytab)
    elif password:
        kinit_password(user, password)
