# SPDX-FileCopyrightText: 2022-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import os
import subprocess
from logging import getLogger
from threading import Lock

from onetl._util.file import readable_local_file
from onetl.impl.local_path import LocalPath

log = getLogger(__name__)
_kinit_lock = Lock()


def kinit_keytab(user: str, keytab: LocalPath) -> None:
    with _kinit_lock:
        cmd = ["kinit", user, "-k", "-t", os.fspath(readable_local_file(keytab))]
        log.info("|onETL| Executing kerberos auth command: %s", " ".join(cmd))
        subprocess.check_call(cmd)  # noqa: S603


def kinit_password(user: str, password: str) -> None:
    cmd = ["kinit", user]
    log.info("|onETL| Executing kerberos auth command: %s", " ".join(cmd))

    with (
        _kinit_lock,
        subprocess.Popen(  # noqa: S603
            cmd,
            stdin=subprocess.PIPE,
            # do not show user 'Please enter password' banner
            stdout=subprocess.PIPE,
            # do not capture stderr, immediately show all errors to user
        ) as proc,
    ):
        proc.communicate(password.encode("utf-8"))
        exit_code = proc.poll()
        if exit_code:
            raise subprocess.CalledProcessError(exit_code, cmd)


def kinit(user: str, keytab: LocalPath | None = None, password: str | None = None) -> None:
    if keytab:
        kinit_keytab(user, keytab)
    elif password:
        kinit_password(user, password)
