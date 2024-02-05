# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
import subprocess
from logging import getLogger

from onetl._util.file import is_file_readable

log = getLogger(__name__)


def kinit_keytab(user: str, keytab: str | os.PathLike) -> None:
    path = is_file_readable(keytab)

    cmd = ["kinit", user, "-k", "-t", os.fspath(path)]
    log.info("|onETL| Executing kerberos auth command: %s", " ".join(cmd))
    subprocess.check_call(cmd)


def kinit_password(user: str, password: str) -> None:
    cmd = ["kinit", user]
    log.info("|onETL| Executing kerberos auth command: %s", " ".join(cmd))

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.PIPE,
    )

    _stdout, stderr = proc.communicate(password.encode("utf-8"))
    exit_code = proc.poll()
    if exit_code:
        raise subprocess.CalledProcessError(exit_code, cmd, output=stderr)


def kinit(user: str, keytab: os.PathLike | None = None, password: str | None = None) -> None:
    if keytab:
        kinit_keytab(user, keytab)
    elif password:
        kinit_password(user, password)
