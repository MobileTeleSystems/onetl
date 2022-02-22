from __future__ import annotations

import os
from logging import getLogger
from subprocess import check_call

log = getLogger(__name__)


def kinit(user: str, keytab: os.PathLike | None = None, password: str | None = None) -> None:
    if not user or (not keytab and not password):
        raise KerberosAuthError("Not user or keytab and password")

    if keytab:
        log_cmd = cmd = f"kinit {user} -k -t {os.fspath(keytab)}"  # noqa: WPS429
    elif password:
        cmd = f"echo '{password}' | kinit {user}"
        log_cmd = cmd.replace(password, "***")

    log.info(f"|onETL| Executing kerberos auth command: {log_cmd}")
    check_call(cmd, shell=True)  # noqa: S602


class KerberosAuthError(Exception):
    pass  # noqa: WPS420, WPS604
