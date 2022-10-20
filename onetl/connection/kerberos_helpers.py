#  Copyright 2022 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

import os
from logging import getLogger
from shlex import quote
from subprocess import check_call

log = getLogger(__name__)


def kinit(user: str, keytab: os.PathLike | None = None, password: str | None = None) -> None:
    if not user or (not keytab and not password):
        raise KerberosAuthError("Not user or keytab and password")

    if keytab:
        log_cmd = cmd = f"kinit {user} -k -t {os.fspath(keytab)}"  # noqa: WPS429
    elif password:
        quoted_password = quote(password)
        cmd = f"echo {quoted_password} | kinit {user}"
        log_cmd = cmd.replace(quoted_password, "***")

    log.info(f"|onETL| Executing kerberos auth command: {log_cmd}")
    check_call(cmd, shell=True)  # noqa: S602


class KerberosAuthError(Exception):
    pass  # noqa: WPS420, WPS604
