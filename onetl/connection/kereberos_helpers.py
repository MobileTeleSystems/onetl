import os
import uuid

from datetime import datetime, timedelta
from logging import getLogger

log = getLogger(__name__)


# TODO:(@mivasil6) make separate package
class KerberosMixin:
    ticket_life_time = timedelta(hours=8)

    def __init__(self):
        self.krb_tmp_dir = None
        self.ticket_life_start = datetime.now()

    def kinit(self, user, keytab=None, password=None):
        if not user or (not keytab and not password):
            raise KerberosAuthException("Not user or keytab and password")

        self._prep_krb_args()
        if keytab:
            cmd = f"kinit {user} -k -t {keytab}"
        else:
            cmd = f'echo "{password}" | kinit {user}'

        log.debug(f'exec_cmd: {cmd.replace(password, "***")}')
        return check_call(cmd, shell=True)  # NOQA S602

    def ticket_expiring(self):
        return (datetime.utcnow() - self.ticket_life_start) > self.ticket_life_time

    def _prep_krb_args(self):
        tmp_dir = f"/tmp/tmp_dir_{uuid.uuid4()}"  # NOSONAR
        krb5ccname = f"DIR:{tmp_dir}"
        if not self.krb_tmp_dir:
            os.mkdir(tmp_dir)
            self.krb_tmp_dir = tmp_dir
            os.environ["KRB5CCNAME"] = krb5ccname


class KerberosAuthException(Exception):
    pass  # noqa: WPS420, WPS604
