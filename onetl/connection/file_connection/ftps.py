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

import ftplib  # NOQA: S402

from ftputil import FTPHost
from ftputil import session as ftp_session

from onetl.connection.file_connection.ftp import FTP


class TLSfix(ftplib.FTP_TLS):  # noqa: N801
    """
    Fix for python 3.6+
    https://stackoverflow.com/questions/14659154/ftpes-session-reuse-required
    """

    def ntransfercmd(self, cmd, rest=None):
        conn, size = ftplib.FTP.ntransfercmd(self, cmd, rest)  # noqa: S321
        if self._prot_p:
            conn = self.context.wrap_socket(
                conn,
                server_hostname=self.host,
                session=self.sock.session,
            )  # this is the fix
        return conn, size


class FTPS(FTP):
    """Class for FTPS file connection.

    Parameters
    ----------
    host : str
        Host of FTPS source. For example: ``ftps.domain.com``

    port : int, default: ``21``
        Port of FTPS source

    user : str
        User, which have access to the file source. For example: ``someuser``

    password : str, default: ``None``
        Password for file source connection

    Examples
    --------

    FTPS file connection initialization

    .. code:: python

        from onetl.connection import FTPS

        ftps = FTPS(
            host="ftps.domain.com",
            user="someuser",
            password="*****",
        )
    """

    def _get_client(self) -> FTPHost:
        """
        Returns a FTPS connection object
        """

        session_factory = ftp_session.session_factory(
            base_class=TLSfix,
            port=self.port,
            encrypt_data_channel=True,
            debug_level=0,
        )

        return FTPHost(
            self.host,
            self.user or "",
            self.password.get_secret_value() if self.password else "None",
            session_factory=session_factory,
        )
