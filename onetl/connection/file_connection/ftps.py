# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import ftplib  # noqa: S402  # nosec
import textwrap

from ftputil import FTPHost
from ftputil import session as ftp_session

try:
    from onetl.connection.file_connection.ftp import FTP
except (ImportError, NameError) as e:
    raise ImportError(
        textwrap.dedent(
            """
            Cannot import module "ftputil".

            Since onETL v0.7.0 you should install package as follows:
                pip install onetl[ftps]

            or
                pip install onetl[files]
            """,
        ).strip(),
    ) from e


class TLSfix(ftplib.FTP_TLS):  # noqa: N801
    """
    Fix for python 3.6+
    https://stackoverflow.com/questions/14659154/ftpes-session-reuse-required
    """

    def ntransfercmd(self, cmd, rest=None):
        conn, size = ftplib.FTP.ntransfercmd(self, cmd, rest)  # noqa: S321  # nosec
        if self._prot_p:
            conn = self.context.wrap_socket(
                conn,
                server_hostname=self.host,
                session=self.sock.session,
            )  # this is the fix
        return conn, size


class FTPS(FTP):
    """FTPS file connection. |support_hooks|

    Based on `FTPUtil library <https://pypi.org/project/ftputil/>`_.

    .. warning::

        Since onETL v0.7.0 to use FTPS connector you should install package as follows:

        .. code:: bash

            pip install onetl[ftps]

            # or
            pip install onetl[files]

        See :ref:`install-files` installation instruction for more details.

    .. versionadded:: 0.1.0

    Parameters
    ----------
    host : str
        Host of FTPS source. For example: ``ftps.domain.com``

    port : int, default: ``21``
        Port of FTPS source

    user : str, default: ``None``
        User, which have access to the file source. For example: ``someuser``.

        ``None`` means that the user is anonymous.

    password : str, default: ``None``
        Password for file source connection.

        ``None`` means that the user is anonymous.

    Examples
    --------

    Create and check FTPS connection:

    .. code:: python

        from onetl.connection import FTPS

        ftps = FTPS(
            host="ftps.domain.com",
            user="someuser",
            password="*****",
        ).check()
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
            self.user,
            self.password.get_secret_value() if self.password else None,
            session_factory=session_factory,
        )
