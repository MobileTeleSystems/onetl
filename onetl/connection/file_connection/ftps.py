# SPDX-FileCopyrightText: 2021-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import ftplib  # nosec
import textwrap
from typing import ClassVar

from ftputil import FTPHost
from ftputil import session as ftp_session
from pydantic import Field

try:
    from onetl.connection.file_connection.ftp import FTP, FTPExtra
except (ImportError, NameError) as e:
    raise ImportError(
        textwrap.dedent(
            """
            Cannot import module "ftputil".

            Since onETL v0.7.0 you should install package as follows:
                pip install "onetl[ftps]"

            or
                pip install "onetl[files]"
            """,
        ).strip(),
    ) from e


class TLSfix(ftplib.FTP_TLS):
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


class FTPSExtra(FTPExtra):
    __doc__ = FTPExtra.__doc__.replace("FTP", "FTPS")  # type: ignore[union-attr]


class FTPS(FTP):
    __doc__ = FTP.__doc__.replace("FTP", "FTPS")  # type: ignore[union-attr]

    extra: FTPSExtra = Field(default_factory=FTPSExtra)

    Extra: ClassVar = FTPSExtra

    def _get_client(self) -> FTPHost:
        """
        Returns a FTPS connection object
        """

        extra = self.extra.model_dump(by_alias=True)
        extra.setdefault("debug_level", 0)

        session_factory = ftp_session.session_factory(
            base_class=TLSfix,
            port=self.port,
            encrypt_data_channel=True,
            **extra,
        )

        return FTPHost(
            self.host,
            self.user,
            self.password.get_secret_value() if self.password else None,
            session_factory=session_factory,
        )
