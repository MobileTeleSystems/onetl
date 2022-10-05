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
        Host of ftp source. For example: ``10.226.174.94``
    port : int, default: ``21``
        Port of ftp source
    user : str
        User, which have access to the file source. For example: ``sa0000sphretltest``
    password : str, default: ``None``
        Password for file source connection

    Examples
    --------

    FTP file connection initialization

    .. code:: python

        from onetl.connection import FTPS

        ftps = FTPS(
            host="10.226.174.94",
            user="sa0000sphretltest",
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
