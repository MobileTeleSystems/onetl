import ftplib  # NOQA: S402

from ftputil import FTPHost, session as ftp_session

from onetl.connection.file_connection.ftp_connection import FTP


class FTP_TLS_fix(ftplib.FTP_TLS):  # noqa: N801
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
    def get_client(self):
        """
        Returns a FTPS connection object
        """

        session_factory = ftp_session.session_factory(
            base_class=FTP_TLS_fix,
            port=self.port,
            encrypt_data_channel=True,
            debug_level=0,
        )

        password = self.password

        return FTPHost(
            self.host,
            self.login,
            password,
            session_factory=session_factory,
        )
