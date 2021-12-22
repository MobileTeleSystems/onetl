import pytest
from unittest.mock import Mock

from onetl.connection.db_connection import Oracle, Postgres, Teradata, Hive, MySQL, MSSQL, Clickhouse
from onetl.connection.file_connection import FTP, FTPS, HDFS, SFTP, Samba, FileConnection
from onetl.connection.connection_helpers import get_sql_query
from onetl.writer.db_writer import DBWriter


class TestDBConnection:
    spark = Mock()

    def test_secure_str_and_repr(self):
        conn = Oracle(host="some_host", user="user", password="passwd", spark=self.spark)

        assert "password=" not in str(conn)
        assert "password=" not in repr(conn)

    def test_empty_connection(self):
        with pytest.raises(TypeError):
            conn = Hive()  # noqa: F841

    @pytest.mark.parametrize(
        "connection,options",
        [
            (
                Oracle(host="some_host", user="user", password="passwd", spark=spark),
                Hive.Options(sort_by="hwm_int"),
            ),
            (
                Hive(spark=spark),
                Oracle.Options(fetchsize=1000, truncate=True, partition_column="id_int"),
            ),
        ],
        ids=["JDBC connection with Hive options.", "Hive connection with JDBC options."],
    )
    def test_inappropriate_connection_and_options_types(self, connection, options):
        with pytest.raises(ValueError):
            DBWriter(
                connection=connection,
                table="onetl.some_table",
                options=options,
            )

    def test_wrong_mode_option(self):
        oracle = Oracle(host="some_host", user="user", password="passwd", spark=self.spark)

        with pytest.raises(ValueError):
            DBWriter(
                connection=oracle,
                table="onetl.some_table",
                options=Oracle.Options(mode="wrong_mode"),  # wrong mode
            )


class TestJDBCConnection:
    spark = Mock()

    def test_jdbc_connection_without_host_and_credentials(self):
        with pytest.raises(TypeError):
            conn = Postgres(spark=self.spark)  # noqa: F841

    def test_oracle_driver_and_uri(self):
        conn = Oracle(host="some_host", user="user", password="passwd", sid="PE", spark=self.spark)

        assert conn.jdbc_url == "jdbc:oracle:thin:@some_host:1521:PE"
        assert Oracle.driver == "oracle.jdbc.driver.OracleDriver"
        assert Oracle.package == "com.oracle:ojdbc7:12.1.0.2"
        assert Oracle.port == 1521

    def test_oracle_uri_with_service_name(self):
        conn = Oracle(host="some_host", user="user", password="passwd", service_name="DWHLDTS", spark=self.spark)

        assert conn.jdbc_url == "jdbc:oracle:thin:@//some_host:1521/DWHLDTS"

    def test_oracle_without_extra(self):
        conn = Oracle(host="some_host", user="user", password="passwd", spark=self.spark)

        with pytest.raises(ValueError):
            conn.jdbc_url  # noqa: WPS428

    def test_postgres_driver_and_uri(self):
        conn = Postgres(host="some_host", user="user", password="passwd", spark=self.spark)

        assert conn.jdbc_url == "jdbc:postgresql://some_host:5432/default"
        assert Postgres.driver == "org.postgresql.Driver"
        assert Postgres.package == "org.postgresql:postgresql:42.2.5"
        assert Postgres.port == 5432

    def test_teradata_driver_and_uri(self):
        conn = Teradata(
            host="some_host",
            user="user",
            password="passwd",
            extra={"TMODE": "TERA", "LOGMECH": "LDAP"},
            spark=self.spark,
        )

        assert conn.jdbc_url == "jdbc:teradata://some_host/TMODE=TERA,LOGMECH=LDAP,DATABASE=default,DBS_PORT=1025"
        assert Teradata.driver == "com.teradata.jdbc.TeraDriver"
        assert Teradata.package == "com.teradata.jdbc:terajdbc4:16.20.00.10"
        assert Teradata.port == 1025

    def test_mysql_driver_and_uri(self):
        conn = MySQL(host="some_host", user="user", password="passwd", spark=self.spark)

        assert conn.jdbc_url == "jdbc:mysql://some_host:3306/default?useUnicode=yes&characterEncoding=UTF-8"
        assert MySQL.driver == "com.mysql.jdbc.Driver"
        assert MySQL.package == "mysql:mysql-connector-java:8.0.26"
        assert MySQL.port == 3306

    def test_mssql_driver_and_uri(self):
        conn = MSSQL(
            host="some_host",
            user="user",
            password="passwd",
            extra={"characterEncoding": "UTF-8"},
            spark=self.spark,
        )

        assert conn.jdbc_url == "jdbc:sqlserver://some_host:1433;databaseName=default;characterEncoding=UTF-8"
        assert MSSQL.driver == "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        assert MSSQL.package == "com.microsoft.sqlserver:mssql-jdbc:7.2.0.jre8"
        assert MSSQL.port == 1433

    def test_clickhouse_driver_and_uri(self):
        conn = Clickhouse(host="some_host", user="user", password="passwd", spark=self.spark)

        assert conn.jdbc_url == "jdbc:clickhouse://some_host:8123/default"
        assert Clickhouse.driver == "ru.yandex.clickhouse.ClickHouseDriver"
        assert Clickhouse.package == "ru.yandex.clickhouse:clickhouse-jdbc:0.3.0"
        assert Clickhouse.port == 8123


class TestConnectionHelpers:
    def test_get_sql_without_extra_params(self):

        table_sql = get_sql_query(table="default.test")

        assert table_sql == "SELECT * FROM default.test"

    def test_dbreader_table_sql_with_extra_params(self):

        table_sql = get_sql_query(
            table="default.test",
            hint="NOWAIT",
            columns="d_id, d_name, d_age",
            where="d_id > 100",
        )
        expected_sql = "SELECT /*+ NOWAIT */ d_id, d_name, d_age FROM default.test WHERE d_id > 100"

        assert table_sql == expected_sql


class TestFileConnections:
    def test_wrong_source_check(self):
        ftp = FTP(host="some_host", user="some_user", password="pwd")
        with pytest.raises(RuntimeError):
            ftp.check()

    def test_file_connection_without_host_and_user(self):
        with pytest.raises(TypeError):
            conn = FTP()  # noqa: F841

    def test_ftp_connection(self):
        ftp = FTP(host="some_host", user="some_user", password="pwd")
        assert isinstance(ftp, FileConnection)
        assert ftp.port == 21

    def test_ftps_connection(self):
        ftps = FTPS(host="some_host", user="some_user", password="pwd")
        assert isinstance(ftps, FileConnection)
        assert ftps.port == 21

    def test_hdfs_connection(self):
        hdfs = HDFS(host="some_host", user="some_user", password="pwd")
        assert isinstance(hdfs, FileConnection)
        assert hdfs.port == 50070

    def test_hdfs_connection_with_password_and_keytab(self):
        with pytest.raises(ValueError):
            hdfs = HDFS(host="hive2", port=50070, user="usr", password="pwd", keytab="/path/to/keytab")  # noqa: F841

    def test_sftp_connection(self):
        sftp = SFTP(host="some_host", user="some_user", password="pwd")
        assert isinstance(sftp, FileConnection)
        assert sftp.port == 22

    def test_samba_connection(self):
        samba = Samba(host="some_host", user="some_user", password="pwd")
        assert isinstance(samba, FileConnection)
        assert samba.port == 445
