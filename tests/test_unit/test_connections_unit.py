import pytest

from onetl.connection.db_connection import Oracle, Postgres, Teradata, Hive, MySQL, MSSQL, Clickhouse
from onetl.connection.file_connection import FTP, FTPS, HDFS, SFTP, Samba, FileConnection


class TestDBConnection:
    def test_secure_str_and_repr(self):
        conn = Oracle(host="some_host", user="user", password="passwd")

        assert "password=" not in str(conn)
        assert "password=" not in repr(conn)

    def test_oracle_driver_and_uri(self):
        conn = Oracle(host="some_host", user="user", password="passwd", sid="PE")

        assert conn.url == "jdbc:oracle:thin:@some_host:1521:PE"
        assert Oracle.driver == "oracle.jdbc.driver.OracleDriver"
        assert Oracle.package == "com.oracle:ojdbc7:12.1.0.2"
        assert Oracle.port == 1521

    def test_oracle_uri_with_service_name(self):
        conn = Oracle(host="some_host", user="user", password="passwd", service_name="DWHLDTS")

        assert conn.url == "jdbc:oracle:thin:@//some_host:1521/DWHLDTS"

    def test_oracle_without_extra(self):
        conn = Oracle(host="some_host", user="user", password="passwd")

        with pytest.raises(ValueError):
            conn.url  # noqa: WPS428

    def test_postgres_driver_and_uri(self):
        conn = Postgres(host="some_host", user="user", password="passwd")

        assert conn.url == "jdbc:postgresql://some_host:5432/default"
        assert Postgres.driver == "org.postgresql.Driver"
        assert Postgres.package == "org.postgresql:postgresql:42.2.5"
        assert Postgres.port == 5432

    def test_teradata_driver_and_uri(self):
        conn = Teradata(host="some_host", user="user", password="passwd", extra={"TMODE": "TERA", "LOGMECH": "LDAP"})

        assert conn.url == "jdbc:teradata://some_host/TMODE=TERA,LOGMECH=LDAP,DATABASE=default,DBS_PORT=1025"
        assert Teradata.driver == "com.teradata.jdbc.TeraDriver"
        assert Teradata.package == "com.teradata.jdbc:terajdbc4:16.20.00.10"
        assert Teradata.port == 1025

    def test_mysql_driver_and_uri(self):
        conn = MySQL(host="some_host", user="user", password="passwd")

        assert conn.url == "jdbc:mysql://some_host:3306/default?useUnicode=yes&characterEncoding=UTF-8"
        assert MySQL.driver == "com.mysql.jdbc.Driver"
        assert MySQL.package == "mysql:mysql-connector-java:5.1.8"
        assert MySQL.port == 3306

    def test_mssql_driver_and_uri(self):
        conn = MSSQL(host="some_host", user="user", password="passwd", extra={"characterEncoding": "UTF-8"})

        assert conn.url == "jdbc:sqlserver://some_host:1433;databaseName=default;characterEncoding=UTF-8"
        assert MSSQL.driver == "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        assert MSSQL.package == "com.microsoft.sqlserver:mssql-jdbc:7.2.0.jre8"
        assert MSSQL.port == 1433

    def test_clickhouse_driver_and_uri(self):
        conn = Clickhouse(host="some_host", user="user", password="passwd")

        assert conn.url == "jdbc:clickhouse://some_host:9000/default"
        assert Clickhouse.driver == "ru.yandex.clickhouse.ClickHouseDriver"
        assert Clickhouse.package == "ru.yandex.clickhouse:clickhouse-jdbc:0.2.4"
        assert Clickhouse.port == 9000

    def test_empty_connection(self):
        conn = Hive()
        # TODO: what to do on empty connection??
        assert conn

    def test_jdbc_params_creator(self):
        jdbc_options = {
            "user": "user_name",
            "lowerBound": 10,
            "upperBound": 1000,
            "url": "connection_url",
            "driver": "some_driver",
            "partitionColumn": "some_column",
            "numPartitions": 20,
            "fetchsize": 1000,
        }

        conn = Postgres(host="some_host", user="user", password="passwd")

        jdbc_options = conn.jdbc_params_creator(jdbc_options=jdbc_options, table="test_table")

        assert jdbc_options == {
            "lowerBound": 10,
            "upperBound": 1000,
            "url": "jdbc:postgresql://some_host:5432/default",
            "column": "some_column",
            "numPartitions": 20,
            "properties": {
                "user": "user",
                "driver": "org.postgresql.Driver",
                "fetchsize": 1000,
                "password": "passwd",
            },
        }

    def test_get_sql_without_extra_params(self):
        connection = Oracle()
        table_sql = connection.get_sql_query(table="default.test")

        assert table_sql == "SELECT * FROM default.test"

    def test_dbreader_table_sql_with_extra_params(self):
        connection = Oracle()
        table_sql = connection.get_sql_query(
            table="default.test",
            sql_hint="NOWAIT",
            columns="d_id, d_name, d_age",
            sql_where="d_id > 100",
        )
        expected_sql = "SELECT /*+ NOWAIT */ d_id, d_name, d_age FROM default.test WHERE d_id > 100"

        assert table_sql == expected_sql

    def test_hive_read_table_without_spark(self):
        hive = Hive()
        with pytest.raises(ValueError):
            hive.read_table({"test": "test"}, "test_table")

    def test_hive_connection_uri(self):
        hive = Hive(host="some_host", user="user", password="passwd", extra={"param": "value"})

        assert hive.url == "hiveserver2://user:passwd@some_host:10000?param=value"


class TestFileConnections:
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

    def test_sftp_connection(self):
        sftp = SFTP(host="some_host", user="some_user", password="pwd")
        assert isinstance(sftp, FileConnection)
        assert sftp.port == 22

    def test_samba_connection(self):
        samba = Samba(host="some_host", user="some_user", password="pwd")
        assert isinstance(samba, FileConnection)
        assert samba.port == 445

    # TODO: decide on sharepoint
