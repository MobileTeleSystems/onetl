import pytest

from onetl.connection.db_connection import Oracle, Postgres, Teradata, Hive, MySQL, MSSQL, Clickhouse


class TestDBConnection:

    def test_secure_str_and_repr(self):
        conn = Oracle(host='some_host', login='user', password='passwd')

        assert "password='XXX'" in str(conn)
        assert 'password=' not in repr(conn)

    def test_oracle_driver_and_uri(self):
        conn = Oracle(host='some_host', login='user', password='passwd', extra={'sid': 'PE'})

        assert conn.url == 'jdbc:oracle:thin:@some_host:1521:PE'
        assert Oracle.driver == 'oracle.jdbc.driver.OracleDriver'
        assert Oracle.port == 1521

    def test_oracle_uri_with_service_name(self):
        conn = Oracle(host='some_host', login='user', password='passwd', extra={'service_name': 'DWHLDTS'})

        assert conn.url == 'jdbc:oracle:thin:@//some_host:1521/DWHLDTS'

    def test_oracle_without_extra(self):
        conn = Oracle(host='some_host', login='user', password='passwd')

        with pytest.raises(ValueError):
            conn.url  # noqa: WPS428

    def test_postgres_driver_and_uri(self):
        conn = Postgres(host='some_host', login='user', password='passwd')

        assert conn.url == 'jdbc:postgresql://some_host:5432/default'
        assert Postgres.driver == 'org.postgresql.Driver'
        assert Postgres.port == 5432

    def test_teradata_driver_and_uri(self):
        conn = Teradata(host='some_host', login='user', password='passwd', extra={'TMODE': 'TERA', 'LOGMECH': 'LDAP'})

        assert conn.url == 'jdbc:teradata://some_host/TMODE=TERA,LOGMECH=LDAP,DATABASE=default,DBS_PORT=1025'
        assert Teradata.driver == 'com.teradata.jdbc.TeraDriver'
        assert Teradata.port == 1025

    def test_mysql_driver_and_uri(self):
        conn = MySQL(host='some_host', login='user', password='passwd')

        assert conn.url == 'jdbc:mysql://some_host:3306/default?useUnicode=yes&characterEncoding=UTF-8'
        assert MySQL.driver == 'com.mysql.jdbc.Driver'
        assert MySQL.port == 3306

    def test_mssql_driver_and_uri(self):
        conn = MSSQL(host='some_host', login='user', password='passwd', extra={'characterEncoding': 'UTF-8'})

        assert conn.url == 'jdbc:sqlserver://some_host:1433;databaseName=default;characterEncoding=UTF-8'
        assert MSSQL.driver == 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
        assert MSSQL.port == 1433

    def test_clickhouse_driver_and_uri(self):
        conn = Clickhouse(host='some_host', login='user', password='passwd')

        assert conn.url == 'jdbc:clickhouse://some_host:9000/default'
        assert Clickhouse.driver == 'ru.yandex.clickhouse.ClickHouseDriver'
        assert Clickhouse.port == 9000

    def test_empty_connection(self):
        conn = Hive()
        # TODO: what to do on empty connection??
        assert conn
