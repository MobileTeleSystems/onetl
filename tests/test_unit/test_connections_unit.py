from unittest.mock import Mock

import pytest

from onetl.connection import (
    FTP,
    FTPS,
    HDFS,
    MSSQL,
    SFTP,
    Clickhouse,
    FileConnection,
    Hive,
    MySQL,
    Oracle,
    Postgres,
    Samba,
    Teradata,
)
from onetl.core import DBWriter


class TestDBConnection:
    spark = Mock()

    def test_secure_str_and_repr(self):
        conn = Oracle(host="some_host", user="user", password="passwd", sid="sid", spark=self.spark)

        assert "password=" not in str(conn)
        assert "password=" not in repr(conn)

    def test_empty_connection(self):
        with pytest.raises(TypeError):
            Hive()  # noqa: F841

    @pytest.mark.parametrize(
        "connection,options",
        [
            (
                Oracle(host="some_host", user="user", password="passwd", sid="sid", spark=spark),
                Hive.Options(bucket_by=(10, "hwm_int"), sort_by="hwm_int"),
            ),
            (
                Hive(spark=spark),
                Oracle.Options(fetchsize=1000, truncate=True, partition_column="id_int"),
            ),
        ],
        ids=["JDBC connection with Hive options.", "Hive connection with JDBC options."],
    )
    def test_inappropriate_connection_and_options_types(self, connection, options):
        with pytest.raises(TypeError):
            DBWriter(
                connection=connection,
                table="onetl.some_table",
                options=options,
            )

    def test_wrong_mode_option(self):
        oracle = Oracle(host="some_host", user="user", password="passwd", sid="sid", spark=self.spark)

        with pytest.raises(ValueError):
            DBWriter(
                connection=oracle,
                table="onetl.some_table",
                options=Oracle.Options(mode="wrong_mode"),  # wrong mode
            )

    def test_get_sql_without_extra_params(self):
        conn = Postgres(host="some_host", user="user", password="passwd", database="abc", spark=self.spark)

        table_sql = conn.get_sql_query("default.test", columns=["*"])

        assert table_sql == "SELECT * FROM default.test"

    def test_get_sql_query_with_extra_params(self):
        conn = Postgres(host="some_host", user="user", password="passwd", database="abc", spark=self.spark)

        table_sql = conn.get_sql_query(
            table="default.test",
            hint="NOWAIT",
            columns=["d_id", "d_name", "d_age"],
            where="d_id > 100",
        )
        expected_sql = "SELECT /*+ NOWAIT */ d_id, d_name, d_age FROM default.test WHERE d_id > 100"

        assert table_sql == expected_sql

    @pytest.mark.parametrize("hint, real_hint", [(None, ""), ("TEMP", " /*+ TEMP */")])
    @pytest.mark.parametrize("columns, real_columns", [(None, "*"), (["a", "b", "c"], "a, b, c")])
    @pytest.mark.parametrize("where, real_where", [(None, ""), ("a = b", " WHERE a = b")])
    @pytest.mark.parametrize("cte_hint, real_cte_hint", [(None, ""), ("TEMP", " /*+ TEMP */")])
    @pytest.mark.parametrize(
        "cte_columns, real_cte_columns",
        [(None, "*"), (["d_id", "d_name", "d_age"], "d_id, d_name, d_age")],
    )
    @pytest.mark.parametrize("cte_where, real_cte_where", [(None, ""), ("d_id > 100", " WHERE d_id > 100")])
    def test_get_sql_query_cte(
        self,
        hint,
        real_hint,
        columns,
        real_columns,
        where,
        real_where,
        cte_hint,
        real_cte_hint,
        cte_columns,
        real_cte_columns,
        cte_where,
        real_cte_where,
    ):
        conn = Postgres(host="some_host", user="user", password="passwd", database="abc", spark=self.spark)

        table_sql = conn.get_sql_query_cte(
            table="default.test",
            hint=hint,
            columns=columns,
            where=where,
            cte_hint=cte_hint,
            cte_columns=cte_columns,
            cte_where=cte_where,
        )

        expected_sql = (
            f"WITH cte AS (SELECT{real_cte_hint} {real_cte_columns} FROM default.test{real_cte_where}) "
            f"SELECT{real_hint} {real_columns} FROM cte{real_where}"
        )

        assert table_sql == expected_sql


class TestHiveOptions:
    @pytest.mark.parametrize(
        "sort_by",
        ["id_int", ["id_int", "hwm_int"]],
        ids=["sortBy as string.", "sortBy as List."],
    )
    def test_sort_by_without_bucket_by(self, sort_by):
        with pytest.raises(ValueError):
            Hive.Options(sortBy=sort_by)

    @pytest.mark.parametrize(
        "options",
        [
            # disallowed modes
            {"mode": "error"},
            {"mode": "ignore"},
            # options user only for table creation
            {"compression": True},
            {"partitionBy": "id_int"},
            {"bucketBy": (10, "id_int")},
            {"bucketBy": (10, "id_int"), "sortBy": "id_int"},
        ],
    )
    def test_insert_into_wrong_options(self, options):
        with pytest.raises(ValueError):
            Hive.Options(insert_into=True, **options)


class TestJDBCConnection:
    spark = Mock()

    def test_jdbc_connection_without_host_and_credentials(self):
        with pytest.raises(TypeError):
            Postgres(spark=self.spark)  # noqa: F841

    def test_jdbc_default_fetchsize(self):
        conn = Oracle(host="some_host", user="user", password="passwd", sid="PE", spark=self.spark)
        options = conn.Options()

        assert options.fetchsize == 100000

    # ORACLE

    def test_oracle_driver_and_uri(self):
        conn = Oracle(host="some_host", user="user", password="passwd", sid="PE", spark=self.spark)

        assert conn.jdbc_url == "jdbc:oracle:thin:@some_host:1521:PE"
        assert Oracle.driver == "oracle.jdbc.driver.OracleDriver"
        assert Oracle.package == "com.oracle:ojdbc7:12.1.0.2"
        assert Oracle.port == 1521

    def test_oracle_uri_with_service_name(self):
        conn = Oracle(host="some_host", user="user", password="passwd", service_name="DWHLDTS", spark=self.spark)

        assert conn.jdbc_url == "jdbc:oracle:thin:@//some_host:1521/DWHLDTS"

    def test_oracle_without_set_sid_service_name(self):
        with pytest.raises(ValueError):
            Oracle(host="some_host", user="user", password="passwd", spark=self.spark)

    def test_oracle_set_sid_and_service_name(self):
        with pytest.raises(ValueError):
            Oracle(
                host="some_host",
                user="user",
                password="passwd",
                spark=self.spark,
                service_name="service",
                sid="sid",
            )

    # POSTGRES

    def test_postgres_without_database_error(self):
        with pytest.raises(ValueError):
            Postgres(host="some_host", user="user", password="passwd", spark=self.spark)

    def test_postgres_driver_and_uri(self):
        conn = Postgres(host="some_host", user="user", password="passwd", database="default", spark=self.spark)

        assert conn.jdbc_url == "jdbc:postgresql://some_host:5432/default"
        assert Postgres.driver == "org.postgresql.Driver"
        assert Postgres.package == "org.postgresql:postgresql:42.2.5"
        assert Postgres.port == 5432

    # TERADATA

    def test_teradata_driver_and_uri(self):
        conn = Teradata(
            host="some_host",
            user="user",
            password="passwd",
            extra={"TMODE": "TERA", "LOGMECH": "LDAP"},
            spark=self.spark,
            database="default",
        )

        assert conn.jdbc_url == "jdbc:teradata://some_host/TMODE=TERA,LOGMECH=LDAP,DATABASE=default,DBS_PORT=1025"
        assert Teradata.driver == "com.teradata.jdbc.TeraDriver"
        assert Teradata.package == "com.teradata.jdbc:terajdbc4:17.10.00.25"
        assert Teradata.port == 1025

    def test_teradata_without_database(self):
        conn = Teradata(
            host="some_host",
            user="user",
            password="passwd",
            extra={"TMODE": "TERA", "LOGMECH": "LDAP"},
            spark=self.spark,
        )

        assert conn.jdbc_url == "jdbc:teradata://some_host/TMODE=TERA,LOGMECH=LDAP,DBS_PORT=1025"

    # MYSQL

    def test_mysql_driver_and_uri(self):
        conn = MySQL(host="some_host", user="user", database="default", password="passwd", spark=self.spark)

        assert conn.jdbc_url == "jdbc:mysql://some_host:3306/default?useUnicode=yes&characterEncoding=UTF-8"
        assert MySQL.driver == "com.mysql.jdbc.Driver"
        assert MySQL.package == "mysql:mysql-connector-java:8.0.26"
        assert MySQL.port == 3306

    def test_mysql_without_database(self):
        conn = MySQL(
            host="some_host",
            user="user",
            password="passwd",
            spark=self.spark,
            extra={"allowMultiQueries": "true", "requireSSL": "true"},
        )

        assert conn.jdbc_url == (
            "jdbc:mysql://some_host:3306?allowMultiQueries=true&requireSSL=true"
            "&useUnicode=yes&characterEncoding=UTF-8"
        )

    # MSSQL

    def test_mssql_without_database_error(self):
        with pytest.raises(ValueError):
            MSSQL(host="some_host", user="user", password="passwd", spark=self.spark)

    def test_mssql_driver_and_uri(self):
        conn = MSSQL(
            host="some_host",
            user="user",
            password="passwd",
            extra={"characterEncoding": "UTF-8"},
            spark=self.spark,
            database="default",
        )

        assert conn.jdbc_url == "jdbc:sqlserver://some_host:1433;databaseName=default;characterEncoding=UTF-8"
        assert MSSQL.driver == "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        assert MSSQL.package == "com.microsoft.sqlserver:mssql-jdbc:7.2.0.jre8"
        assert MSSQL.port == 1433

    @pytest.mark.parametrize("hint, real_hint", [(None, ""), ("TEMP", " /*+ TEMP */")])
    @pytest.mark.parametrize("columns, real_columns", [(None, "*"), (["a", "b", "c"], "a, b, c")])
    @pytest.mark.parametrize("where, real_where", [(None, ""), ("a = b", " WHERE a = b")])
    @pytest.mark.parametrize("cte_hint, real_cte_hint", [(None, ""), ("TEMP", " /*+ TEMP */")])
    @pytest.mark.parametrize(
        "cte_columns, real_cte_columns",
        [(None, "*"), (["d_id", "d_name", "d_age"], "d_id, d_name, d_age")],
    )
    @pytest.mark.parametrize("cte_where, real_cte_where", [(None, ""), ("d_id > 100", " WHERE d_id > 100")])
    def test_mssql_get_sql_query_cte(
        self,
        hint,
        real_hint,
        columns,
        real_columns,
        where,
        real_where,
        cte_hint,
        real_cte_hint,
        cte_columns,
        real_cte_columns,
        cte_where,
        real_cte_where,
    ):
        conn = MSSQL(
            host="some_host",
            user="user",
            password="passwd",
            spark=self.spark,
            database="default",
        )

        table_sql = conn.get_sql_query_cte(
            table="default.test",
            hint=hint,
            columns=columns,
            where=where,
            cte_hint=cte_hint,
            cte_columns=cte_columns,
            cte_where=cte_where,
        )

        expected_sql = (
            f"SELECT{real_hint} {real_columns} FROM (SELECT{real_cte_hint} {real_cte_columns} "
            f"FROM default.test{real_cte_where}) as cte{real_where}"
        )

        assert table_sql == expected_sql

    # CLICKHOUSE

    def test_clickhouse_driver_and_uri(self):
        conn = Clickhouse(host="some_host", user="user", database="default", password="passwd", spark=self.spark)

        assert conn.jdbc_url == "jdbc:clickhouse://some_host:8123/default"
        assert Clickhouse.driver == "ru.yandex.clickhouse.ClickHouseDriver"
        assert Clickhouse.package == "ru.yandex.clickhouse:clickhouse-jdbc:0.3.0"
        assert Clickhouse.port == 8123

    def test_clickhouse_without_database(self):
        conn = Clickhouse(
            host="some_host",
            user="user",
            password="passwd",
            extra={"socket_timeout": "120000", "query": "SELECT%201%3B"},
            spark=self.spark,
        )

        assert conn.jdbc_url == "jdbc:clickhouse://some_host:8123?socket_timeout=120000&query=SELECT%201%3B"


class TestFileConnections:
    def test_file_connection_without_host_and_user(self):
        with pytest.raises(TypeError):
            FTP()  # noqa: F841

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
            HDFS(host="hive2", port=50070, user="usr", password="pwd", keytab="/path/to/keytab")  # noqa: F841

    def test_sftp_connection(self):
        sftp = SFTP(host="some_host", user="some_user", password="pwd")
        assert isinstance(sftp, FileConnection)
        assert sftp.port == 22

    def test_samba_connection(self):
        samba = Samba(host="some_host", user="some_user", password="pwd")
        assert isinstance(samba, FileConnection)
        assert samba.port == 445
