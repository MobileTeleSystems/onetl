import logging

import pytest

from onetl.connection import MSSQL, Clickhouse, Hive, MySQL, Oracle, Postgres


def test_clickhouse_connection_check(spark, processing, caplog):
    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    with caplog.at_level(logging.INFO):
        assert clickhouse.check() == clickhouse

    assert "Connection is available" in caplog.text


def test_clickhouse_wrong_connection_check(spark):
    clickhouse = Clickhouse(host="host", user="some_user", password="pwd", database="abc", spark=spark)
    with pytest.raises(RuntimeError):
        clickhouse.check()


def test_hive_check(spark, caplog):
    hive = Hive(spark=spark)
    with caplog.at_level(logging.INFO):
        assert hive.check() == hive
    assert "Connection is available" in caplog.text


def test_mssql_connection_check(spark, processing, caplog):
    mssql = MSSQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra={"trustServerCertificate": "true"},
    )

    with caplog.at_level(logging.INFO):
        assert mssql.check() == mssql

    assert "Connection is available" in caplog.text


def test_mssql_wrong_connection_check(spark):
    mssql = MSSQL(
        host="host",
        user="some_user",
        password="pwd",
        database="abc",
        spark=spark,
        extra={"trustServerCertificate": "true"},
    )
    with pytest.raises(RuntimeError):
        mssql.check()


def test_mysql_connection_check(spark, processing, caplog):
    mysql = MySQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    with caplog.at_level(logging.INFO):
        assert mysql.check() == mysql

    assert "Connection is available" in caplog.text


def test_mysql_wrong_connection_check(spark):
    mysql = MySQL(host="host", user="some_user", password="pwd", database="abc", spark=spark)

    with pytest.raises(RuntimeError):
        mysql.check()


def test_oracle_connection_check(spark, processing, caplog):
    oracle = Oracle(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        sid=processing.sid,
    )

    with caplog.at_level(logging.INFO):
        assert oracle.check() == oracle

    assert "Connection is available" in caplog.text


def test_oracle_wrong_connection_check(spark):
    oracle = Oracle(host="host", user="some_user", password="pwd", database="abc", sid="cde", spark=spark)

    with pytest.raises(RuntimeError):
        oracle.check()


def test_postgres_connection_check(spark, processing, caplog):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    with caplog.at_level(logging.INFO):
        assert postgres.check() == postgres

    assert "Connection is available" in caplog.text


def test_postgres_wrong_connection_check(spark):
    postgres = Postgres(host="host", database="db", user="some_user", password="pwd", spark=spark)

    with pytest.raises(RuntimeError):
        postgres.check()
