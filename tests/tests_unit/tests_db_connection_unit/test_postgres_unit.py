from unittest.mock import Mock

import pytest

from onetl.connection import Postgres

spark = Mock()


def test_postgres_without_database_error():
    with pytest.raises(ValueError):
        Postgres(host="some_host", user="user", password="passwd", spark=spark)


def test_postgres_driver_and_uri():
    conn = Postgres(
        host="some_host",
        user="user",
        password="passwd",
        database="default",
        spark=spark,
        extra={"ssl": "true", "autosave": "always"},
    )

    assert conn.jdbc_url == "jdbc:postgresql://some_host:5432/default?ssl=true&autosave=always"
    assert Postgres.driver == "org.postgresql.Driver"
    assert Postgres.package == "org.postgresql:postgresql:42.2.5"
    assert Postgres.port == 5432
