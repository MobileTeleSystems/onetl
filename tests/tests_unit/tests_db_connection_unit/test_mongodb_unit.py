from datetime import datetime

import pytest

from onetl.connection import MongoDB

pytestmark = pytest.mark.mongodb


def test_mongodb(spark_mock):
    conn = MongoDB(
        host="host",
        user="user",
        password="password",
        database="database",
        spark=spark_mock,
    )

    assert conn.host == "host"
    assert conn.port == 27017
    assert conn.user == "user"
    assert conn.password != "password"
    assert conn.password.get_secret_value() == "password"
    assert conn.database == "database"

    assert conn.connection_url == "mongodb://user:password@host:27017/database"

    assert "password='passwd'" not in str(conn)
    assert "password='passwd'" not in repr(conn)


def test_mongodb_class_attributes():
    assert MongoDB.package_spark_3_2 == "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"
    assert MongoDB.package_spark_3_3 == "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"
    assert MongoDB.package_spark_3_4 == "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"


@pytest.mark.parametrize(
    "prohibited_options",
    [
        {"uri": "uri"},
        {"database": "db"},
        {"collection": "collection"},
        {"pipeline": {"$match": {"_id": [{"$eq": 1}]}}},
    ],
)
def test_mongodb_prohibited_options_error(prohibited_options):
    with pytest.raises(ValueError):  # noqa: PT011
        MongoDB.PipelineOptions(**prohibited_options)


def test_mongodb_options_hint():
    MongoDB.PipelineOptions(hint={"_id": 1})


def test_mongodb_with_port(spark_mock):
    mongo = MongoDB(
        host="host",
        user="user",
        password="password",
        database="database",
        port=12345,
        spark=spark_mock,
    )

    assert mongo.host == "host"
    assert mongo.port == 12345
    assert mongo.user == "user"
    assert mongo.password != "password"
    assert mongo.password.get_secret_value() == "password"
    assert mongo.database == "database"

    assert mongo.connection_url == "mongodb://user:password@host:12345/database"


def test_mongodb_without_mandatory_args(spark_mock):
    with pytest.raises(ValueError, match="field required"):
        MongoDB()

    with pytest.raises(ValueError, match="field required"):
        MongoDB(
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        MongoDB(
            host="some_host",
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        MongoDB(
            host="some_host",
            user="user",
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        MongoDB(
            host="some_host",
            password="passwd",
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        MongoDB(
            host="some_host",
            user="user",
            password="passwd",
            spark=spark_mock,
        )


def test_mongodb_with_extra(spark_mock):
    mongo = MongoDB(
        host="host",
        user="user",
        password="password",
        database="database",
        extra={"tls": "true", "opt1": "value1"},
        spark=spark_mock,
    )

    assert mongo.connection_url == "mongodb://user:password@host:27017/database?opt1=value1&tls=true"


def test_mongodb_convert_list_to_str():
    where = [
        {"$or": [{"col_1": {"$gt": 1, "$eq": True}}, {"col_2": {"$eq": None}}]},
        {
            "$and": [
                {"col_3": {"$eq": "Hello"}},
                {"col_4": {"$eq": datetime.fromisoformat("2022-12-23T11:22:33.456+03:00")}},
            ],
        },
    ]

    assert MongoDB.Dialect.convert_to_str(where) == (
        '[{"$or": [{"col_1": {"$gt": 1, "$eq": true}}, {"col_2": {"$eq": null}}]}, '
        '{"$and": [{"col_3": {"$eq": "Hello"}}, {"col_4": {"$eq": {"$date": "2022-12-23T08:22:33.456000+00:00"}}}]}]'
    )


def test_mongodb_convert_dict_to_str():
    where = {
        "$and": [
            {"$or": [{"col_1": {"$gt": 1, "$eq": True}}, {"col_2": {"$eq": None}}]},
            {
                "$and": [
                    {"col_3": {"$eq": "Hello"}},
                    {"col_4": {"$eq": datetime.fromisoformat("2022-12-23T11:22:33.456+03:00")}},
                ],
            },
        ],
    }

    assert MongoDB.Dialect.convert_to_str(where) == (
        '{"$and": '
        '[{"$or": [{"col_1": {"$gt": 1, "$eq": true}}, {"col_2": {"$eq": null}}]}, '
        '{"$and": [{"col_3": {"$eq": "Hello"}}, {"col_4": {"$eq": {"$date": "2022-12-23T08:22:33.456000+00:00"}}}]}]'
        "}"
    )
