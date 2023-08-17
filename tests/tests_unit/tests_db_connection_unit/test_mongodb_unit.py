import re
from datetime import datetime

import pytest

from onetl.connection import MongoDB
from onetl.connection.db_connection.mongodb import MongoDBCollectionExistBehavior

pytestmark = [pytest.mark.mongodb, pytest.mark.db_connection, pytest.mark.connection]


def test_mongodb_package():
    warning_msg = re.escape("will be removed in 1.0.0, use `MongoDB.get_packages(spark_version=")
    with pytest.warns(UserWarning, match=warning_msg):
        assert MongoDB.package_spark_3_2 == "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"
        assert MongoDB.package_spark_3_3 == "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"
        assert MongoDB.package_spark_3_4 == "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"


def test_mongodb_get_packages_no_input():
    with pytest.raises(ValueError, match="You should pass either `scala_version` or `spark_version`"):
        MongoDB.get_packages()


@pytest.mark.parametrize(
    "spark_version",
    [
        "2.2",
        "2.3",
        "2.4",
    ],
)
def test_mongodb_get_packages_spark_version_not_supported(spark_version):
    with pytest.raises(ValueError, match=f"Spark version must be at least 3.0, got {spark_version}"):
        MongoDB.get_packages(spark_version=spark_version)


@pytest.mark.parametrize(
    "scala_version",
    [
        "2.11",
        "2.14",
        "3.0",
    ],
)
def test_mongodb_get_packages_scala_version_not_supported(scala_version):
    with pytest.raises(ValueError, match=f"Scala version must be 2.12 - 2.13, got {scala_version}"):
        MongoDB.get_packages(scala_version=scala_version)


@pytest.mark.parametrize(
    "spark_version, scala_version, package",
    [
        # use Scala version directly
        (None, "2.12", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"),
        (None, "2.13", "org.mongodb.spark:mongo-spark-connector_2.13:10.1.1"),
        # Detect Scala version by Spark version
        ("3.2", None, "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"),
        ("3.3", None, "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"),
        ("3.4", None, "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"),
        # Override Scala version detected automatically
        ("3.2", "2.12", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"),
        ("3.4", "2.13", "org.mongodb.spark:mongo-spark-connector_2.13:10.1.1"),
    ],
)
def test_mongodb_get_packages(spark_version, scala_version, package):
    assert MongoDB.get_packages(spark_version=spark_version, scala_version=scala_version) == [package]


def test_mongodb_missing_package(spark_no_packages):
    msg = "Cannot import Java class 'com.mongodb.spark.sql.connector.MongoTableProvider'"
    with pytest.raises(ValueError, match=msg):
        MongoDB(
            host="host",
            user="user",
            password="password",
            database="database",
            spark=spark_no_packages,
        )


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


@pytest.mark.parametrize(
    "options, value",
    [
        ({}, MongoDBCollectionExistBehavior.APPEND),
        ({"if_exists": "append"}, MongoDBCollectionExistBehavior.APPEND),
        ({"if_exists": "replace_entire_collection"}, MongoDBCollectionExistBehavior.REPLACE_ENTIRE_COLLECTION),
    ],
)
def test_mongodb_write_options_if_exists(options, value):
    assert MongoDB.WriteOptions(**options).if_exists == value


@pytest.mark.parametrize(
    "options, value, message",
    [
        (
            {"mode": "append"},
            MongoDBCollectionExistBehavior.APPEND,
            "Option `MongoDB.WriteOptions(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `MongoDB.WriteOptions(if_exists=...)` instead",
        ),
        (
            {"mode": "replace_entire_collection"},
            MongoDBCollectionExistBehavior.REPLACE_ENTIRE_COLLECTION,
            "Option `MongoDB.WriteOptions(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `MongoDB.WriteOptions(if_exists=...)` instead",
        ),
        (
            {"mode": "overwrite"},
            MongoDBCollectionExistBehavior.REPLACE_ENTIRE_COLLECTION,
            "Mode `overwrite` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `replace_entire_collection` instead",
        ),
    ],
)
def test_mongodb_write_options_mode_deprecated(options, value, message):
    with pytest.warns(UserWarning, match=re.escape(message)):
        options = MongoDB.WriteOptions(**options)
        assert options.if_exists == value


@pytest.mark.parametrize(
    "options",
    [
        # disallowed modes
        {"mode": "error"},
        {"mode": "ignore"},
        # wrong mode
        {"mode": "wrong_mode"},
    ],
)
def test_mongodb_write_options_mode_wrong(options):
    with pytest.raises(ValueError, match="value is not a valid enumeration member"):
        MongoDB.WriteOptions(**options)
