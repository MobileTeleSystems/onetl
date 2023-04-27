import pytest
from frozendict import frozendict

from onetl.connection.db_connection.mongo import MongoDB

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
    assert MongoDB.package_spark_2_4 == "org.mongodb.spark:mongo-spark-connector_2.11:2.4.4"
    assert MongoDB.package_spark_2_3 == "org.mongodb.spark:mongo-spark-connector_2.11:2.3.6"
    assert MongoDB.package_spark_3_2 == "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2"
    assert MongoDB.package_spark_3_3 == "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2"


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
        extra={"tls": "true", "tlsCertificateKeyFile": "/some/path"},
        spark=spark_mock,
    )

    assert (
        mongo.connection_url == "mongodb://user:password@host:27017/database?tls=true&tlsCertificateKeyFile=/some"
        "/path"
    )

    mongo = MongoDB(
        host="host",
        user="user",
        password="password",
        database="database",
        spark=spark_mock,
    )

    assert mongo.connection_url == "mongodb://user:password@host:27017/database"


def test_mongodb_generate_pipeline(spark_mock):
    mongo = MongoDB(
        host="host",
        user="user",
        password="password",
        database="database",
        spark=spark_mock,
    )

    pipeline = frozendict({"$col_1": {"$gt": 1, "$lt": 100}, "$col_2": {"$gt": 2}, "$col_3": {"$eq": "hello"}})

    assert mongo.Dialect.generate_where_request(where=pipeline) == (
        "{'$match':{'$col_1':{'$gt':1,'$lt':100},'$col_2':{'$gt':2},'$col_3':{'$eq':'hello'}}}"
    )


def test_mongodb_generate_pipeline_with_or_and(spark_mock):
    mongo = MongoDB(
        host="host",
        user="user",
        password="password",
        database="database",
        spark=spark_mock,
    )

    pipeline = {
        "$and": [
            {"$or": [{"col_1": {"$gt": 1, "$eq": True}}, {"col_2": {"$eq": None}}]},
            {"$and": [{"col_3": {"$eq": "Hello"}}, {"col_4": {"$eq": "Tom"}}]},
        ],
    }

    assert mongo.Dialect.generate_where_request(where=pipeline) == (
        "{'$match':"
        "{'$and':["
        "{'$or':[{'col_1':{'$gt':1,'$eq':true}},{'col_2':{'$eq':null}}]},"
        "{'$and':[{'col_3':{'$eq':'Hello'}},{'col_4':{'$eq':'Tom'}}]}"
        "]}}"
    )
