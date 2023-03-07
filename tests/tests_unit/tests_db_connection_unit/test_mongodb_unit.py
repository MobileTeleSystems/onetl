from frozendict import frozendict

from onetl.connection.db_connection.mongo import MongoDB

# TODO(@msmarty4): add unit tests for MongoDB


def test_mongodb_generate_pipeline(spark_mock):
    mongo = MongoDB(
        host="host",
        user="user",
        password="password",
        database="database",
        spark=spark_mock,
    )

    where = frozendict({"$col_1": {"$gt": 1, "$lt": 100}, "$col_2": {"$gt": 2}, "$col_3": {"$eq": "hello"}})

    assert mongo.Dialect.generate_where_request(where=where) == (
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

    where = {
        "$and": [
            {"$or": [{"$col_1": {"$gt": 1, "$eq": True}}, {"$col_2": {"$eq": None}}]},
            {"$and": [{"$col_3": {"$eq": "Hello"}}, {"$col_4": {"$eq": "Tom"}}]},
        ],
    }

    assert mongo.Dialect.generate_where_request(where=where) == (
        "{'$match':"
        "{'$and':["
        "{'$or':[{'$col_1':{'$gt':1,'$eq':true}},{'$col_2':{'$eq':null}}]},"
        "{'$and':[{'$col_3':{'$eq':'Hello'}},{'$col_4':{'$eq':'Tom'}}]}"
        "]}}"
    )
