from unittest.mock import Mock

import pytest
from frozendict import frozendict
from pyspark.sql import SparkSession

from onetl.connection.db_connection.mongo import MongoDB
from onetl.core.db_reader.db_reader import DBReader

spark = Mock(spec=SparkSession)
spark.sparkContext = Mock()
spark.sparkContext.appName = "abc"


def test_mongodb_reader_pass_str_to_hint(processing, load_table_data):
    mongo = MongoDB(
        host="host",
        user="user",
        password="password",
        database="database",
        spark=spark,
    )

    with pytest.raises(
        ValueError,
        match=".*Parameter 'hint' cannot be a string, 'dict' must be passed.",
    ):
        DBReader(
            connection=mongo,
            where={"col_2": {"$eq": 2}, "col_1": {"$gt": 1, "$lt": 100}},
            hint="{'col1': 1}",
            table=load_table_data.table,
        )


def test_mongodb_reader_pass_str_to_where(processing, load_table_data):
    mongo = MongoDB(
        host="host",
        user="user",
        password="password",
        database="database",
        spark=spark,
    )

    with pytest.raises(
        ValueError,
        match=".*Parameter 'where' cannot be a string, 'dict' must be passed.",
    ):
        DBReader(
            connection=mongo,
            where="{'col_2': {'$eq': 2}, 'col_1': {'$gt': 1, '$lt': 100}, }",
            hint={"col1": 1},
            table=load_table_data.table,
        )


def test_mongodb_reader_wrong_value_match(processing, load_table_data):
    wrong_param = "$match"
    mongo = MongoDB(
        host="host",
        user="user",
        password="password",
        database="database",
        spark=spark,
    )
    where = frozendict({wrong_param: {"col_2": {"$eq": 2}, "col_1": {"$gt": 1, "$lt": 100}}})

    with pytest.raises(
        ValueError,
        match=r".*\$match operator not allowed at the top level of the 'where' parameter dictionary.*",
    ):
        DBReader(
            connection=mongo,
            where=where,
            table=load_table_data.table,
        )


def test_mongodb_reader_wrong_value(processing, load_table_data):
    wrong_param = "$limit"
    mongo = MongoDB(
        host="host",
        user="user",
        password="password",
        database="database",
        spark=spark,
    )
    where = frozendict({wrong_param: {"col_2": {"$eq": 2}, "col_1": {"$gt": 1, "$lt": 100}}})

    with pytest.raises(
        ValueError,
        match=".*An invalid parameter '\\" + wrong_param + "' was specified in the 'where' field.*",
    ):
        DBReader(
            connection=mongo,
            where=where,
            table=load_table_data.table,
        )


def test_mongodb_generate_pipeline(processing, load_table_data):
    mongo = MongoDB(
        host="host",
        user="user",
        password="password",
        database="database",
        spark=spark,
    )

    where = frozendict({"$col_1": {"$gt": 1, "$lt": 100}, "$col_2": {"$gt": 2}, "$col_3": {"$eq": "hello"}})

    assert mongo.Dialect.generate_where_request(where=where) == (
        "{'$match':{'$col_1':{'$gt':1,'$lt':100},'$col_2':{'$gt':2},'$col_3':{'$eq':'hello'}}}"
    )


def test_mongodb_generate_pipeline_with_or_and(processing, load_table_data):
    mongo = MongoDB(
        host="host",
        user="user",
        password="password",
        database="database",
        spark=spark,
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
