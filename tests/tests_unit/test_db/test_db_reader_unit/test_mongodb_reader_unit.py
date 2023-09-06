import pytest

from onetl.connection import MongoDB
from onetl.db import DBReader

pytestmark = pytest.mark.mongodb


@pytest.fixture()
def df_schema():
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("_id", IntegerType()),
            StructField("text_string", StringType()),
            StructField("hwm_int", IntegerType()),
            StructField("hwm_datetime", TimestampType()),
            StructField("float_value", DoubleType()),
        ],
    )


def test_mongodb_reader_wrong_hint_type(spark_mock, df_schema):
    mongo = MongoDB(
        host="host",
        user="user",
        password="password",
        database="database",
        spark=spark_mock,
    )

    with pytest.raises(
        ValueError,
        match="MongoDB requires 'hint' parameter type to be 'dict', got 'str'",
    ):
        DBReader(
            connection=mongo,
            where={"col_2": {"$eq": 2}, "col_1": {"$gt": 1, "$lt": 100}},
            hint="{'col1': 1}",
            table="table",
            df_schema=df_schema,
        )


def test_mongodb_reader_wrong_where_type(spark_mock, df_schema):
    mongo = MongoDB(
        host="host",
        user="user",
        password="password",
        database="database",
        spark=spark_mock,
    )

    with pytest.raises(
        ValueError,
        match="MongoDB requires 'where' parameter type to be 'dict', got 'str'",
    ):
        DBReader(
            connection=mongo,
            where="{'col_2': {'$eq': 2}, 'col_1': {'$gt': 1, '$lt': 100}, }",
            hint={"col1": 1},
            table="table",
            df_schema=df_schema,
        )


def test_mongodb_reader_where_wrong_value_match(spark_mock, df_schema):
    wrong_param = "$match"
    mongo = MongoDB(
        host="host",
        user="user",
        password="password",
        database="database",
        spark=spark_mock,
    )
    where = {wrong_param: {"col_2": {"$eq": 2}, "col_1": {"$gt": 1, "$lt": 100}}}

    with pytest.raises(
        ValueError,
        match=r"'\$match' operator not allowed at the top level of the 'where' parameter dictionary.*",
    ):
        DBReader(
            connection=mongo,
            where=where,
            table="table",
            df_schema=df_schema,
        )


def test_mongodb_reader_where_wrong_value(spark_mock, df_schema):
    wrong_param = "$limit"
    mongo = MongoDB(
        host="host",
        user="user",
        password="password",
        database="database",
        spark=spark_mock,
    )
    where = {wrong_param: {"col_2": {"$eq": 2}, "col_1": {"$gt": 1, "$lt": 100}}}

    with pytest.raises(
        ValueError,
        match="An invalid parameter '\\" + wrong_param + "' was specified in the 'where' field.*",
    ):
        DBReader(
            connection=mongo,
            where=where,
            table="table",
            df_schema=df_schema,
        )


def test_mongodb_reader_without_df_schema(spark_mock):
    mongo = MongoDB(
        host="host",
        user="user",
        password="password",
        database="database",
        spark=spark_mock,
    )

    with pytest.raises(ValueError, match="'df_schema' parameter is mandatory for MongoDB"):
        DBReader(
            connection=mongo,
            table="table",
        )


def test_mongodb_reader_error_pass_hwm_expression(spark_mock, df_schema):
    mongo = MongoDB(
        host="host",
        user="user",
        password="password",
        database="database",
        spark=spark_mock,
    )

    with pytest.raises(
        ValueError,
        match="'hwm_expression' parameter is not supported by MongoDB",
    ):
        DBReader(connection=mongo, table="table", df_schema=df_schema, hwm_column=("hwm_int", "expr"))


def test_mongodb_reader_error_pass_columns(spark_mock, df_schema):
    mongo = MongoDB(
        host="host",
        user="user",
        password="password",
        database="database",
        spark=spark_mock,
    )

    with pytest.raises(
        ValueError,
        match="'columns' parameter is not supported by MongoDB",
    ):
        DBReader(connection=mongo, table="table", columns=["_id", "test"], df_schema=df_schema)


def test_mongodb_reader_hwm_column_not_in_df_schema(spark_mock, df_schema):
    mongo = MongoDB(
        host="host",
        user="user",
        password="password",
        database="database",
        spark=spark_mock,
    )

    with pytest.raises(
        ValueError,
        match="'df_schema' struct must contain column specified in 'hwm_column'.*",
    ):
        DBReader(connection=mongo, table="table", hwm_column="_id2", df_schema=df_schema)
