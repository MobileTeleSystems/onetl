import datetime

import pytest
from pytest_lazyfixture import lazy_fixture


@pytest.fixture()
def file_df_schema():
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("id", IntegerType()),
            StructField("str_value", StringType()),
            StructField("int_value", IntegerType()),
            StructField("date_value", DateType()),
            StructField("datetime_value", TimestampType()),
            StructField("float_value", DoubleType()),
        ],
    )


@pytest.fixture()
def file_df_schema_str_value_last():
    # partitioned dataframe has "str_value" column moved to the end
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("id", IntegerType()),
            StructField("int_value", IntegerType()),
            StructField("date_value", DateType()),
            StructField("datetime_value", TimestampType()),
            StructField("float_value", DoubleType()),
            StructField("str_value", StringType()),
        ],
    )


@pytest.fixture()
def file_df_dataframe(spark, file_df_schema):
    data = [
        [1, "val1", 123, datetime.date(2021, 1, 1), datetime.datetime(2021, 1, 1, 1, 1, 1), 1.23],
        [2, "val1", 234, datetime.date(2022, 2, 2), datetime.datetime(2022, 2, 2, 2, 2, 2), 2.34],
        [3, "val2", 345, datetime.date(2023, 3, 3), datetime.datetime(2023, 3, 3, 3, 3, 3), 3.45],
        [4, "val2", 456, datetime.date(2024, 4, 4), datetime.datetime(2024, 4, 4, 4, 4, 4), 4.56],
        [5, "val3", 567, datetime.date(2025, 5, 5), datetime.datetime(2025, 5, 5, 5, 5, 5), 5.67],
        [6, "val3", 678, datetime.date(2026, 6, 6), datetime.datetime(2026, 6, 6, 6, 6, 6), 6.78],
        [7, "val3", 789, datetime.date(2027, 7, 7), datetime.datetime(2027, 7, 7, 7, 7, 7), 7.89],
    ]
    return spark.createDataFrame(data, schema=file_df_schema)


@pytest.fixture(
    params=[
        lazy_fixture("local_fs_file_df_connection"),
        lazy_fixture("s3_file_df_connection"),
        lazy_fixture("hdfs_file_df_connection"),
    ],
)
def file_df_connection(request):
    return request.param


@pytest.fixture(
    params=[
        lazy_fixture("local_fs_file_df_connection_with_path"),
        lazy_fixture("s3_file_df_connection_with_path"),
        lazy_fixture("hdfs_file_df_connection_with_path"),
    ],
)
def file_df_connection_with_path(request):
    return request.param


@pytest.fixture(
    params=[
        lazy_fixture("local_fs_file_df_connection_with_path_and_files"),
        lazy_fixture("s3_file_df_connection_with_path_and_files"),
        lazy_fixture("hdfs_file_df_connection_with_path_and_files"),
    ],
)
def file_df_connection_with_path_and_files(request):
    return request.param
