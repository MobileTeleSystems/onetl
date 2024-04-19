"""Integration tests for JSON file format.

Test only that options are passed to Spark in both FileDFReader & FileDFWriter.
Do not test all the possible options and combinations, we are not testing Spark here.
"""

import pytest
from pyspark.sql import Row
from pyspark.sql.functions import col, struct
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from onetl.file import FileDFReader, FileDFWriter
from onetl.file.format import JSON

try:
    from tests.util.assert_df import assert_equal_df
except ImportError:
    pytest.skip("Missing pandas", allow_module_level=True)

pytestmark = [pytest.mark.local_fs, pytest.mark.file_df_connection, pytest.mark.connection]


@pytest.mark.parametrize(
    "path, options",
    [
        ("without_compression", {}),
        ("with_compression", {"compression": "gzip"}),
    ],
    ids=["without_compression", "with_compression"],
)
def test_json_reader(
    local_fs_file_df_connection_with_path_and_files,
    file_df_dataframe,
    path,
    options,
):
    """Reading JSON files working as expected on any Spark, Python and Java versions"""
    local_fs, source_path, _ = local_fs_file_df_connection_with_path_and_files
    df = file_df_dataframe
    json_root = source_path / "json" / path

    reader = FileDFReader(
        connection=local_fs,
        format=JSON.parse(options),
        df_schema=df.schema,
        source_path=json_root,
    )
    read_df = reader.run()

    assert read_df.count()
    assert read_df.schema == df.schema
    assert_equal_df(read_df, df, order_by="id")


def test_json_writer_is_not_supported(
    local_fs_file_df_connection_with_path,
):
    """Writing JSON files is not supported"""
    file_df_connection, source_path = local_fs_file_df_connection_with_path
    json_root = source_path / "json"

    with pytest.raises(ValueError):
        FileDFWriter(
            connection=file_df_connection,
            format=JSON(),
            target_path=json_root,
        )


@pytest.mark.parametrize(
    "json_string,schema,expected",
    [
        (
            '{"id": 1, "name": "Alice"}',
            StructType([StructField("id", IntegerType()), StructField("name", StringType())]),
            Row(id=1, name="Alice"),
        ),
        (
            '[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]',
            ArrayType(StructType([StructField("id", IntegerType()), StructField("name", StringType())])),
            [Row(id=1, name="Alice"), Row(id=2, name="Bob")],
        ),
        (
            '{"key1": "value1", "key2": "value2"}',
            MapType(StringType(), StringType()),
            {"key1": "value1", "key2": "value2"},
        ),
    ],
)
def test_json_parse_column(spark, json_string, schema, expected):
    json_class = JSON()
    df = spark.createDataFrame([(json_string,)], ["json_string"])
    parsed_df = df.withColumn("parsed", json_class.parse_column(col("json_string"), schema))
    assert parsed_df.select("parsed").first()["parsed"] == expected


@pytest.mark.parametrize(
    "data,schema,expected_json",
    [
        (
            {"id": 1, "name": "Alice"},
            StructType([StructField("id", IntegerType(), True), StructField("name", StringType(), True)]),
            '{"id":1,"name":"Alice"}',
        ),
        (
            [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            ArrayType(StructType([StructField("id", IntegerType(), True), StructField("name", StringType(), True)])),
            '[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]',
        ),
        (
            {"key1": "value1", "key2": "value2"},
            MapType(StringType(), StringType()),
            '{"key1":"value1","key2":"value2"}',
        ),
    ],
)
def test_json_serialize_column(spark, data, schema, expected_json):
    json_class = JSON()

    if isinstance(schema, StructType):
        df = spark.createDataFrame([data], schema=schema)
        serialized_df = df.withColumn("json_string", json_class.serialize_column(struct(*df.columns)))
    elif isinstance(schema, ArrayType):
        df = spark.createDataFrame([data], schema)
        serialized_df = df.withColumn("json_string", json_class.serialize_column(col("value")))
    elif isinstance(schema, MapType):
        df = spark.createDataFrame([data], schema)
        serialized_df = df.withColumn("json_string", json_class.serialize_column(col("value")))

    actual_json = serialized_df.select("json_string").first()["json_string"]
    assert actual_json == expected_json
