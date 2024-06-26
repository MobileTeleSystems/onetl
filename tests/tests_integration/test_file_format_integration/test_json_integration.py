"""Integration tests for JSON file format.

Test only that options are passed to Spark in both FileDFReader & FileDFWriter.
Do not test all the possible options and combinations, we are not testing Spark here.
"""

import pytest

from onetl._util.spark import get_spark_version
from onetl._util.version import Version
from onetl.file import FileDFReader, FileDFWriter
from onetl.file.format import JSON

try:
    from pyspark.sql import Row
    from pyspark.sql.functions import col
    from pyspark.sql.types import (
        ArrayType,
        IntegerType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    from tests.util.assert_df import assert_equal_df
except ImportError:
    pytest.skip("Missing pandas or pyspark", allow_module_level=True)

pytestmark = [pytest.mark.local_fs, pytest.mark.file_df_connection, pytest.mark.connection, pytest.mark.json]


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
    "json_string, schema, expected",
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
    ids=["struct", "map", "array"],
)
@pytest.mark.parametrize("column_type", [str, col])
def test_json_parse_column(spark, json_string, schema, expected, column_type):
    spark_version = get_spark_version(spark)
    if spark_version < Version("2.4") and isinstance(schema, MapType):
        pytest.skip("JSON.parse_column accepts MapType only in Spark 2.4+")

    json = JSON()
    df = spark.createDataFrame([(json_string,)], ["json_column"])
    parsed_df = df.select(json.parse_column(column_type("json_column"), schema))
    assert parsed_df.columns == ["json_column"]
    assert parsed_df.select("json_column").first()["json_column"] == expected


@pytest.mark.parametrize(
    "data, schema, expected_json",
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
    ids=["struct", "array", "map"],
)
@pytest.mark.parametrize("column_type", [str, col])
def test_json_serialize_column(spark, data, schema, expected_json, column_type):
    json = JSON()
    df_schema = StructType([StructField("json_string", schema)])
    df = spark.createDataFrame([(data,)], df_schema)
    serialized_df = df.select(json.serialize_column(column_type("json_string")))
    actual_json = serialized_df.select("json_string").first()["json_string"]
    assert actual_json == expected_json
    assert serialized_df.columns == ["json_string"]
