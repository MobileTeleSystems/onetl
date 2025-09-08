"""Integration tests for JSON file format.

Test only that options are passed to Spark in both FileDFReader & FileDFWriter.
Do not test all the possible options and combinations, we are not testing Spark here.
"""

import pytest

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
    "json_row, schema, expected_row",
    [
        (
            Row(json_column='{"id": 1, "name": "Alice"}'),
            StructType([StructField("id", IntegerType()), StructField("name", StringType())]),
            Row(json_column=Row(id=1, name="Alice")),
        ),
        (
            Row(json_column='[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]'),
            ArrayType(StructType([StructField("id", IntegerType()), StructField("name", StringType())])),
            Row(json_column=[Row(id=1, name="Alice"), Row(id=2, name="Bob")]),
        ),
        (
            Row(json_column='{"key1": "value1", "key2": "value2"}'),
            MapType(StringType(), StringType()),
            Row(json_column={"key1": "value1", "key2": "value2"}),
        ),
    ],
    ids=["struct", "map", "array"],
)
@pytest.mark.parametrize("column_type", [str, col])
def test_json_parse_column(spark, json_row, schema, expected_row, column_type):
    json = JSON()
    df = spark.createDataFrame([json_row])
    parsed_df = df.select(json.parse_column(column_type("json_column"), schema))
    assert parsed_df.collect() == [expected_row]


@pytest.mark.parametrize(
    "row, expected_row",
    [
        (
            Row(json_column=Row(id=1, name="Alice")),
            Row(json_column='{"id":1,"name":"Alice"}'),
        ),
        (
            Row(json_column=[Row(id=1, name="Alice"), Row(id=2, name="Bob")]),
            Row(json_column='[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]'),
        ),
        (
            Row(json_column={"key1": "value1", "key2": "value2"}),
            Row(json_column='{"key1":"value1","key2":"value2"}'),
        ),
    ],
    ids=["struct", "array", "map"],
)
@pytest.mark.parametrize("column_type", [str, col])
def test_json_serialize_column(spark, row, expected_row, column_type):
    json = JSON()
    df = spark.createDataFrame([row])
    serialized_df = df.select(json.serialize_column(column_type("json_column")))
    assert serialized_df.collect() == [expected_row]


def test_json_serialize_column_unsupported_options_warning(spark):
    df = spark.createDataFrame([Row(json_column=Row(id=1, name="Alice"))])

    json = JSON(
        encoding="UTF-8",
        lineSep="\r\n",
    )
    msg = (
        "Options `['encoding', 'lineSep']` are set "
        "but not supported in `JSON.parse_column` or `JSON.serialize_column`."
    )

    with pytest.warns(UserWarning) as record:
        df.select(json.serialize_column(df.json_column)).collect()
        assert record
        assert msg in str(record[0].message)


def test_json_parse_column_unsupported_options_warning(spark):
    schema = StructType([StructField("id", IntegerType()), StructField("name", StringType())])
    df = spark.createDataFrame([Row(json_column='{"id":1,"name":"Alice"}')])

    json = JSON(
        encoding="UTF-8",
        lineSep="\r\n",
        samplingRatio=0.1,
        primitivesAsString=True,
        prefersDecimal=True,
        dropFieldIfAllNull=True,
    )
    msg = (
        "Options `['dropFieldIfAllNull', 'encoding', 'lineSep', 'prefersDecimal', 'primitivesAsString', 'samplingRatio']` "
        "are set but not supported in `JSON.parse_column` or `JSON.serialize_column`."
    )

    with pytest.warns(UserWarning) as record:
        df.select(json.parse_column(df.json_column, schema)).collect()
        assert record
        assert msg in str(record[0].message)
