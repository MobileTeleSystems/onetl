"""Integration tests for Avro file format.

Test only that options are passed to Spark in both FileDFReader & FileDFWriter.
Do not test all the possible options and combinations, we are not testing Spark here.
"""

import pytest
import responses

from onetl.file import FileDFReader, FileDFWriter
from onetl.file.format import Avro

try:
    from pyspark.sql.functions import col

    from tests.util.assert_df import assert_equal_df
except ImportError:
    pytest.skip("Missing pandas or pyspark", allow_module_level=True)

pytestmark = [pytest.mark.local_fs, pytest.mark.file_df_connection, pytest.mark.connection, pytest.mark.avro]


@pytest.fixture()
def avro_schema():
    return {
        "type": "record",
        "name": "MyType",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "str_value", "type": "string"},
            {"name": "int_value", "type": "int"},
            {"name": "date_value", "type": {"type": "int", "logicalType": "date"}},
            {
                "name": "datetime_value",
                "type": {"type": "long", "logicalType": "timestamp-millis"},
            },
            {"name": "float_value", "type": "double"},
        ],
    }


@pytest.mark.parametrize(
    "path, options",
    [
        ("without_compression", {}),
        ("with_compression", {"compression": "snappy"}),
    ],
    ids=["without_compression", "with_compression"],
)
def test_avro_reader(
    spark,
    local_fs_file_df_connection_with_path_and_files,
    file_df_dataframe,
    avro_schema,
    path,
    options,
):
    """Reading Avro files working as expected on any Spark, Python and Java versions"""
    local_fs, source_path, _ = local_fs_file_df_connection_with_path_and_files
    df = file_df_dataframe
    avro_root = source_path / "avro" / path

    reader = FileDFReader(
        connection=local_fs,
        format=Avro(avroSchema=avro_schema, **options),
        df_schema=df.schema,
        source_path=avro_root,
    )
    read_df = reader.run()

    assert read_df.count()
    assert read_df.schema == df.schema
    assert_equal_df(read_df, df, order_by="id")


@pytest.mark.parametrize(
    "options",
    [
        {},
        {"compression": "snappy"},
    ],
    ids=["without_compression", "with_compression"],
)
def test_avro_writer(
    spark,
    local_fs_file_df_connection_with_path,
    file_df_dataframe,
    avro_schema,
    options,
):
    """Written files can be read by Spark"""
    file_df_connection, source_path = local_fs_file_df_connection_with_path
    df = file_df_dataframe
    avro_root = source_path / "avro"

    writer = FileDFWriter(
        connection=file_df_connection,
        format=Avro(avroSchema=avro_schema, **options),
        target_path=avro_root,
    )
    writer.run(df)

    reader = FileDFReader(
        connection=file_df_connection,
        format=Avro(),
        source_path=avro_root,
        df_schema=df.schema,
    )
    read_df = reader.run()

    assert read_df.count()
    assert read_df.schema == df.schema
    assert_equal_df(read_df, df, order_by="id")


@pytest.mark.parametrize("column_type", [str, col])
def test_avro_serialize_and_parse_column(
    spark,
    file_df_dataframe,
    avro_schema,
    column_type,
):
    from pyspark.sql.functions import struct
    from pyspark.sql.types import BinaryType

    df = file_df_dataframe
    avro = Avro(avroSchema=avro_schema)

    combined_df = df.withColumn("combined", struct([col(c) for c in df.columns]))

    serialized_df = combined_df.select(avro.serialize_column(column_type("combined")))
    assert isinstance(serialized_df.schema["combined"].dataType, BinaryType)
    parsed_df = serialized_df.select(avro.parse_column(column_type("combined")))
    assert combined_df.select("combined").collect() == parsed_df.collect()


@pytest.mark.parametrize("column_type", [str, col])
def test_avro_serialize_and_parse_no_schema(
    spark,
    file_df_dataframe,
    column_type,
):
    from pyspark.sql.functions import struct
    from pyspark.sql.types import BinaryType

    df = file_df_dataframe
    avro = Avro()

    combined_df = df.withColumn("combined", struct([col(c) for c in df.columns]))
    serialized_df = combined_df.select(avro.serialize_column(column_type("combined")))
    assert isinstance(serialized_df.schema["combined"].dataType, BinaryType)

    with pytest.raises(
        ValueError,
        match="Avro.parse_column can be used only with defined `avroSchema` or `avroSchemaUrl`",
    ):
        serialized_df.select(avro.parse_column(column_type("combined")))


@pytest.mark.parametrize("column_type", [str, col])
@responses.activate
def test_avro_serialize_and_parse_with_schema_url(
    spark,
    file_df_dataframe,
    column_type,
    avro_schema,
):
    from pyspark.sql.functions import struct
    from pyspark.sql.types import BinaryType

    # mocking the request to return a JSON schema
    schema_url = "http://example.com/avro_schema"
    responses.add(responses.GET, schema_url, json=avro_schema, status=200)

    df = file_df_dataframe
    avro = Avro(avroSchemaUrl=schema_url)

    combined_df = df.withColumn("combined", struct([col(c) for c in df.columns]))
    serialized_df = combined_df.select(avro.serialize_column(column_type("combined")))
    assert isinstance(serialized_df.schema["combined"].dataType, BinaryType)
    parsed_df = serialized_df.select(avro.parse_column(column_type("combined")))
    assert combined_df.select("combined").collect() == parsed_df.collect()


def test_avro_serialize_and_parse_column_unsupported_options_warning(
    spark,
    file_df_dataframe,
    avro_schema,
):
    from pyspark.sql.functions import struct

    df = file_df_dataframe
    combined_df = df.withColumn("combined", struct([col(c) for c in df.columns]))

    avro_serialize = Avro(
        avroSchema=avro_schema,
        compression="gzip",
        mode="PERMISSIVE",
        positionalFieldMatching=True,
        enableStableIdentifiersForUnionType=True,
    )
    msg = (
        "Options `['compression', 'enableStableIdentifiersForUnionType', 'mode', 'positionalFieldMatching']` "
        "are set but not supported by `Avro.serialize_column`."
    )
    with pytest.warns(UserWarning) as record:
        serialized_df = combined_df.select(avro_serialize.serialize_column(combined_df.combined))
        assert record
        assert msg in str(record[0].message)

    avro_parse = Avro(avroSchema=avro_schema, compression="gzip")
    msg = "Options `['compression']` are set but not supported by `Avro.parse_column`."
    with pytest.warns(UserWarning) as record:
        serialized_df.select(avro_parse.parse_column(serialized_df.combined))
        assert record
        assert msg in str(record[0].message)
