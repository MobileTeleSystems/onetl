"""Integration tests for Avro file format.

Test only that options are passed to Spark in both FileDFReader & FileDFWriter.
Do not test all the possible options and combinations, we are not testing Spark here.
"""

import pytest

from onetl._util.spark import get_spark_version
from onetl.file import FileDFReader, FileDFWriter
from onetl.file.format import Avro

try:
    from tests.util.assert_df import assert_equal_df
except ImportError:
    # pandas and spark can be missing if someone runs tests for file connections only
    pass

pytestmark = [pytest.mark.local_fs, pytest.mark.file_df_connection, pytest.mark.connection]


@pytest.fixture()
def avro_schema():
    return {
        "type": "record",
        "name": "MyType",
        "fields": [
            # Without "null" writing data is failing on Spark 2.4
            # https://www.waitingforcode.com/apache-spark-sql/apache-avro-apache-spark-compatibility/read#problem_2_nullability
            {"name": "id", "type": ["null", "int"]},
            {"name": "str_value", "type": ["null", "string"]},
            {"name": "int_value", "type": ["null", "int"]},
            {"name": "date_value", "type": ["null", "int"]},
            {"name": "datetime_value", "type": ["null", "long"]},
            {"name": "float_value", "type": ["null", "double"]},
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
    spark_version = get_spark_version(spark)
    if spark_version < (2, 4):
        pytest.skip("Avro only supported on Spark 2.4+")

    local_fs, source_path, _ = local_fs_file_df_connection_with_path_and_files
    df = file_df_dataframe
    avro_root = source_path / "avro" / path

    reader = FileDFReader(
        connection=local_fs,
        format=Avro(schema_dict=avro_schema, **options),
        df_schema=df.schema,
        source_path=avro_root,
    )
    read_df = reader.run()

    assert read_df.count()
    assert read_df.schema == df.schema
    assert_equal_df(read_df, df)


@pytest.mark.parametrize(
    "path, options",
    [
        ("without_compression", {}),
        ("with_compression", {"compression": "snappy"}),
    ],
    ids=["without_compression", "with_compression"],
)
def test_avro_writer(
    spark,
    local_fs_file_df_connection_with_path,
    file_df_dataframe,
    avro_schema,
    path,
    options,
):
    """Written files can be read by Spark"""
    spark_version = get_spark_version(spark)
    if spark_version < (2, 4):
        pytest.skip("Avro only supported on Spark 2.4+")

    file_df_connection, source_path = local_fs_file_df_connection_with_path
    df = file_df_dataframe
    avro_root = source_path / "avro"

    writer = FileDFWriter(
        connection=file_df_connection,
        format=Avro(schema_dict=avro_schema, **options),
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
    assert_equal_df(read_df, df)
