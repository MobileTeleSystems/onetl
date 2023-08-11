"""Integration tests for CSV file format.

Test only that options are passed to Spark in both FileDFReader & FileDFWriter.
Do not test all the possible options and combinations, we are not testing Spark here.
"""

import pytest

from onetl._util.spark import get_spark_version
from onetl.file import FileDFReader, FileDFWriter
from onetl.file.format import CSV

try:
    from pyspark.sql.functions import col

    from tests.util.assert_df import assert_equal_df
    from tests.util.spark_df import reset_column_names
except ImportError:
    # pandas and spark can be missing if someone runs tests for file connections only
    pass

pytestmark = [pytest.mark.local_fs, pytest.mark.file_df_connection, pytest.mark.connection]


def test_csv_reader_with_infer_schema(
    spark,
    local_fs_file_df_connection_with_path_and_files,
    file_df_dataframe,
):
    file_df_connection, source_path, _ = local_fs_file_df_connection_with_path_and_files
    df = file_df_dataframe
    csv_root = source_path / "csv/without_header"

    reader = FileDFReader(
        connection=file_df_connection,
        format=CSV(inferSchema=True),
        source_path=csv_root,
    )
    read_df = reader.run()

    assert read_df.count()

    expected_df = df

    if get_spark_version(spark).major < 3:
        # Spark 2 infers "date_value" as timestamp instead of date
        expected_df = df.withColumn("date_value", col("date_value").cast("timestamp"))

    # csv does not have header, so columns are named like "_c0", "_c1", etc
    expected_df = reset_column_names(expected_df)

    assert read_df.schema != df.schema
    assert read_df.schema == expected_df.schema
    assert_equal_df(read_df, expected_df)


@pytest.mark.parametrize(
    "option, value",
    [
        ("header", True),
        ("delimiter", ";"),
        ("compression", "gzip"),
    ],
    ids=["with_header", "with_delimiter", "with_compression"],
)
def test_csv_reader_with_options(
    local_fs_file_df_connection_with_path_and_files,
    file_df_dataframe,
    option,
    value,
):
    """Reading CSV files working as expected on any Spark, Python and Java versions"""
    local_fs, source_path, _ = local_fs_file_df_connection_with_path_and_files
    df = file_df_dataframe
    csv_root = source_path / f"csv/with_{option}"

    reader = FileDFReader(
        connection=local_fs,
        format=CSV.parse({option: value}),
        df_schema=df.schema,
        source_path=csv_root,
    )
    read_df = reader.run()

    assert read_df.count()
    assert read_df.schema == df.schema
    assert_equal_df(read_df, df)


@pytest.mark.parametrize(
    "option, value",
    [
        ("header", "True"),
        ("delimiter", ";"),
        ("compression", "gzip"),
    ],
    ids=["with_header", "with_delimiter", "with_compression"],
)
def test_csv_writer_with_options(
    local_fs_file_df_connection_with_path,
    file_df_dataframe,
    option,
    value,
):
    """Written files can be read by Spark"""
    file_df_connection, source_path = local_fs_file_df_connection_with_path
    df = file_df_dataframe
    csv_root = source_path / "csv"

    csv = CSV.parse({option: value})

    writer = FileDFWriter(
        connection=file_df_connection,
        format=csv,
        target_path=csv_root,
    )
    writer.run(df)

    reader = FileDFReader(
        connection=file_df_connection,
        format=csv,
        source_path=csv_root,
        df_schema=df.schema,
    )
    read_df = reader.run()

    assert read_df.count()
    assert read_df.schema == df.schema
    assert_equal_df(read_df, df)
