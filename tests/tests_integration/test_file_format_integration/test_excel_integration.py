"""Integration tests for Excel file format.

Test only that options are passed to Spark in both FileDFReader & FileDFWriter.
Do not test all the possible options and combinations, we are not testing Spark here.
"""

import pytest

from onetl._util.spark import get_spark_version
from onetl.file import FileDFReader, FileDFWriter
from onetl.file.format import Excel

try:
    from pyspark.sql.functions import col

    from tests.util.assert_df import assert_equal_df
    from tests.util.spark_df import reset_column_names
except ImportError:
    # pandas and spark can be missing if someone runs tests for file connections only
    pass

pytestmark = [pytest.mark.local_fs, pytest.mark.file_df_connection, pytest.mark.connection]


@pytest.mark.parametrize("format", ["xlsx", "xls"])
def test_excel_reader_with_infer_schema(
    spark,
    local_fs_file_df_connection_with_path_and_files,
    file_df_dataframe,
    format,
):
    """Reading CSV files with inferSchema=True working as expected on any Spark, Python and Java versions"""
    spark_version = get_spark_version(spark)
    if spark_version < (3, 2):
        pytest.skip("Excel files are supported on Spark 3.2+ only")
    if spark_version >= (3, 5):
        pytest.skip("Excel files are not supported on Spark 3.5+ yet")

    file_df_connection, source_path, _ = local_fs_file_df_connection_with_path_and_files
    df = file_df_dataframe
    excel_root = source_path / format / "without_header"

    reader = FileDFReader(
        connection=file_df_connection,
        format=Excel(inferSchema=True),
        source_path=excel_root,
    )
    read_df = reader.run()

    assert read_df.count()

    expected_df = df
    # Spark infers "date_value" as timestamp instead of date
    expected_df = df.withColumn("date_value", col("date_value").cast("timestamp"))

    # excel does not have header, so columns are named like "_c0", "_c1", etc
    expected_df = reset_column_names(expected_df)

    assert read_df.schema != df.schema
    assert read_df.schema == expected_df.schema
    assert_equal_df(read_df, expected_df)


@pytest.mark.parametrize("format", ["xlsx", "xls"])
@pytest.mark.parametrize(
    "path, options",
    [
        ("without_header", {}),
        ("with_header", {"header": True}),
        ("with_data_address", {"dataAddress": "'ABC'!K6"}),
    ],
    ids=["without_header", "with_header", "with_data_address"],
)
def test_excel_reader_with_options(
    spark,
    local_fs_file_df_connection_with_path_and_files,
    file_df_dataframe,
    format,
    path,
    options,
):
    """Reading Excel files working as expected on any Spark, Python and Java versions"""
    spark_version = get_spark_version(spark)
    if spark_version < (3, 2):
        pytest.skip("Excel files are supported on Spark 3.2+ only")
    if spark_version >= (3, 5):
        pytest.skip("Excel files are not supported on Spark 3.5+ yet")

    local_fs, source_path, _ = local_fs_file_df_connection_with_path_and_files
    df = file_df_dataframe
    excel_root = source_path / format / path

    reader = FileDFReader(
        connection=local_fs,
        format=Excel.parse(options),
        df_schema=df.schema,
        source_path=excel_root,
    )
    read_df = reader.run()

    assert read_df.count()
    assert read_df.schema == df.schema
    assert_equal_df(read_df, df)


@pytest.mark.parametrize(
    "options",
    [
        {},
        {"header": True},
    ],
    ids=["without_header", "with_header"],
)
def test_excel_writer(
    spark,
    local_fs_file_df_connection_with_path,
    file_df_dataframe,
    options,
):
    """Written files can be read by Spark"""
    spark_version = get_spark_version(spark)
    if spark_version < (3, 2):
        pytest.skip("Excel files are supported on Spark 3.2+ only")
    if spark_version >= (3, 5):
        pytest.skip("Excel files are not supported on Spark 3.5+ yet")

    file_df_connection, source_path = local_fs_file_df_connection_with_path
    df = file_df_dataframe
    excel_root = source_path / "excel"

    writer = FileDFWriter(
        connection=file_df_connection,
        format=Excel.parse(options),
        target_path=excel_root,
    )
    writer.run(df)

    reader = FileDFReader(
        connection=file_df_connection,
        format=Excel.parse(options),
        source_path=excel_root,
        df_schema=df.schema,
    )
    read_df = reader.run()

    assert read_df.count()
    assert read_df.schema == df.schema
    assert_equal_df(read_df, df)
