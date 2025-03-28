"""Integration tests for CSV file format.

Test only that options are passed to Spark in both FileDFReader & FileDFWriter.
Do not test all the possible options and combinations, we are not testing Spark here.
"""

import contextlib
import re

import pytest

from onetl._util.spark import get_spark_version
from onetl._util.version import Version
from onetl.file import FileDFReader, FileDFWriter
from onetl.file.format import CSV

try:
    from pyspark.sql import Row
    from pyspark.sql.functions import col, struct
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    from tests.util.assert_df import assert_equal_df
    from tests.util.spark_df import reset_column_names
except ImportError:
    pytest.skip("Missing pandas or pyspark", allow_module_level=True)

pytestmark = [pytest.mark.local_fs, pytest.mark.file_df_connection, pytest.mark.connection, pytest.mark.csv]


def test_csv_reader_with_infer_schema(
    spark,
    local_fs_file_df_connection_with_path_and_files,
    file_df_dataframe,
):
    """Reading CSV files with inferSchema=True working as expected on any Spark, Python and Java versions"""
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

    spark_version = get_spark_version(spark)
    if spark_version.major < 3:
        # Spark 2 infers "date_value" as timestamp instead of date
        expected_df = df.withColumn("date_value", col("date_value").cast("timestamp"))
    elif spark_version < Version("3.3"):
        # Spark 3.2 cannot infer "date_value", and return it as string
        expected_df = df.withColumn("date_value", col("date_value").cast("string"))

    # csv does not have header, so columns are named like "_c0", "_c1", etc
    expected_df = reset_column_names(expected_df)
    first_column = expected_df.schema[0].name

    assert read_df.schema != df.schema
    assert read_df.schema == expected_df.schema
    assert_equal_df(read_df, expected_df, order_by=first_column)


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
    assert_equal_df(read_df, df, order_by="id")


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
    assert_equal_df(read_df, df, order_by="id")


@pytest.mark.parametrize(
    "csv_string, schema, options, expected",
    [
        (
            "1,Anne",
            StructType([StructField("id", IntegerType()), StructField("name", StringType())]),
            {"delimiter": ","},
            Row(id=1, name="Anne"),
        ),
        (
            "1;Anne",
            StructType([StructField("id", IntegerType()), StructField("name", StringType())]),
            {"delimiter": ";"},
            Row(id=1, name="Anne"),
        ),
        (
            '"1","Anne"',
            StructType([StructField("id", IntegerType()), StructField("name", StringType())]),
            {"delimiter": ",", "quote": '"'},
            Row(id=1, name="Anne"),
        ),
    ],
    ids=["comma-delimited", "semicolon-delimited", "quoted-comma-delimited"],
)
@pytest.mark.parametrize("column_type", [str, col])
def test_csv_parse_column(spark, csv_string, schema, options, expected, column_type):
    spark_version = get_spark_version(spark)
    if spark_version.major < 3:
        msg = (
            f"`CSV.parse_column` or `CSV.serialize_column` are available "
            f"only since Spark 3.x, but got {spark_version}"
        )
        context_manager = pytest.raises(ValueError, match=re.escape(msg))
    else:
        context_manager = contextlib.nullcontext()

    csv_handler = CSV(**options)
    df = spark.createDataFrame([(csv_string,)], ["csv_string"])

    with context_manager:
        parsed_df = df.select(csv_handler.parse_column(column_type("csv_string"), schema))
        assert parsed_df.columns == ["csv_string"]
        assert parsed_df.first()["csv_string"] == expected


@pytest.mark.parametrize(
    "data, schema, options, expected_csv",
    [
        (
            Row(id=1, name="Alice"),
            StructType([StructField("id", IntegerType()), StructField("name", StringType())]),
            {"delimiter": ","},
            "1,Alice",
        ),
        (
            Row(id=1, name="Alice"),
            StructType([StructField("id", IntegerType()), StructField("name", StringType())]),
            {"delimiter": ";"},
            "1;Alice",
        ),
    ],
    ids=["comma-delimited", "semicolon-delimited"],
)
@pytest.mark.parametrize("column_type", [str, col])
def test_csv_serialize_column(spark, data, schema, options, expected_csv, column_type):
    spark_version = get_spark_version(spark)
    if spark_version.major < 3:
        msg = (
            f"`CSV.parse_column` or `CSV.serialize_column` are available "
            f"only since Spark 3.x, but got {spark_version}"
        )
        context_manager = pytest.raises(ValueError, match=re.escape(msg))
    else:
        context_manager = contextlib.nullcontext()

    csv_handler = CSV(**options)
    df = spark.createDataFrame([data], schema)
    df = df.withColumn("csv_column", struct("id", "name"))

    with context_manager:
        serialized_df = df.select(csv_handler.serialize_column(column_type("csv_column")))
        assert serialized_df.columns == ["csv_column"]
        assert serialized_df.first()["csv_column"] == expected_csv


def test_csv_serialize_column_unsupported_options_warning(spark):
    spark_version = get_spark_version(spark)
    if spark_version.major < 3:
        pytest.skip("CSV.serialize_column in supported on Spark 3.x only")

    schema = StructType([StructField("id", IntegerType()), StructField("name", StringType())])
    df = spark.createDataFrame([Row(id=1, name="Alice")], schema)
    df = df.withColumn("csv_column", struct("id", "name"))

    csv = CSV(
        compression="gzip",
        encoding="utf-8",
        header=False,
    )
    msg = (
        "Options `['compression', 'encoding', 'header']` are set "
        "but not supported in `CSV.parse_column` or `CSV.serialize_column`."
    )

    with pytest.warns(UserWarning) as record:
        df.select(csv.serialize_column("csv_column")).collect()
        assert record
        assert msg in str(record[0].message)


def test_csv_parse_column_unsupported_options_warning(spark):
    spark_version = get_spark_version(spark)
    if spark_version.major < 3:
        pytest.skip("CSV.parse in supported on Spark 3.x only")

    schema = StructType([StructField("id", IntegerType()), StructField("name", StringType())])
    df = spark.createDataFrame([Row(csv_column="1,Alice")])

    csv = CSV(
        header=False,
        encoding="utf-8",
        inferSchema=True,
        samplingRatio=0.1,
        enforceSchema=True,
        preferDate=True,
        multiLine=True,
    )

    msg = (
        "Options `['encoding', 'enforceSchema', 'header', 'inferSchema', 'multiLine', 'preferDate', 'samplingRatio']` "
        "are set but not supported in `CSV.parse_column` or `CSV.serialize_column`."
    )

    with pytest.warns(UserWarning) as record:
        df.select(csv.parse_column(df.csv_column, schema)).collect()
        assert record
        assert msg in str(record[0].message)
