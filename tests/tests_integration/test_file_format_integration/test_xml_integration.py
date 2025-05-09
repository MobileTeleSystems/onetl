"""Integration tests for XML file format.

Test only that options are passed to Spark in both FileDFReader & FileDFWriter.
Do not test all the possible options and combinations, we are not testing Spark here.
"""

import datetime

import pytest

from onetl._util.spark import get_spark_version
from onetl.file import FileDFReader, FileDFWriter
from onetl.file.format import XML

try:
    from pyspark.sql import Row
    from pyspark.sql.functions import col

    from tests.util.assert_df import assert_equal_df
except ImportError:
    pytest.skip("Missing pandas or pyspark", allow_module_level=True)

pytestmark = [pytest.mark.local_fs, pytest.mark.file_df_connection, pytest.mark.connection, pytest.mark.xml]


@pytest.fixture()
def expected_xml_attributes_df(file_df_dataframe):
    col_names = file_df_dataframe.columns
    exprs = [f"{col} as _{col}" for col in col_names] + col_names
    return file_df_dataframe.selectExpr(*exprs)


@pytest.mark.parametrize(
    "path, options",
    [
        ("without_compression", {"rowTag": "item"}),
        ("with_compression", {"rowTag": "item", "compression": "gzip"}),
        ("with_attributes", {"rowTag": "item", "attributePrefix": "_"}),
    ],
    ids=["without_compression", "with_compression", "with_attributes"],
)
def test_xml_reader(
    spark,
    local_fs_file_df_connection_with_path_and_files,
    file_df_dataframe,
    path,
    options,
):
    """Reading XML files working as expected on any Spark, Python and Java versions"""
    spark_version = get_spark_version(spark)
    if spark_version.major < 3:
        pytest.skip("XML files are supported on Spark 3.x only")

    local_fs, source_path, _ = local_fs_file_df_connection_with_path_and_files
    df = file_df_dataframe
    xml_root = source_path / "xml" / path

    reader = FileDFReader(
        connection=local_fs,
        format=XML.parse(options),
        df_schema=df.schema,
        source_path=xml_root,
    )
    read_df = reader.run()
    assert read_df.count()
    assert read_df.schema == df.schema
    assert_equal_df(read_df, df, order_by="id")


def test_xml_reader_with_infer_schema(
    spark,
    local_fs_file_df_connection_with_path_and_files,
    expected_xml_attributes_df,
    file_df_dataframe,
):
    """Reading XML files with inferSchema=True working as expected on any Spark, Python and Java versions"""
    spark_version = get_spark_version(spark)
    if spark_version.major < 3:
        pytest.skip("XML files are supported on Spark 3.x only")

    file_df_connection, source_path, _ = local_fs_file_df_connection_with_path_and_files
    df = file_df_dataframe
    xml_root = source_path / "xml" / "with_attributes"

    reader = FileDFReader(
        connection=file_df_connection,
        format=XML(rowTag="item", inferSchema=True),
        source_path=xml_root,
    )
    read_df = reader.run()

    assert read_df.count()
    assert read_df.schema != df.schema
    assert set(read_df.columns) == set(
        expected_xml_attributes_df.columns,
    )  # "DataFrames have different column types: StructField('id', IntegerType(), True), StructField('id', LongType(), True), etc."
    assert_equal_df(read_df, expected_xml_attributes_df, order_by="id")


@pytest.mark.parametrize(
    "options",
    [
        {"rowTag": "item", "rootTag": "root"},
        {"rowTag": "item", "rootTag": "root", "compression": "gzip"},
    ],
    ids=["without_compression", "with_compression"],
)
def test_xml_writer(
    spark,
    local_fs_file_df_connection_with_path,
    file_df_dataframe,
    options,
):
    """Written files can be read by Spark"""
    spark_version = get_spark_version(spark)
    if spark_version.major < 3:
        pytest.skip("XML files are supported on Spark 3.x only")

    file_df_connection, source_path = local_fs_file_df_connection_with_path
    df = file_df_dataframe
    xml_root = source_path / "xml"

    writer = FileDFWriter(
        connection=file_df_connection,
        format=XML.parse(options),
        target_path=xml_root,
    )
    writer.run(df)

    reader = FileDFReader(
        connection=file_df_connection,
        format=XML.parse(options),
        source_path=xml_root,
        df_schema=df.schema,
    )
    read_df = reader.run()

    assert read_df.count()
    assert read_df.schema == df.schema
    assert_equal_df(read_df, df, order_by="id")


@pytest.mark.parametrize(
    "options",
    [
        {"rowTag": "item", "attributePrefix": "_"},
    ],
    ids=["read_attributes"],
)
def test_xml_reader_with_attributes(
    spark,
    local_fs_file_df_connection_with_path_and_files,
    expected_xml_attributes_df,
    options,
):
    """Reading XML files with attributes works as expected"""
    spark_version = get_spark_version(spark)
    if spark_version.major < 3:
        pytest.skip("XML files are supported on Spark 3.x only")

    local_fs, source_path, _ = local_fs_file_df_connection_with_path_and_files
    xml_root = source_path / "xml" / "with_attributes"

    reader = FileDFReader(
        connection=local_fs,
        format=XML.parse(options),
        df_schema=expected_xml_attributes_df.schema,
        source_path=xml_root,
    )
    read_df = reader.run()
    assert read_df.count()
    assert read_df.schema == expected_xml_attributes_df.schema
    assert_equal_df(read_df, expected_xml_attributes_df, order_by="id")


@pytest.mark.parametrize("column_type", [str, col])
def test_xml_parse_column(spark, column_type, file_df_schema):
    from onetl.file.format import XML

    spark_version = get_spark_version(spark)
    if spark_version.major < 3:
        pytest.skip("XML files are supported on Spark 3.x only")

    xml = XML(rowTag="item")
    df = spark.createDataFrame(
        [
            Row(
                xml_string="""
                <item>
                    <id>1</id>
                    <str_value>Alice</str_value>
                    <int_value>123</int_value>
                    <date_value>2021-01-01</date_value>
                    <datetime_value>2021-01-01T07:01:01Z</datetime_value>
                    <float_value>1.23</float_value>
                </item>
                """,
            ),
        ],
    )
    parsed_df = df.select(xml.parse_column(column_type("xml_string"), schema=file_df_schema))

    assert parsed_df.collect() == [
        Row(
            xml_string=Row(
                id=1,
                str_value="Alice",
                int_value=123,
                date_value=datetime.date(2021, 1, 1),
                datetime_value=datetime.datetime(2021, 1, 1, 7, 1, 1),
                float_value=1.23,
            ),
        ),
    ]


def test_xml_parse_column_unsupported_options_warning(spark, file_df_schema):
    spark_version = get_spark_version(spark)
    if spark_version.major < 3:
        pytest.skip("CSV.parse in supported on Spark 3.x only")

    df = spark.createDataFrame(
        [
            Row(
                xml_string="""
                <item>
                    <id>1</id>
                    <str_value>Alice</str_value>
                    <int_value>123</int_value>
                    <date_value>2021-01-01</date_value>
                    <datetime_value>2021-01-01T07:01:01Z</datetime_value>
                    <float_value>1.23</float_value>
                </item>
                """,
            ),
        ],
    )

    xml = XML(rowTag="item", inferSchema=True, samplingRatio=0.1)

    msg = "Options `['inferSchema', 'samplingRatio']` " "are set but not supported in `XML.parse_column`."

    with pytest.warns(UserWarning) as record:
        df.select(xml.parse_column(df.xml_string, file_df_schema)).collect()
        assert record
        assert msg in str(record[0].message)
