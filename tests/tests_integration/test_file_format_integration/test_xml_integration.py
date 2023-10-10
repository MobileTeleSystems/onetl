"""Integration tests for XML file format.

Test only that options are passed to Spark in both FileDFReader & FileDFWriter.
Do not test all the possible options and combinations, we are not testing Spark here.
"""

import pytest

from onetl._util.spark import get_spark_version
from onetl.file import FileDFReader, FileDFWriter
from onetl.file.format import XML

try:
    from tests.util.assert_df import assert_equal_df
except ImportError:
    # pandas and spark can be missing if someone runs tests for file connections only
    pass

pytestmark = [pytest.mark.local_fs, pytest.mark.file_df_connection, pytest.mark.connection]


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
    if spark_version < (3, 0):
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
    assert_equal_df(read_df, df)


def test_xml_reader_with_infer_schema(
    spark,
    local_fs_file_df_connection_with_path_and_files,
    expected_xml_attributes_df,
    file_df_dataframe,
):
    """Reading XML files with inferSchema=True working as expected on any Spark, Python and Java versions"""
    spark_version = get_spark_version(spark)
    if spark_version < (3, 0):
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
    assert_equal_df(read_df, expected_xml_attributes_df)


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
    if spark_version < (3, 0):
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
    assert_equal_df(read_df, df)


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
    if spark_version < (3, 0):
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
    assert_equal_df(read_df, expected_xml_attributes_df)
