"""Integration tests for XML file format.

Test only that options are passed to Spark in both FileDFReader & FileDFWriter.
Do not test all the possible options and combinations, we are not testing Spark here.
"""

import pytest

from onetl.file import FileDFReader, FileDFWriter
from onetl.file.format import XML

try:
    from tests.util.assert_df import assert_equal_df
except ImportError:
    # pandas and spark can be missing if someone runs tests for file connections only
    pass

pytestmark = [pytest.mark.local_fs, pytest.mark.file_df_connection, pytest.mark.connection]


@pytest.mark.parametrize(
    "path, options",
    [
        ("without_compression", {"rowTag": "item", "timestampFormat": "yyyy-MM-dd HH:mm:ssXXX"}),
        ("with_attributes", {"rowTag": "item", "timestampFormat": "yyyy-MM-dd HH:mm:ssXXX", "attributePrefix": "_"}),
    ],
    ids=["without_compression", "with_attributes"],
)
def test_xml_reader(
    local_fs_file_df_connection_with_path_and_files,
    file_df_dataframe,
    path,
    options,
):
    """Reading XML files working as expected on any Spark, Python and Java versions"""

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
