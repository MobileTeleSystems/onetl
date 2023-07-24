"""Integration tests for JSONLine file format.

Test only that options are passed to Spark in both FileReader & FileWriter.
Do not test all the possible options and combinations, we are not testing Spark here.
"""

import pytest

from onetl.file import FileReader, FileWriter
from onetl.file.format import JSONLine

try:
    from tests.util.assert_df import assert_equal_df
except ImportError:
    # pandas and spark can be missing if someone runs tests for file connections only
    pass

pytestmark = [pytest.mark.local_fs, pytest.mark.file_df_connection, pytest.mark.connection]


def test_jsonline_reader(
    local_fs_file_df_connection_with_path_and_files,
    file_df_dataframe,
):
    """Reading JSONLine files working as expected on any Spark, Python and Java versions"""
    local_fs, source_path, _ = local_fs_file_df_connection_with_path_and_files
    df = file_df_dataframe
    jsonline_root = source_path / "jsonline/without_compression"

    reader = FileReader(
        connection=local_fs,
        format=JSONLine(),
        df_schema=df.schema,
        source_path=jsonline_root,
    )
    read_df = reader.run()

    assert read_df.count()
    assert read_df.schema == df.schema
    assert_equal_df(read_df, df)


def test_jsonline_reader_with_compression(
    local_fs_file_df_connection_with_path_and_files,
    file_df_dataframe,
):
    local_fs, source_path, _ = local_fs_file_df_connection_with_path_and_files
    df = file_df_dataframe
    jsonline_root = source_path / "jsonline/with_compression"

    reader = FileReader(
        connection=local_fs,
        format=JSONLine(compression="gzip"),
        df_schema=df.schema,
        source_path=jsonline_root,
    )
    read_df = reader.run()

    assert read_df.count()
    assert read_df.schema == df.schema
    assert_equal_df(read_df, df)


def test_jsonline_writer(
    local_fs_file_df_connection_with_path,
    file_df_dataframe,
):
    """Written files can be read by Spark"""
    file_df_connection, source_path = local_fs_file_df_connection_with_path
    df = file_df_dataframe
    jsonline_root = source_path / "jsonline"

    writer = FileWriter(
        connection=file_df_connection,
        format=JSONLine(),
        target_path=jsonline_root,
    )
    writer.run(df)

    reader = FileReader(
        connection=file_df_connection,
        format=JSONLine(),
        source_path=jsonline_root,
        df_schema=df.schema,
    )
    read_df = reader.run()

    assert read_df.count()
    assert read_df.schema == df.schema
    assert_equal_df(read_df, df)


def test_jsonline_writer_with_compression(
    local_fs_file_df_connection_with_path,
    file_df_dataframe,
):
    """Written files can be read by Spark"""
    file_df_connection, source_path = local_fs_file_df_connection_with_path
    df = file_df_dataframe
    jsonline_root = source_path / "jsonline"

    jsonline = JSONLine(compression="gzip")

    writer = FileWriter(
        connection=file_df_connection,
        format=jsonline,
        target_path=jsonline_root,
    )
    writer.run(df)

    reader = FileReader(
        connection=file_df_connection,
        format=jsonline,
        source_path=jsonline_root,
        df_schema=df.schema,
    )
    read_df = reader.run()

    assert read_df.count()
    assert read_df.schema == df.schema
    assert_equal_df(read_df, df)
