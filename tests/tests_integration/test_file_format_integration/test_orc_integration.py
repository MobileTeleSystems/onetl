"""Integration tests for ORC file format.

Test only that options are passed to Spark in both FileReader & FileWriter.
Do not test all the possible options and combinations, we are not testing Spark here.
"""

import pytest

from onetl.file import FileReader, FileWriter
from onetl.file.format import ORC

try:
    from tests.util.assert_df import assert_equal_df
except ImportError:
    # pandas and spark can be missing if someone runs tests for file connections only
    pass

pytestmark = [pytest.mark.local_fs, pytest.mark.file_df_connection, pytest.mark.connection]


def test_orc_reader(
    local_fs_file_df_connection_with_path_and_files,
    file_df_dataframe,
):
    """Reading ORC files working as expected on any Spark, Python and Java versions"""
    local_fs, source_path, _ = local_fs_file_df_connection_with_path_and_files
    df = file_df_dataframe
    orc_root = source_path / "orc/without_compression"

    reader = FileReader(
        connection=local_fs,
        format=ORC(),
        df_schema=df.schema,
        source_path=orc_root,
    )
    read_df = reader.run()

    assert read_df.count()
    assert read_df.schema == df.schema
    assert_equal_df(read_df, df)


def test_orc_reader_with_compression(
    local_fs_file_df_connection_with_path_and_files,
    file_df_dataframe,
):
    local_fs, source_path, _ = local_fs_file_df_connection_with_path_and_files
    df = file_df_dataframe
    orc_root = source_path / "orc/with_compression"

    reader = FileReader(
        connection=local_fs,
        format=ORC(compression="snappy"),
        df_schema=df.schema,
        source_path=orc_root,
    )
    read_df = reader.run()

    assert read_df.count()
    assert read_df.schema == df.schema
    assert_equal_df(read_df, df)


def test_orc_writer(
    local_fs_file_df_connection_with_path,
    file_df_dataframe,
):
    """Written files can be read by Spark"""
    file_df_connection, source_path = local_fs_file_df_connection_with_path
    df = file_df_dataframe
    orc_root = source_path / "orc"

    writer = FileWriter(
        connection=file_df_connection,
        format=ORC(),
        target_path=orc_root,
    )
    writer.run(df)

    reader = FileReader(
        connection=file_df_connection,
        format=ORC(),
        source_path=orc_root,
        df_schema=df.schema,
    )
    read_df = reader.run()

    assert read_df.count()
    assert read_df.schema == df.schema
    assert_equal_df(read_df, df)


def test_orc_writer_with_compression(
    local_fs_file_df_connection_with_path,
    file_df_dataframe,
):
    file_df_connection, source_path = local_fs_file_df_connection_with_path
    df = file_df_dataframe
    orc_root = source_path / "orc"

    orc = ORC(compression="snappy")

    writer = FileWriter(
        connection=file_df_connection,
        format=orc,
        target_path=orc_root,
    )
    writer.run(df)

    reader = FileReader(
        connection=file_df_connection,
        format=orc,
        source_path=orc_root,
        df_schema=df.schema,
    )
    read_df = reader.run()

    assert read_df.count()
    assert read_df.schema == df.schema
    assert_equal_df(read_df, df)
