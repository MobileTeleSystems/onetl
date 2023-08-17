"""Integration tests for JSON file format.

Test only that options are passed to Spark in both FileDFReader & FileDFWriter.
Do not test all the possible options and combinations, we are not testing Spark here.
"""

import pytest

from onetl.file import FileDFReader, FileDFWriter
from onetl.file.format import JSON

try:
    from tests.util.assert_df import assert_equal_df
except ImportError:
    # pandas and spark can be missing if someone runs tests for file connections only
    pass

pytestmark = [pytest.mark.local_fs, pytest.mark.file_df_connection, pytest.mark.connection]


@pytest.mark.parametrize(
    "path, options",
    [
        ("without_compression", {}),
        ("with_compression", {"compression": "gzip"}),
    ],
    ids=["without_compression", "with_compression"],
)
def test_json_reader(
    local_fs_file_df_connection_with_path_and_files,
    file_df_dataframe,
    path,
    options,
):
    """Reading JSON files working as expected on any Spark, Python and Java versions"""
    local_fs, source_path, _ = local_fs_file_df_connection_with_path_and_files
    df = file_df_dataframe
    json_root = source_path / "json" / path

    reader = FileDFReader(
        connection=local_fs,
        format=JSON.parse(options),
        df_schema=df.schema,
        source_path=json_root,
    )
    read_df = reader.run()

    assert read_df.count()
    assert read_df.schema == df.schema
    assert_equal_df(read_df, df)


def test_json_writer_is_not_supported(
    local_fs_file_df_connection_with_path,
):
    """Writing JSON files is not supported"""
    file_df_connection, source_path = local_fs_file_df_connection_with_path
    json_root = source_path / "json"

    with pytest.raises(ValueError):
        FileDFWriter(
            connection=file_df_connection,
            format=JSON(),
            target_path=json_root,
        )
