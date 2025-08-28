import pytest

from onetl.file import FileDFReader
from onetl.file.format import Iceberg

try:
    from tests.util.assert_df import assert_equal_df
except ImportError:
    pytest.skip("Missing pandas", allow_module_level=True)

pytestmark = [pytest.mark.s3, pytest.mark.iceberg, pytest.mark.file_df_connection, pytest.mark.connection]


def test_iceberg_reader_s3(
    s3_file_df_connection_with_path_and_files,
    iceberg_file_df,
):
    """Test reading Iceberg table from S3."""
    s3_connection, source_path, _ = s3_file_df_connection_with_path_and_files
    catalog, table, df = iceberg_file_df

    reader = FileDFReader(
        connection=s3_connection,
        format=Iceberg(),
        source_path=source_path / "iceberg" / table,
    )
    read_df = reader.run()

    assert read_df.count() == df.count()
    assert read_df.schema == df.schema
    assert_equal_df(read_df, df, order_by="id")
