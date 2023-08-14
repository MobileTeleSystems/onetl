import re

import pytest

from onetl.file import FileDFWriter


@pytest.mark.parametrize(
    "option, value",
    [
        ("if_exists", "error"),
        ("partition_by", "month"),
        ("partition_by", ["year", "month"]),
        ("unknown", "abc"),
    ],
)
def test_file_df_writer_options(option, value):
    options = FileDFWriter.Options.parse({option: value})
    assert getattr(options, option) == value


def test_file_df_writer_options_mode_prohibited():
    msg = re.escape("Parameter `mode` is not allowed. Please use `if_exists` parameter instead.")
    with pytest.raises(ValueError, match=msg):
        FileDFWriter.Options(mode="error")


@pytest.mark.parametrize(
    "mode, recommended",
    [
        ("dynamic", "replace_overlapping_partitions"),
        ("static", "replace_entire_directory"),
    ],
)
def test_file_df_writer_options_unsupported_partition_overwrite(mode, recommended):
    error_msg = f"`partitionOverwriteMode` option should be replaced with if_exists='{recommended}'"

    with pytest.raises(ValueError, match=error_msg):
        FileDFWriter.Options(partitionOverwriteMode=mode)

    with pytest.raises(ValueError, match=error_msg):
        FileDFWriter.Options(partition_overwrite_mode=mode)
