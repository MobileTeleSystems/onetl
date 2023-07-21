import pytest

from onetl.file import FileWriter


@pytest.mark.parametrize(
    "option, value",
    [
        ("if_exists", "error"),
        ("partition_by", "month"),
        ("partition_by", ["year", "month"]),
        ("unknown", "abc"),
    ],
)
def test_file_writer_options(option, value):
    options = FileWriter.Options(**{option: value})
    assert getattr(options, option) == value


def test_file_writer_options_mode_prohibited():
    with pytest.raises(ValueError):
        FileWriter.Options(mode="error")


@pytest.mark.parametrize(
    "mode, recommended",
    [
        ("dynamic", "replace_overlapping_partitions"),
        ("static", "replace_entire_directory"),
    ],
)
def test_file_writer_options_unsupported_partition_overwrite(mode, recommended):
    error_msg = f"`partitionOverwriteMode` option should be replaced with if_exists='{recommended}'"

    with pytest.raises(ValueError, match=error_msg):
        FileWriter.Options(partitionOverwriteMode=mode)

    with pytest.raises(ValueError, match=error_msg):
        FileWriter.Options(partition_overwrite_mode=mode)
