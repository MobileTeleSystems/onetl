import pytest

from onetl.file.format import CSV


@pytest.mark.parametrize(
    "prohibited_option",
    [
        "ignoreCorruptFiles",
        "ignoreMissingFiles",
        "modifiedAfter",
        "modifiedBefore",
        "pathGlobFilter",
        "recursiveFileLookup",
    ],
)
@pytest.mark.parametrize(
    "format_class",
    [
        CSV,
    ],
)
def test_file_format_prohibited_options(prohibited_option, format_class):
    msg = rf"Options \['{prohibited_option}'\] are not allowed to use in a {format_class.__name__}"
    with pytest.raises(ValueError, match=msg):
        format_class.parse({prohibited_option: "value"})
