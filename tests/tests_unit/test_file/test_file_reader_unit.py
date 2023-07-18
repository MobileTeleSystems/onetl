from onetl.file import FileReader


def test_file_reader_unknown_options():
    options = FileReader.Options(unknown="abc")
    assert options.unknown == "abc"
