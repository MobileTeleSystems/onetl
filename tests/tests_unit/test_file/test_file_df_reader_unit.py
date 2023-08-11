from onetl.file import FileDFReader


def test_file_df_reader_unknown_options():
    options = FileDFReader.Options(unknown="abc")
    assert options.unknown == "abc"
