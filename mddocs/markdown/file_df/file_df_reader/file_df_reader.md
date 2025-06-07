<a id="file-df-reader"></a>

# FileDF Reader

### *class* onetl.file.file_df_reader.file_df_reader.FileDFReader(\*, connection: [BaseFileDFConnection](../../connection/file_df_connection/base.md#onetl.base.base_file_df_connection.BaseFileDFConnection), format: [BaseReadableFileFormat](../file_formats/base.md#onetl.base.base_file_format.BaseReadableFileFormat), source_path: PurePathProtocol | None = None, df_schema: StructType | None = None, options: [FileDFReaderOptions](options.md#onetl.file.file_df_reader.options.FileDFReaderOptions) = FileDFReaderOptions(recursive=None))

Allows you to read files from a source path with specified file connection
and parameters, and return a Spark DataFrame. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### WARNING
This class does **not** support read strategies.

#### Versionadded
Added in version 0.9.0.

* **Parameters:**
  **connection**
  : File DataFrame connection. See [File DataFrame Connections](../../connection/file_df_connection/index.md#file-df-connections) section.

  **format**
  : File format to read.

  **source_path**
  : Directory path to read data from.
    <br/>
    Could be `None`, but only if you pass file paths directly to
    [`run`](#onetl.file.file_df_reader.file_df_reader.FileDFReader.run) method

  **df_schema**
  : Spark DataFrame schema.

  **options**
  : Common reading options.

### Examples

Read CSV files from local filesystem

```py
from onetl.connection import SparkLocalFS
from onetl.file import FileDFReader
from onetl.file.format import CSV

csv = CSV(delimiter=",")
local_fs = SparkLocalFS(spark=spark)

reader = FileDFReader(
    connection=local_fs,
    format=csv,
    source_path="/path/to/directory",
)
```

All supported options

```py
from onetl.connection import SparkLocalFS
from onetl.file import FileDFReader
from onetl.file.format import CSV

csv = CSV(delimiter=",")
local_fs = SparkLocalFS(spark=spark)

reader = FileDFReader(
    connection=local_fs,
    format=csv,
    source_path="/path/to/directory",
    options=FileDFReader.Options(recursive=False),
)
```

<!-- !! processed by numpydoc !! -->

#### run(files: Iterable[str | os.PathLike] | None = None) → DataFrame

Method for reading files as DataFrame. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.9.0.

* **Parameters:**
  **files**
  : File list to read.
    <br/>
    If empty, read files from `source_path`.
* **Returns:**
  **df**
  : Spark DataFrame

### Examples

Read CSV files from directory `/path`:

```python
from onetl.connection import SparkLocalFS
from onetl.file import FileDFReader
from onetl.file.format import CSV

csv = CSV(delimiter=",")
local_fs = SparkLocalFS(spark=spark)

reader = FileDFReader(
    connection=local_fs,
    format=csv,
    source_path="/path",
)
df = reader.run()
```

Read some CSV files using file paths:

```python
from onetl.connection import SparkLocalFS
from onetl.file import FileDFReader
from onetl.file.format import CSV

csv = CSV(delimiter=",")
local_fs = SparkLocalFS(spark=spark)

reader = FileDFReader(
    connection=local_fs,
    format=csv,
)

df = reader.run(
    [
        "/path/file1.csv",
        "/path/nested/file2.csv",
    ]
)
```

Read only specific CSV files in directory:

```python
from onetl.connection import SparkLocalFS
from onetl.file import FileDFReader
from onetl.file.format import CSV

csv = CSV(delimiter=",")
local_fs = SparkLocalFS(spark=spark)

reader = FileDFReader(
    connection=local_fs,
    format=csv,
    source_path="/path",
)

df = reader.run(
    [
        # file paths could be relative
        "/path/file1.csv",
        "/path/nested/file2.csv",
    ]
)
```

<!-- !! processed by numpydoc !! -->
