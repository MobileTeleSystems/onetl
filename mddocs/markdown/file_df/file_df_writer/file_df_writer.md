<a id="file-df-writer"></a>

# FileDF Writer

### *class* onetl.file.file_df_writer.file_df_writer.FileDFWriter(\*, connection: ~onetl.base.base_file_df_connection.BaseFileDFConnection, format: ~onetl.base.base_file_format.BaseWritableFileFormat, target_path: ~onetl.base.pure_path_protocol.PurePathProtocol, options: ~onetl.file.file_df_writer.options.FileDFWriterOptions = FileDFWriterOptions(if_exists=<FileDFExistBehavior.APPEND: 'append'>, partition_by=None))

Allows you to write Spark DataFrame as files in a target path of specified file connection
with parameters. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

* **Parameters:**
  **connection**
  : File DataFrame connection. See [File DataFrame Connections](../../connection/file_df_connection/index.md#file-df-connections) section.

  **format**
  : File format to write.

  **target_path**
  : Directory path to write data to.

  **options**
  : Common writing options.

### Examples

Write CSV files to local filesystem

```py
from onetl.connection import SparkLocalFS
from onetl.file import FileDFWriter
from onetl.file.format import CSV

local_fs = SparkLocalFS(spark=spark)

writer = FileDFWriter(
    connection=local_fs,
    format=CSV(delimiter=","),
    target_path="/path/to/directory",
)
```

All supported options

```py
from onetl.connection import SparkLocalFS
from onetl.file import FileDFWriter
from onetl.file.format import CSV

csv = CSV(delimiter=",")
local_fs = SparkLocalFS(spark=spark)

writer = FileDFWriter(
    connection=local_fs,
    format=csv,
    target_path="/path/to/directory",
    options=FileDFWriter.Options(if_exists="replace_entire_directory"),
)
```

<!-- !! processed by numpydoc !! -->

#### run(df: DataFrame) → None

Method for writing DataFrame as files. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### NOTE
Method does support only **batching** DataFrames.

* **Parameters:**
  **df**
  : Spark dataframe

### Examples

Write df to target:

```python
writer.run(df)
```

<!-- !! processed by numpydoc !! -->
