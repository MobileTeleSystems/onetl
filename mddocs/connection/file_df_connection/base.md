<a id="base-file-df-connection"></a>

# Base interface

### *class* onetl.base.base_file_df_connection.BaseFileDFConnection

Implements generic methods for reading  and writing dataframe as files.

#### Versionadded
Added in version 0.9.0.

<!-- !! processed by numpydoc !! -->

#### *abstract* check() → T

Check source availability. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

If not, an exception will be raised.

* **Returns:**
  Connection itself
* **Raises:**
  RuntimeError
  : If the connection is not available

### Examples

```python
connection.check()
```

<!-- !! processed by numpydoc !! -->

#### *abstract* check_if_format_supported(format: [BaseReadableFileFormat](../../file_df/file_formats/base.md#onetl.base.base_file_format.BaseReadableFileFormat) | [BaseWritableFileFormat](../../file_df/file_formats/base.md#onetl.base.base_file_format.BaseWritableFileFormat)) → None

Validate if specific file format is supported. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.9.0.

* **Raises:**
  RuntimeError
  : If file format is not supported.

<!-- !! processed by numpydoc !! -->

#### *abstract* read_files_as_df(paths: list[PurePathProtocol], format: [BaseReadableFileFormat](../../file_df/file_formats/base.md#onetl.base.base_file_format.BaseReadableFileFormat), root: PurePathProtocol | None = None, df_schema: StructType | None = None, options: FileDFReadOptions | None = None) → DataFrame

Read files in some paths list as dataframe. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.9.0.

<!-- !! processed by numpydoc !! -->

#### *abstract* write_df_as_files(df: DataFrame, path: PurePathProtocol, format: [BaseWritableFileFormat](../../file_df/file_formats/base.md#onetl.base.base_file_format.BaseWritableFileFormat), options: FileDFWriteOptions | None = None) → None

Write dataframe as files in some path. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.9.0.

<!-- !! processed by numpydoc !! -->
