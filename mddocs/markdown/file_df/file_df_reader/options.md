<a id="file-df-reader-options"></a>

# Options

### *class* onetl.file.file_df_reader.options.FileDFReaderOptions(\*, recursiveFileLookup: bool | None = None, \*\*kwargs)

Options for [`FileDFReader`](file_df_reader.md#onetl.file.file_df_reader.file_df_reader.FileDFReader).

#### Versionadded
Added in version 0.9.0.

### Examples

#### NOTE
You can pass any value [supported by Spark](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html),
even if it is not mentioned in this documentation. **Option names should be in** `camelCase`!

The set of supported options depends on Spark version.

```python
from onetl.file import FileDFReader

options = FileDFReader.Options(recursive=True)
```

<!-- !! processed by numpydoc !! -->

#### *field* recursive *: bool | None* *= None* *(alias 'recursiveFileLookup')*

If `True`, perform recursive file lookup.

#### WARNING
This disables partition inferring using file paths.

#### WARNING
Can be used only in Spark 3+. See [SPARK-27990](https://issues.apache.org/jira/browse/SPARK-27990).

<!-- !! processed by numpydoc !! -->
