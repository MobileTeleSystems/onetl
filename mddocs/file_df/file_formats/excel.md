<a id="excel-file-format"></a>

# Excel

### *class* onetl.file.format.excel.Excel(\*, header: bool = False, dataAddress: str | None = None, timestampFormat: str | None = None, dateFormat: str | None = None, treatEmptyValuesAsNulls: bool | None = None, setErrorCellsToFallbackValues: bool | None = None, usePlainNumberFormat: bool | None = None, inferSchema: bool | None = None, workbookPassword: SecretStr | None = None, maxRowsInMemory: int | None = None, maxByteArraySize: ByteSize | None = None, tempFileThreshold: ByteSize | None = None, excerptSize: int | None = None, \*\*kwargs)

Excel file format. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Based on [Spark Excel](https://github.com/crealytics/spark-excel) file format.

Supports reading/writing files with `.xlsx` (read/write) and `.xls` (read only) extensions.

### Version compatibility

* Spark versions: 3.2.x - 3.5.x
  > #### WARNING
  > Not all combinations of Spark version and package version are supported.
  > See [Maven index](https://mvnrepository.com/artifact/com.crealytics/spark-excel)
  > and [official documentation](https://github.com/crealytics/spark-excel).
* Java versions: 8 - 20

See documentation from link above.

#### Versionadded
Added in version 0.9.4.

### Examples

#### NOTE
You can pass any option mentioned in
[official documentation](https://github.com/crealytics/spark-excel).
**Option names should be in** `camelCase`!

The set of supported options depends on `spark-excel` package version.

Reading files

```py
from pyspark.sql import SparkSession
from onetl.file.format import Excel

# Create Spark session with Excel package loaded
maven_packages = Excel.get_packages(spark_version="3.5.1")
spark = (
    SparkSession.builder.appName("spark-app-name")
    .config("spark.jars.packages", ",".join(maven_packages))
    .getOrCreate()
)

excel = Excel(header=True, inferSchema=True)
```

Writing files

```py
# Create Spark session with Excel package loaded
spark = ...

from onetl.file.format import XML

excel = Excel(header=True, dataAddress="'Sheet1'!A1")
```

<!-- !! processed by numpydoc !! -->

#### *field* header *: bool* *= False*

If `True`, the first row in file is conditioned as a header.
Default `False`.

<!-- !! processed by numpydoc !! -->

#### *field* dataAddress *: str | None* *= None*

Cell address used as starting point.
For example: `'A1'` or `'Sheet1'!A1`

<!-- !! processed by numpydoc !! -->

#### *field* timestampFormat *: str | None* *= None*

Format string used for parsing or serializing timestamp values.
Default `yyyy-mm-dd hh:mm:ss[.fffffffff]`.

<!-- !! processed by numpydoc !! -->

#### *field* treatEmptyValuesAsNulls *: bool | None* *= None*

If `True`, empty cells are parsed as `null` values.
If `False`, empty cells are parsed as empty strings.
Default `True`.

#### NOTE
Used only for reading files.

<!-- !! processed by numpydoc !! -->

#### *field* setErrorCellsToFallbackValues *: bool | None* *= None*

If `True`, cells containing `#N/A` value are replaced with default value for column type,
e.g. 0 for `IntegerType()`. If `False`, `#N/A` values are replaced with `null`.
Default `False`.

#### NOTE
Used only for reading files.

<!-- !! processed by numpydoc !! -->

#### *field* usePlainNumberFormat *: bool | None* *= None*

If `True`, read or write numeric values with plain format, without using scientific notation or rounding.
Default `False`.

<!-- !! processed by numpydoc !! -->

#### *field* inferSchema *: bool | None* *= None*

If `True`, infer DataFrame schema based on cell content.
If `False` and no explicit DataFrame schema is passed, all columns are `StringType()`.

#### NOTE
Used only for reading files.

<!-- !! processed by numpydoc !! -->

#### *field* workbookPassword *: SecretStr | None* *= None*

If Excel file is encrypted, provide password to open it.

#### NOTE
Used only for reading files. Cannot be used to write files.

<!-- !! processed by numpydoc !! -->
* **Constraints:**
  - **type** = string
  - **writeOnly** = True
  - **format** = password

#### *field* maxRowsInMemory *: int | None* *= None*

If set, use streaming reader and fetch only specified number of rows per iteration.
This reduces memory usage for large files.
Default `None`, which means reading the entire file content to memory.

#### WARNING
Can be used only with `.xlsx` files, but fails on `.xls`.

#### NOTE
Used only for reading files.

<!-- !! processed by numpydoc !! -->

#### *field* maxByteArraySize *: ByteSize | None* *= None*

If set, overrides memory limit (in bytes) of byte array size used for reading rows from input file.
Default `0`, which means using default limit.

See [IOUtils.setByteArrayMaxOverride](https://poi.apache.org/apidocs/5.0/org/apache/poi/util/IOUtils.html#setByteArrayMaxOverride-int-)
documentation.

#### NOTE
Used only for reading files.

<!-- !! processed by numpydoc !! -->

#### *field* tempFileThreshold *: ByteSize | None* *= None*

If value is greater than 0, large zip entries will be written to temporary files after reaching this threshold.
If value is 0, all zip entries will be written to temporary files.
If value is -1, no temp files will be created, which may cause errors if zip entry is larger than 2GiB.

#### NOTE
Used only for reading files.

<!-- !! processed by numpydoc !! -->

#### *field* excerptSize *: int | None* *= None*

If `inferSchema=True`, set number of rows to infer schema from.
Default `10`.

#### NOTE
Used only for reading files.

<!-- !! processed by numpydoc !! -->

#### *classmethod* get_packages(spark_version: str, scala_version: str | None = None, package_version: str | None = None) → list[str]

Get package names to be downloaded by Spark. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### WARNING
Not all combinations of Spark version and package version are supported.
See [Maven index](https://mvnrepository.com/artifact/com.crealytics/spark-excel)
and [official documentation](https://github.com/crealytics/spark-excel).

#### Versionadded
Added in version 0.9.4.

* **Parameters:**
  **spark_version**
  : Spark version in format `major.minor.patch`.

  **scala_version**
  : Scala version in format `major.minor`.
    <br/>
    If `None`, `spark_version` is used to determine Scala version.

  **package_version**
  : Package version in format `major.minor.patch`. Default is `0.20.4`.
    <br/>
    #### WARNING
    Version `0.14` and below are not supported.
    <br/>
    #### NOTE
    It is not guaranteed that custom package versions are supported.
    Tests are performed only for default version.

### Examples

```python
from onetl.file.format import Excel

Excel.get_packages(spark_version="3.5.1")
Excel.get_packages(spark_version="3.5.1", scala_version="2.12")
Excel.get_packages(
    spark_version="3.5.1",
    scala_version="2.12",
    package_version="0.20.4",
)
```

<!-- !! processed by numpydoc !! -->
