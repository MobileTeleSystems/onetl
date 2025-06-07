<a id="csv-file-format"></a>

# CSV

### *class* onetl.file.format.csv.CSV(\*, sep: str = ',', header: bool | None = None, quote: ConstrainedStrValue = '"', quoteAll: bool | None = None, escape: ConstrainedStrValue = '\\\\', lineSep: ConstrainedStrValue | None = None, encoding: ConstrainedStrValue | None = None, compression: str | Literal['none', 'bzip2', 'gzip', 'lz4', 'snappy', 'deflate'] | None = None, inferSchema: bool | None = None, samplingRatio: ConstrainedFloatValue | None = None, comment: ConstrainedStrValue | None = None, enforceSchema: bool | None = None, escapeQuotes: bool | None = None, unescapedQuoteHandling: None | Literal['STOP_AT_CLOSING_QUOTE', 'BACK_TO_DELIMITER', 'STOP_AT_DELIMITER', 'SKIP_VALUE', 'RAISE_ERROR'] = None, ignoreLeadingWhiteSpace: bool | None = None, ignoreTrailingWhiteSpace: bool | None = None, emptyValue: str | None = None, nullValue: str | None = None, nanValue: str | None = None, positiveInf: ConstrainedStrValue | None = None, negativeInf: ConstrainedStrValue | None = None, preferDate: bool | None = None, dateFormat: ConstrainedStrValue | None = None, timestampFormat: ConstrainedStrValue | None = None, timestampNTZFormat: ConstrainedStrValue | None = None, locale: ConstrainedStrValue | None = None, maxCharsPerColumn: int | None = None, mode: Literal['PERMISSIVE', 'DROPMALFORMED', 'FAILFAST'] | None = None, columnNameOfCorruptRecord: ConstrainedStrValue | None = None, multiLine: bool | None = None, charToEscapeQuoteEscaping: ConstrainedStrValue | None = None, \*\*kwargs)

CSV file format. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Based on [Spark CSV](https://spark.apache.org/docs/latest/sql-data-sources-csv.html) file format.

Supports reading/writing files with `.csv` extension with content like:

```csv
"some","value"
"another","value"
```

#### Versionadded
Added in version 0.9.0.

### Examples

#### NOTE
You can pass any option mentioned in
[official documentation](https://spark.apache.org/docs/latest/sql-data-sources-csv.html).
**Option names should be in** `camelCase`!

The set of supported options depends on Spark version.

Reading files

```py
from onetl.file.format import CSV

csv = CSV(header=True, inferSchema=True, mode="PERMISSIVE")
```

Writing files

```py
from onetl.file.format import CSV

csv = CSV(header=True, compression="gzip")
```

<!-- !! processed by numpydoc !! -->

#### *field* delimiter *: str* *= ','* *(alias 'sep')*

Character used to separate fields in CSV row.

<!-- !! processed by numpydoc !! -->

#### *field* header *: bool | None* *= None*

If `True`, the first row of the file is considered a header.
Default `False`.

<!-- !! processed by numpydoc !! -->

#### *field* quote *: str* *= '"'*

Character used to quote field values within CSV field.

Empty string is considered as `\u0000` (`NUL`) character.

<!-- !! processed by numpydoc !! -->
* **Constraints:**
  - **maxLength** = 1

#### *field* quoteAll *: bool | None* *= None*

If `True`, all fields are quoted:

```csv
"some","field with \"quote","123",""
```

If `False`, only quote fields containing [`quote`](#onetl.file.format.csv.CSV.quote) symbols.

```csv
any,"field with \"quote",123,
```

Default `False`.

#### NOTE
Used only for writing files.

<!-- !! processed by numpydoc !! -->

#### *field* compression *: str | Literal['none', 'bzip2', 'gzip', 'lz4', 'snappy', 'deflate'] | None* *= None*

Compression codec of the CSV file.
Default `none`.

#### NOTE
Used only for writing files. Ignored by [`parse_column`](#onetl.file.format.csv.CSV.parse_column) method.

<!-- !! processed by numpydoc !! -->

#### *field* inferSchema *: bool | None* *= None*

If `True`, try to infer the input schema by reading a sample of the file (see [`samplingRatio`](#onetl.file.format.csv.CSV.samplingRatio)).
Default `False` which means that all parsed columns will be `StringType()`.

#### NOTE
Used only for reading files, and only if user haven’t provider explicit DataFrame schema.
Ignored by [`parse_column`](#onetl.file.format.csv.CSV.parse_column) function.

<!-- !! processed by numpydoc !! -->

#### *field* samplingRatio *: float | None* *= None*

For `inferSchema=True`, read the specified fraction of rows to infer the schema.
Default `1`.

#### NOTE
Used only for reading files. Ignored by [`parse_column`](#onetl.file.format.csv.CSV.parse_column) function.

<!-- !! processed by numpydoc !! -->
* **Constraints:**
  - **minimum** = 0
  - **maximum** = 1

#### *field* comment *: str | None* *= None*

If set, all lines starting with specified character (e.g. `#`) are considered a comment, and skipped.
Default is not set, meaning that CSV lines should not contain comments.

#### NOTE
Used only for reading files and [`parse_column`](#onetl.file.format.csv.CSV.parse_column) method.

<!-- !! processed by numpydoc !! -->
* **Constraints:**
  - **maxLength** = 1

#### *field* enforceSchema *: bool | None* *= None*

If `True`, inferred or user-provided schema has higher priority than CSV file headers.
This means that all input files should have the same structure.

If `False`, CSV headers are used as a primary source of information about column names and their position.

Default `True`.

#### NOTE
Used only for reading files. Ignored by [`parse_column`](#onetl.file.format.csv.CSV.parse_column) function.

<!-- !! processed by numpydoc !! -->

#### *field* escapeQuotes *: bool | None* *= None*

If `True`, escape quotes within CSV field.

```csv
any,field with \"quote,123,
```

If `False`, wrap fields containing [`quote`](#onetl.file.format.csv.CSV.quote) symbols with quotes.

```csv
any,"field with ""quote ",123,
```

Default `True`.

#### NOTE
Used only for writing files.

<!-- !! processed by numpydoc !! -->

#### *field* unescapedQuoteHandling *: None | Literal['STOP_AT_CLOSING_QUOTE', 'BACK_TO_DELIMITER', 'STOP_AT_DELIMITER', 'SKIP_VALUE', 'RAISE_ERROR']* *= None*

Define how to handle unescaped quotes within CSV field.

* `STOP_AT_CLOSING_QUOTE` - collect all characters until closing quote.
* `BACK_TO_DELIMITER` - collect all characters until delimiter or line end.
* `STOP_AT_DELIMITER` - collect all characters until delimiter or line end.
  : If quotes are not closed, this may produce incorrect results (e.g. including delimiter inside field value).
* `SKIP_VALUE` - skip field and consider it as [`nullValue`](#onetl.file.format.csv.CSV.nullValue).
* `RAISE_ERROR` - raise error immediately.

Default `STOP_AT_DELIMITER`.

#### NOTE
Used only for reading files and [`parse_column`](#onetl.file.format.csv.CSV.parse_column) method.

<!-- !! processed by numpydoc !! -->

#### *field* ignoreLeadingWhiteSpace *: bool | None* *= None*

If `True`, trim leading whitespaces in field value.

Defaults:
: * `True` for reading.
  * `False` for writing.

<!-- !! processed by numpydoc !! -->

#### *field* ignoreTrailingWhiteSpace *: bool | None* *= None*

If `True`, trim trailing whitespaces in field value.

Defaults:
: * `True` for reading.
  * `False` for writing.

<!-- !! processed by numpydoc !! -->

#### *field* emptyValue *: str | None* *= None*

Value used for empty string fields.

Defaults:
: * empty string for reading.
  * `""` for writing.

<!-- !! processed by numpydoc !! -->

#### *field* nullValue *: str | None* *= None*

If set, this value will be converted to `null`.
Default is empty string.

<!-- !! processed by numpydoc !! -->

#### *field* nanValue *: str | None* *= None*

If set, this string will be considered as Not-A-Number (NaN) value for `FloatType()` and `DoubleType()`.
Default is `NaN`.

#### NOTE
Used only for reading files and [`parse_column`](#onetl.file.format.csv.CSV.parse_column) method.

<!-- !! processed by numpydoc !! -->

#### *field* positiveInf *: str | None* *= None*

If set, this string will be considered as positive infinity value for `FloatType()` and `DoubleType()`.
Default is `Inf`.

#### NOTE
Used only for reading files and [`parse_column`](#onetl.file.format.csv.CSV.parse_column) method.

<!-- !! processed by numpydoc !! -->
* **Constraints:**
  - **minLength** = 1

#### \_\_init_\_(\*\*kwargs)

<!-- !! processed by numpydoc !! -->

#### *field* negativeInf *: str | None* *= None*

If set, this string will be considered as negative infinity value for `FloatType()` and `DoubleType()`.
Default is `-Inf`.

#### NOTE
Used only for reading files and [`parse_column`](#onetl.file.format.csv.CSV.parse_column) method.

<!-- !! processed by numpydoc !! -->
* **Constraints:**
  - **minLength** = 1

#### *field* preferDate *: bool | None* *= None*

If `True` and `inferSchema=True` and column does match [`dateFormat`](#onetl.file.format.csv.CSV.dateFormat), consider it as `DateType()`.
For columns matching both [`timestampFormat`](#onetl.file.format.csv.CSV.timestampFormat) and [`dateFormat`](#onetl.file.format.csv.CSV.dateFormat), consider it as `TimestampType()`.

If `False`, date and timestamp columns will be considered as `StringType()`.

Default `True`.

#### NOTE
Used only for reading files. Ignored by [`parse_column`](#onetl.file.format.csv.CSV.parse_column) function.

<!-- !! processed by numpydoc !! -->

#### *field* dateFormat *: str | None* *= None*

String format for `DateType()` representation.
Default is `yyyy-MM-dd`.

<!-- !! processed by numpydoc !! -->
* **Constraints:**
  - **minLength** = 1

#### *field* timestampFormat *: str | None* *= None*

String format for TimestampType()\` representation.
Default is `yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]`.

<!-- !! processed by numpydoc !! -->
* **Constraints:**
  - **minLength** = 1

#### *field* timestampNTZFormat *: str | None* *= None*

String format for TimestampNTZType()\` representation.
Default is `yyyy-MM-dd'T'HH:mm:ss[.SSS]`.

#### NOTE
Added in Spark 3.2.0

<!-- !! processed by numpydoc !! -->
* **Constraints:**
  - **minLength** = 1

#### *field* locale *: str | None* *= None*

Locale name used to parse dates and timestamps.
Default is `en-US`

#### NOTE
Used only for reading files and [`parse_column`](#onetl.file.format.csv.CSV.parse_column) method.

<!-- !! processed by numpydoc !! -->
* **Constraints:**
  - **minLength** = 1

#### *field* maxCharsPerColumn *: int | None* *= None*

Maximum number of characters to read per column.
Default is `-1`, which means no limit.

#### NOTE
Used only for reading files and [`parse_column`](#onetl.file.format.csv.CSV.parse_column) method.

<!-- !! processed by numpydoc !! -->

#### *field* mode *: Literal['PERMISSIVE', 'DROPMALFORMED', 'FAILFAST'] | None* *= None*

How to handle parsing errors:
: * `PERMISSIVE` - set field value as `null`, move raw data to [`columnNameOfCorruptRecord`](#onetl.file.format.csv.CSV.columnNameOfCorruptRecord) column.
  * `DROPMALFORMED` - skip the malformed row.
  * `FAILFAST` - throw an error immediately.

Default is `PERMISSIVE`.

#### NOTE
Used only for reading files and [`parse_column`](#onetl.file.format.csv.CSV.parse_column) method.

<!-- !! processed by numpydoc !! -->

#### *field* columnNameOfCorruptRecord *: str | None* *= None*

Name of column to put corrupt records in.
Default is `_corrupt_record`.

#### WARNING
If DataFrame schema is provided, this column should be added to schema explicitly:

```python
from onetl.connection import SparkLocalFS
from onetl.file import FileDFReader
from onetl.file.format import CSV

from pyspark.sql.types import StructType, StructField, TimestampType, StringType

spark = ...

schema = StructType(
    [
        StructField("my_field", TimestampType()),
        StructField("_corrupt_record", StringType()),  # <-- important
    ]
)

csv = CSV(mode="PERMISSIVE", columnNameOfCorruptRecord="_corrupt_record")

reader = FileDFReader(
    connection=connection,
    format=csv,
    schema=schema,  # < ---
)
df = reader.run(["/some/file.csv"])
```

#### NOTE
Used only for reading files and [`parse_column`](#onetl.file.format.csv.CSV.parse_column) method.

<!-- !! processed by numpydoc !! -->
* **Constraints:**
  - **minLength** = 1

#### *field* multiLine *: bool | None* *= None*

If `True`, fields may contain line separators.
If `False`, the input is expected to have one record per file.

Default is `True`.

#### NOTE
Used only for reading files.
Ignored by [`parse_column`](#onetl.file.format.csv.CSV.parse_column) method, as it expects that each DataFrame row will contain exactly one CSV line.

<!-- !! processed by numpydoc !! -->

#### *field* charToEscapeQuoteEscaping *: str | None* *= None*

If CSV field value contains `escape` character, it should be escaped as well.
For example, if `escape="\"`, when line:

```csv
"some \" quoted value",other
"some \\ backslashed value",another
```

will be parsed as:

```python
[
    ('some " quoted value', "other"),
    ("some \ backslashed value", "another"),
]
```

And vice-versa, for writing CSV rows to file.

Default is same as `escape`.

<!-- !! processed by numpydoc !! -->
* **Constraints:**
  - **maxLength** = 1

#### parse_column(column: str | Column, schema: StructType) → Column

Parses a CSV string column to a structured Spark SQL column using Spark’s
[from_csv](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_csv.html) function, based on the provided schema.

#### NOTE
Can be used only with Spark 3.x+

#### Versionadded
Added in version 0.11.0.

* **Parameters:**
  **column**
  : The name of the column or the column object containing CSV strings/bytes to parse.

  **schema**
  : The schema to apply when parsing the CSV data. This defines the structure of the output DataFrame column.
* **Returns:**
  Column with deserialized data, with the same structure as the provided schema. Column name is the same as input column.

### Examples

```pycon
>>> from pyspark.sql.types import StructType, StructField, IntegerType, StringType
>>> from onetl.file.format import CSV
>>> df.show()
+--+--------+
|id|value   |
+--+--------+
|1 |Alice;20|
|2 |Bob;25  |
+--+--------+
>>> df.printSchema()
root
|-- id: integer (nullable = true)
|-- value: string (nullable = true)
>>> csv = CSV(delimiter=";")
>>> csv_schema = StructType(
...     [
...         StructField("name", StringType(), nullable=True),
...         StructField("age", IntegerType(), nullable=True),
...     ],
... )
>>> parsed_df = df.select("id", csv.parse_column("value", csv_schema))
>>> parsed_df.show()
+--+-----------+
|id|value      |
+--+-----------+
|1 |{Alice, 20}|
|2 |  {Bob, 25}|
+--+-----------+
>>> parsed_df.printSchema()
root
|-- id: integer (nullable = true)
|-- value: struct (nullable = true)
|    |-- name: string (nullable = true)
|    |-- age: integer (nullable = true)
```

<!-- !! processed by numpydoc !! -->

#### serialize_column(column: str | Column) → Column

Serializes a structured Spark SQL column into a CSV string column using Spark’s
[to_csv](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_csv.html) function.

#### NOTE
Can be used only with Spark 3.x+

#### Versionadded
Added in version 0.11.0.

* **Parameters:**
  **column**
  : The name of the column or the Column object containing the data to serialize to CSV.
* **Returns:**
  Column with string CSV data. Column name is the same as input column.

### Examples

```pycon
>>> from pyspark.sql.functions import decode
>>> from onetl.file.format import CSV
>>> df.show()
+--+-----------+
|id|value      |
+--+-----------+
|1 |{Alice, 20}|
|2 |  {Bob, 25}|
+--+-----------+
>>> df.printSchema()
root
|-- id: integer (nullable = true)
|-- value: struct (nullable = true)
|    |-- name: string (nullable = true)
|    |-- age: integer (nullable = true)
>>> # serializing data into CSV format
>>> csv = CSV(delimiter=";")
>>> serialized_df = df.select("id", csv.serialize_column("value"))
>>> serialized_df.show(truncate=False)
+--+--------+
|id|value   |
+--+--------+
|1 |Alice;20|
|2 |Bob;25  |
+--+--------+
>>> serialized_df.printSchema()
root
|-- id: integer (nullable = true)
|-- value: string (nullable = true)
```

<!-- !! processed by numpydoc !! -->
