<a id="json-file-format"></a>

# JSON

### *class* onetl.file.format.json.JSON(\*, multiLine: Literal[True] = True, encoding: str | None = None, lineSep: str | None = None, allowComments: bool | None = None, allowUnquotedFieldNames: bool | None = None, allowSingleQuotes: bool | None = None, allowNumericLeadingZeros: bool | None = None, allowNonNumericNumbers: bool | None = None, allowBackslashEscapingAnyCharacter: bool | None = None, allowUnquotedControlChars: bool | None = None, mode: Literal['PERMISSIVE', 'DROPMALFORMED', 'FAILFAST'] | None = None, columnNameOfCorruptRecord: ConstrainedStrValue | None = None, samplingRatio: ConstrainedFloatValue | None = None, primitivesAsString: bool | None = None, prefersDecimal: bool | None = None, dropFieldIfAllNull: bool | None = None, dateFormat: ConstrainedStrValue | None = None, timestampFormat: ConstrainedStrValue | None = None, timestampNTZFormat: ConstrainedStrValue | None = None, timeZone: ConstrainedStrValue | None = None, locale: ConstrainedStrValue | None = None, \*\*kwargs)

JSON file format. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Based on [Spark JSON](https://spark.apache.org/docs/latest/sql-data-sources-json.html) file format.

Supports reading (but **NOT** writing) files with `.json` extension with content like:

```json
[
    {"key": "value1"},
    {"key": "value2"}
]
```

#### Versionadded
Added in version 0.9.0.

### Examples

#### NOTE
You can pass any option mentioned in
[official documentation](https://spark.apache.org/docs/latest/sql-data-sources-json.html).
**Option names should be in** `camelCase`!

The set of supported options depends on Spark version.

Reading files:

```python
from onetl.file.format import JSON

json = JSON(encoding="UTF-8")
```

Writing files:

#### WARNING
Not supported. Use [`JSONLine`](jsonline.md#onetl.file.format.jsonline.JSONLine).

<!-- !! processed by numpydoc !! -->

#### *field* encoding *: str | None* *= None*

Encoding of the JSON file.
Default `UTF-8`.

#### NOTE
Used only for reading and writing files.

Ignored by [`parse_column`](#onetl.file.format.json.JSON.parse_column) and [`serialize_column`](#onetl.file.format.json.JSON.serialize_column) methods.

<!-- !! processed by numpydoc !! -->

#### *field* lineSep *: str | None* *= None*

Character used to separate lines in the JSON file.

Defaults:
: * Try to detect for reading (`\r\n`, `\r`, `\n`)
  * `\n` for writing

#### NOTE
Used only for reading and writing files.

Ignored by [`parse_column`](#onetl.file.format.json.JSON.parse_column) and [`serialize_column`](#onetl.file.format.json.JSON.serialize_column) methods,
as they handle each DataFrame row separately.

<!-- !! processed by numpydoc !! -->

#### *field* allowComments *: bool | None* *= None*

If `True`, add support for C/C++/Java style comments (`//`, `/* */`).
Default `False`, meaning that JSON files should not contain comments.

#### NOTE
Used only for reading files and [`parse_column`](#onetl.file.format.json.JSON.parse_column) method.

<!-- !! processed by numpydoc !! -->

#### *field* allowUnquotedFieldNames *: bool | None* *= None*

If `True`, allow JSON object field names without quotes (JavaScript-style).
Default `False`.

#### NOTE
Used only for reading files and [`parse_column`](#onetl.file.format.json.JSON.parse_column) method.

<!-- !! processed by numpydoc !! -->

#### *field* allowSingleQuotes *: bool | None* *= None*

If `True`, allow JSON object field names to be wrapped with single quotes (`'`).
Default `True`.

#### NOTE
Used only for reading files and [`parse_column`](#onetl.file.format.json.JSON.parse_column) method.

<!-- !! processed by numpydoc !! -->

#### *field* allowNumericLeadingZeros *: bool | None* *= None*

If `True`, allow leading zeros in numbers (e.g. `00012`).
Default `False`.

#### NOTE
Used only for reading files and [`parse_column`](#onetl.file.format.json.JSON.parse_column) method.

<!-- !! processed by numpydoc !! -->

#### *field* allowNonNumericNumbers *: bool | None* *= None*

If `True`, allow numbers to contain non-numeric characters, like:
: * scientific notation (e.g. `12e10`).
  * positive infinity floating point value (`Infinity`, `+Infinity`, `+INF`).
  * negative infinity floating point value (`-Infinity`, `-INF`).
  * Not-a-Number floating point value (`NaN`).

Default `True`.

#### NOTE
Used only for reading files and [`parse_column`](#onetl.file.format.json.JSON.parse_column) method.

<!-- !! processed by numpydoc !! -->

#### *field* allowBackslashEscapingAnyCharacter *: bool | None* *= None*

If `True`, prefix `\` can escape any character.
Default `False`.

#### NOTE
Used only for reading files and [`parse_column`](#onetl.file.format.json.JSON.parse_column) method.

<!-- !! processed by numpydoc !! -->

#### *field* allowUnquotedControlChars *: bool | None* *= None*

If `True`, allow unquoted control characters (ASCII values 0-31) in strings without escaping them with `\`.
Default `False`.

#### NOTE
Used only for reading files and [`parse_column`](#onetl.file.format.json.JSON.parse_column) method.

<!-- !! processed by numpydoc !! -->

#### *field* mode *: Literal['PERMISSIVE', 'DROPMALFORMED', 'FAILFAST'] | None* *= None*

How to handle parsing errors:
: * `PERMISSIVE` - set field value as `null`, move raw data to [`columnNameOfCorruptRecord`](#onetl.file.format.json.JSON.columnNameOfCorruptRecord) column.
  * `DROPMALFORMED` - skip the malformed row.
  * `FAILFAST` - throw an error immediately.

Default is `PERMISSIVE`.

#### NOTE
Used only for reading files and [`parse_column`](#onetl.file.format.json.JSON.parse_column) method.

<!-- !! processed by numpydoc !! -->

#### *field* columnNameOfCorruptRecord *: str | None* *= None*

Name of column to put corrupt records in.
Default is `_corrupt_record`.

#### WARNING
If DataFrame schema is provided, this column should be added to schema explicitly:

```python
from onetl.connection import SparkLocalFS
from onetl.file import FileDFReader
from onetl.file.format import JSON

from pyspark.sql.types import StructType, StructField, TimestampType, StringType

spark = ...

schema = StructType(
    [
        StructField("my_field", TimestampType()),
        StructField("_corrupt_record", StringType()),  # <-- important
    ]
)

json = JSON(mode="PERMISSIVE", columnNameOfCorruptRecord="_corrupt_record")

reader = FileDFReader(
    connection=connection,
    format=json,
    schema=schema,  # < ---
)
df = reader.run(["/some/file.json"])
```

#### NOTE
Used only for reading files and [`parse_column`](#onetl.file.format.json.JSON.parse_column) method.

<!-- !! processed by numpydoc !! -->
* **Constraints:**
  - **minLength** = 1

#### *field* samplingRatio *: float | None* *= None*

While inferring schema, read the specified fraction of file rows.
Default `1`.

#### NOTE
Used only for reading files. Ignored by [`parse_column`](#onetl.file.format.json.JSON.parse_column) function.

<!-- !! processed by numpydoc !! -->
* **Constraints:**
  - **minimum** = 0
  - **maximum** = 1

#### *field* primitivesAsString *: bool | None* *= None*

If `True`, infer all primitive types (string, integer, float, boolean) as strings.
Default `False`.

#### NOTE
Used only for reading files. Ignored by [`parse_column`](#onetl.file.format.json.JSON.parse_column) method.

<!-- !! processed by numpydoc !! -->

#### *field* prefersDecimal *: bool | None* *= None*

If `True`, infer all floating-point values as `Decimal`.
Default `False`.

#### NOTE
Used only for reading files. Ignored by [`parse_column`](#onetl.file.format.json.JSON.parse_column) method.

<!-- !! processed by numpydoc !! -->

#### \_\_init_\_(\*\*kwargs)

<!-- !! processed by numpydoc !! -->

#### *field* dropFieldIfAllNull *: bool | None* *= None*

If `True` and inferred column is always null or empty array, exclude if from DataFrame schema.
Default `False`.

#### NOTE
Used only for reading files. Ignored by [`parse_column`](#onetl.file.format.json.JSON.parse_column) method.

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

#### *field* timezone *: str | None* *= None* *(alias 'timeZone')*

Allows to override timezone used for parsing or serializing date and timestamp values.
By default, `spark.sql.session.timeZone` is used.

<!-- !! processed by numpydoc !! -->
* **Constraints:**
  - **minLength** = 1

#### *field* locale *: str | None* *= None*

Locale name used to parse dates and timestamps.
Default is `en-US`.

#### NOTE
Used only for reading files and [`parse_column`](#onetl.file.format.json.JSON.parse_column) method.

<!-- !! processed by numpydoc !! -->
* **Constraints:**
  - **minLength** = 1

#### parse_column(column: str | Column, schema: StructType | ArrayType | MapType) → Column

Parses a JSON string column to a structured Spark SQL column using Spark’s [from_json](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_json.html) function, based on the provided schema.

#### Versionadded
Added in version 0.11.0.

* **Parameters:**
  **column**
  : The name of the column or the column object containing JSON strings/bytes to parse.

  **schema**
  : The schema to apply when parsing the JSON data. This defines the structure of the output DataFrame column.
* **Returns:**
  Column with deserialized data, with the same structure as the provided schema. Column name is the same as input column.

### Examples

```pycon
>>> from pyspark.sql.types import StructType, StructField, IntegerType, StringType
>>> from pyspark.sql.functions import decode
>>> from onetl.file.format import JSON
>>> df.show()
+----+--------------------+----------+---------+------+-----------------------+-------------+
|key |value               |topic     |partition|offset|timestamp              |timestampType|
+----+--------------------+----------+---------+------+-----------------------+-------------+
|[31]|[7B 22 6E 61 6D 6...|topicJSON |0        |0     |2024-04-24 16:51:11.739|0            |
|[32]|[7B 22 6E 61 6D 6...|topicJSON |0        |1     |2024-04-24 16:51:11.749|0            |
+----+--------------------+----------+---------+------+-----------------------+-------------+
>>> df.printSchema()
root
|-- key: binary (nullable = true)
|-- value: binary (nullable = true)
|-- topic: string (nullable = true)
|-- partition: integer (nullable = true)
|-- offset: integer (nullable = true)
|-- timestamp: timestamp (nullable = true)
|-- timestampType: integer (nullable = true)
>>> json = JSON()
>>> json_schema = StructType(
...     [
...         StructField("name", StringType(), nullable=True),
...         StructField("age", IntegerType(), nullable=True),
...     ],
... )
>>> parsed_df = df.select(decode("key", "UTF-8").alias("key"), json.parse_column("value", json_schema))
>>> parsed_df.show()
+---+-----------+
|key|value      |
+---+-----------+
|1  |{Alice, 20}|
|2  |  {Bob, 25}|
+---+-----------+
>>> parsed_df.printSchema()
root
|-- key: string (nullable = true)
|-- value: struct (nullable = true)
|    |-- name: string (nullable = true)
|    |-- age: integer (nullable = true)
```

<!-- !! processed by numpydoc !! -->

#### serialize_column(column: str | Column) → Column

Serializes a structured Spark SQL column into a JSON string column using Spark’s
[to_json](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_json.html) function.

#### Versionadded
Added in version 0.11.0.

* **Parameters:**
  **column**
  : The name of the column or the column object containing the data to serialize to JSON format.
* **Returns:**
  Column with string JSON data. Column name is the same as input column.

### Examples

```pycon
>>> from pyspark.sql.functions import decode
>>> from onetl.file.format import JSON
>>> df.show()
+---+-----------+
|key|value      |
+---+-----------+
|1  |{Alice, 20}|
|2  |  {Bob, 25}|
+---+-----------+
>>> df.printSchema()
root
|-- key: string (nullable = true)
|-- value: struct (nullable = true)
|    |-- name: string (nullable = true)
|    |-- age: integer (nullable = true)
>>> # serializing data into JSON format
>>> json = JSON()
>>> serialized_df = df.select("key", json.serialize_column("value"))
>>> serialized_df.show(truncate=False)
+---+-------------------------+
|key|value                    |
+---+-------------------------+
|  1|{"name":"Alice","age":20}|
|  2|{"name":"Bob","age":25}  |
+---+-------------------------+
>>> serialized_df.printSchema()
root
|-- key: string (nullable = true)
|-- value: string (nullable = true)
```

<!-- !! processed by numpydoc !! -->
