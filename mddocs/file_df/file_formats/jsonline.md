<a id="jsonline-file-format"></a>

# JSONLine

### *class* onetl.file.format.jsonline.JSONLine(\*, multiLine: Literal[False] = False, encoding: str | None = None, lineSep: str | None = None, compression: str | Literal['none', 'bzip2', 'gzip', 'lz4', 'snappy', 'deflate'] | None = None, ignoreNullFields: bool | None = None, allowComments: bool | None = None, allowUnquotedFieldNames: bool | None = None, allowSingleQuotes: bool | None = None, allowNumericLeadingZeros: bool | None = None, allowNonNumericNumbers: bool | None = None, allowBackslashEscapingAnyCharacter: bool | None = None, allowUnquotedControlChars: bool | None = None, mode: Literal['PERMISSIVE', 'DROPMALFORMED', 'FAILFAST'] | None = None, columnNameOfCorruptRecord: ConstrainedStrValue | None = None, samplingRatio: ConstrainedFloatValue | None = None, primitivesAsString: bool | None = None, prefersDecimal: bool | None = None, dropFieldIfAllNull: bool | None = None, dateFormat: ConstrainedStrValue | None = None, timestampFormat: ConstrainedStrValue | None = None, timestampNTZFormat: ConstrainedStrValue | None = None, timeZone: ConstrainedStrValue | None = None, locale: ConstrainedStrValue | None = None, \*\*kwargs)

JSONLine file format (each line of file contains a JSON object). [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Based on [Spark JSON](https://spark.apache.org/docs/latest/sql-data-sources-json.html) file format.

Supports reading/writing files with `.json` extension with content like:

```json
{"key": "value1"}
{"key": "value2"}
```

#### Versionadded
Added in version 0.9.0.

### Examples

#### NOTE
You can pass any option mentioned in
[official documentation](https://spark.apache.org/docs/latest/sql-data-sources-json.html).
**Option names should be in** `camelCase`!

The set of supported options depends on Spark version.

Reading files

```py
from onetl.file.format import JSONLine

jsonline = JSONLine(encoding="UTF-8", mode="PERMISSIVE")
```

Writing files

#### WARNING
Written files have extension `.json`, not `.jsonl` or `.jsonline`.

```python
from onetl.file.format import JSONLine

jsonline = JSONLine(encoding="UTF-8", compression="gzip")
```

<!-- !! processed by numpydoc !! -->

#### *field* encoding *: str | None* *= None*

Encoding of the JSONLine files.
Default `UTF-8`.

<!-- !! processed by numpydoc !! -->

#### *field* lineSep *: str | None* *= None*

Character used to separate lines in the JSONLine files.

Defaults:
: * Try to detect for reading (`\r\n`, `\r`, `\n`)
  * `\n` for writing.

<!-- !! processed by numpydoc !! -->

#### *field* compression *: str | Literal['none', 'bzip2', 'gzip', 'lz4', 'snappy', 'deflate'] | None* *= None*

Compression codec of the JSONLine file.
Default `none`.

#### NOTE
Used only for writing files.

<!-- !! processed by numpydoc !! -->

#### *field* ignoreNullFields *: bool | None* *= None*

If `True` and field value is `null`, don’t add field into resulting object
Default is value of `spark.sql.jsonGenerator.ignoreNullFields` (`True`).

#### NOTE
Used only for writing files.

<!-- !! processed by numpydoc !! -->

#### *field* allowComments *: bool | None* *= None*

If `True`, add support for C/C++/Java style comments (`//`, `/* */`).
Default `False`, meaning that JSONLine files should not contain comments.

#### NOTE
Used only for reading files.

<!-- !! processed by numpydoc !! -->

#### *field* allowUnquotedFieldNames *: bool | None* *= None*

If `True`, allow JSON object field names without quotes (JavaScript-style).
Default `False`.

#### NOTE
Used only for reading files.

<!-- !! processed by numpydoc !! -->

#### *field* allowSingleQuotes *: bool | None* *= None*

If `True`, allow JSON object field names to be wrapped with single quotes (`'`).
Default `True`.

#### NOTE
Used only for reading files.

<!-- !! processed by numpydoc !! -->

#### *field* allowNumericLeadingZeros *: bool | None* *= None*

If `True`, allow leading zeros in numbers (e.g. `00012`).
Default `False`.

#### NOTE
Used only for reading files.

<!-- !! processed by numpydoc !! -->

#### *field* allowNonNumericNumbers *: bool | None* *= None*

If `True`, allow numbers to contain non-numeric characters, like:
: * scientific notation (e.g. `12e10`).
  * positive infinity floating point value (`Infinity`, `+Infinity`, `+INF`).
  * negative infinity floating point value (`-Infinity`, `-INF`).
  * Not-a-Number floating point value (`NaN`).

Default `True`.

#### NOTE
Used only for reading files.

<!-- !! processed by numpydoc !! -->

#### *field* allowBackslashEscapingAnyCharacter *: bool | None* *= None*

If `True`, prefix `\` can escape any character.
Default `False`.

#### NOTE
Used only for reading files.

<!-- !! processed by numpydoc !! -->

#### *field* allowUnquotedControlChars *: bool | None* *= None*

If `True`, allow unquoted control characters (ASCII values 0-31) in strings without escaping them with `\`.
Default `False`.

#### NOTE
Used only for reading files.

<!-- !! processed by numpydoc !! -->

#### *field* mode *: Literal['PERMISSIVE', 'DROPMALFORMED', 'FAILFAST'] | None* *= None*

How to handle parsing errors:
: * `PERMISSIVE` - set field value as `null`, move raw data to [`columnNameOfCorruptRecord`](#onetl.file.format.jsonline.JSONLine.columnNameOfCorruptRecord) column.
  * `DROPMALFORMED` - skip the malformed row.
  * `FAILFAST` - throw an error immediately.

Default is `PERMISSIVE`.

#### NOTE
Used only for reading files.

<!-- !! processed by numpydoc !! -->

#### *field* columnNameOfCorruptRecord *: str | None* *= None*

Name of column to put corrupt records in.
Default is `_corrupt_record`.

#### WARNING
If DataFrame schema is provided, this column should be added to schema explicitly:

```python
from onetl.connection import SparkLocalFS
from onetl.file import FileDFReader
from onetl.file.format import JSONLine

from pyspark.sql.types import StructType, StructField, TimestampType, StringType

spark = ...

schema = StructType(
    [
        StructField("my_field", TimestampType()),
        StructField("_corrupt_record", StringType()),  # <-- important
    ]
)

jsonline = JSONLine(mode="PERMISSIVE", columnNameOfCorruptRecord="_corrupt_record")

reader = FileDFReader(
    connection=connection,
    format=jsonline,
    schema=schema,  # < ---
)
df = reader.run(["/some/file.jsonl"])
```

#### NOTE
Used only for reading files.

<!-- !! processed by numpydoc !! -->
* **Constraints:**
  - **minLength** = 1

#### \_\_init_\_(\*\*kwargs)

<!-- !! processed by numpydoc !! -->

#### *field* samplingRatio *: float | None* *= None*

While inferring schema, read the specified fraction of file rows.
Default `1`.

#### NOTE
Used only for reading files.

<!-- !! processed by numpydoc !! -->
* **Constraints:**
  - **minimum** = 0
  - **maximum** = 1

#### *field* primitivesAsString *: bool | None* *= None*

If `True`, infer all primitive types (string, integer, float, boolean) as strings.
Default `False`.

#### NOTE
Used only for reading files.

<!-- !! processed by numpydoc !! -->

#### *field* prefersDecimal *: bool | None* *= None*

If `True`, infer all floating-point values as `Decimal`.
Default `False`.

#### NOTE
Used only for reading files.

<!-- !! processed by numpydoc !! -->

#### *field* dropFieldIfAllNull *: bool | None* *= None*

If `True` and inferred column is always null or empty array, exclude if from DataFrame schema.
Default `False`.

#### NOTE
Used only for reading files.

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
Used only for reading files.

<!-- !! processed by numpydoc !! -->
* **Constraints:**
  - **minLength** = 1
