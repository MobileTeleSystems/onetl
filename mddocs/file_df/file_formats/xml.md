<a id="xml-file-format"></a>

# XML

### *class* onetl.file.format.xml.XML(\*, rowTag: str, rootTag: str | None = None, compression: str | Literal['bzip2', 'gzip', 'lz4', 'snappy'] | None = None, timestampFormat: str | None = None, dateFormat: str | None = None, timezone: str | None = None, nullValue: str | None = None, ignoreSurroundingSpaces: bool | None = None, mode: Literal['PERMISSIVE', 'DROPMALFORMED', 'FAILFAST'] | None = None, columnNameOfCorruptRecord: str | None = None, inferSchema: bool | None = None, samplingRatio: ConstrainedFloatValue | None = None, charset: str | None = None, valueTag: str | None = None, attributePrefix: str | None = None, excludeAttribute: bool | None = None, wildcardColName: str | None = None, ignoreNamespace: bool | None = None, rowValidationXSDPath: str | None = None, declaration: str | None = None, arrayElementName: str | None = None, \*\*kwargs)

XML file format. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Based on [Databricks Spark XML](https://github.com/databricks/spark-xml) file format.

Supports reading/writing files with `.xml` extension.

#### Versionadded
Added in version 0.9.5.

### Version compatibility

* Spark versions: 3.2.x - 3.5.x
* Java versions: 8 - 20

See [official documentation](https://github.com/databricks/spark-xml).

### Examples

#### NOTE
You can pass any option mentioned in
[official documentation](https://github.com/databricks/spark-xml).
**Option names should be in** `camelCase`!

The set of supported options depends on `spark-xml` version.

Reading files

```py
from onetl.file.format import XML
from pyspark.sql import SparkSession

# Create Spark session with XML package loaded
maven_packages = XML.get_packages(spark_version="3.5.5")
spark = (
    SparkSession.builder.appName("spark-app-name")
    .config("spark.jars.packages", ",".join(maven_packages))
    .getOrCreate()
)

xml = XML(rowTag="item", mode="PERMISSIVE")
```

Writing files

#### WARNING
Due to [bug](https://github.com/databricks/spark-xml/issues/664) written files currently does not have `.xml` extension.

```python
# Create Spark session with XML package loaded
spark = ...

from onetl.file.format import XML

xml = XML(rowTag="item", rootTag="data", compression="gzip")
```

<!-- !! processed by numpydoc !! -->

#### *field* row_tag *: str* *[Required]* *(alias 'rowTag')*

XML tag that encloses each row in XML. Required.

<!-- !! processed by numpydoc !! -->

#### *field* rootTag *: str | None* *= None*

XML tag that encloses content of all DataFrame. Default is `ROWS`.

#### NOTE
Used only for writing files.

<!-- !! processed by numpydoc !! -->

#### *field* compression *: str | Literal['bzip2', 'gzip', 'lz4', 'snappy'] | None* *= None*

Compression codec. By default no compression is used.

#### NOTE
Used only for writing files.

<!-- !! processed by numpydoc !! -->

#### *field* timestampFormat *: str | None* *= None*

Format string used for parsing or serializing timestamp values.
By default, ISO 8601 format is used (`yyyy-MM-ddTHH:mm:ss.SSSZ`).

<!-- !! processed by numpydoc !! -->

#### *field* dateFormat *: str | None* *= None*

Format string used for parsing or serializing date values.
By default, ISO 8601 format is used (`yyyy-MM-dd`).

<!-- !! processed by numpydoc !! -->

#### *field* nullValue *: str | None* *= None*

String value used to represent null. Default is `null` string.

<!-- !! processed by numpydoc !! -->

#### *field* ignoreSurroundingSpaces *: bool | None* *= None*

If `True`, trim surrounding spaces while parsing values. Default `false`.

#### NOTE
Used only for reading files or by [`parse_column`](#onetl.file.format.xml.XML.parse_column) function.

<!-- !! processed by numpydoc !! -->

#### *field* mode *: Literal['PERMISSIVE', 'DROPMALFORMED', 'FAILFAST'] | None* *= None*

How to handle parsing errors:
: * `PERMISSIVE` - set field value as `null`, move raw data to [`columnNameOfCorruptRecord`](#onetl.file.format.xml.XML.columnNameOfCorruptRecord) column.
  * `DROPMALFORMED` - skip the malformed row.
  * `FAILFAST` - throw an error immediately.

Default is `PERMISSIVE`.

#### NOTE
Used only for reading files or by [`parse_column`](#onetl.file.format.xml.XML.parse_column) function.

<!-- !! processed by numpydoc !! -->

#### *field* columnNameOfCorruptRecord *: str | None* *= None*

Name of DataFrame column there corrupted row is stored with `mode=PERMISSIVE`.

#### WARNING
If DataFrame schema is provided, this column should be added to schema explicitly:

```python
from onetl.connection import SparkLocalFS
from onetl.file import FileDFReader
from onetl.file.format import XML

from pyspark.sql.types import StructType, StructField, TImestampType, StringType

spark = ...
schema = StructType(
    [
        StructField("my_field", TimestampType()),
        StructField("_corrupt_record", StringType()),  # <-- important
    ]
)
xml = XML(rowTag="item", columnNameOfCorruptRecord="_corrupt_record")

reader = FileDFReader(
    connection=connection,
    format=xml,
    schema=schema,  # < ---
)
df = reader.run(["/some/file.xml"])
```

#### NOTE
Used only for reading files or by [`parse_column`](#onetl.file.format.xml.XML.parse_column) function.

<!-- !! processed by numpydoc !! -->

#### *field* inferSchema *: bool | None* *= None*

If `True`, try to infer the input schema by reading a sample of the file (see [`samplingRatio`](#onetl.file.format.xml.XML.samplingRatio)).
Default `False` which means that all parsed columns will be `StringType()`.

#### NOTE
Used only for reading files. Ignored by [`parse_column`](#onetl.file.format.xml.XML.parse_column) function.

<!-- !! processed by numpydoc !! -->

#### *field* samplingRatio *: float | None* *= None*

For `inferSchema=True`, read the specified fraction of rows to infer the schema.
Default `1`.

#### NOTE
Used only for reading files. Ignored by [`parse_column`](#onetl.file.format.xml.XML.parse_column) function.

<!-- !! processed by numpydoc !! -->
* **Constraints:**
  - **minimum** = 0
  - **maximum** = 1

#### *field* charset *: str | None* *= None*

File encoding. Default is `UTF-8`

#### NOTE
Used only for reading files or by [`parse_column`](#onetl.file.format.xml.XML.parse_column) function.

<!-- !! processed by numpydoc !! -->

#### *field* valueTag *: str | None* *= None*

Value used to replace missing values while parsing attributes like `<sometag someattr>`.
Default `_VALUE`.

#### NOTE
Used only for reading files or by [`parse_column`](#onetl.file.format.xml.XML.parse_column) function.

<!-- !! processed by numpydoc !! -->

#### *field* attributePrefix *: str | None* *= None*

While parsing tags containing attributes like `<sometag attr="value">`, attributes are stored as
DataFrame schema columns with specified prefix, e.g. `_attr`.
Default `_`.

#### NOTE
Used only for reading files or by [`parse_column`](#onetl.file.format.xml.XML.parse_column) function.

<!-- !! processed by numpydoc !! -->

#### *field* excludeAttribute *: bool | None* *= None*

If `True`, exclude attributes while parsing tags like `<sometag attr="value">`.
Default `false`.

#### NOTE
Used only for reading files or by [`parse_column`](#onetl.file.format.xml.XML.parse_column) function.

<!-- !! processed by numpydoc !! -->

#### *field* wildcardColName *: str | None* *= None*

Name of column or columns which should be preserved as raw XML string, and not parsed.

#### WARNING
If DataFrame schema is provided, this column should be added to schema explicitly.
See [`columnNameOfCorruptRecord`](#onetl.file.format.xml.XML.columnNameOfCorruptRecord) example.

#### NOTE
Used only for reading files or by [`parse_column`](#onetl.file.format.xml.XML.parse_column) function.

<!-- !! processed by numpydoc !! -->

#### *field* ignoreNamespace *: bool | None* *= None*

If `True`, all namespaces like `<ns:tag>` will be ignored and treated as just `<tag>`.
Default `False`.

#### NOTE
Used only for reading files or by [`parse_column`](#onetl.file.format.xml.XML.parse_column) function.

<!-- !! processed by numpydoc !! -->

#### *field* rowValidationXSDPath *: str | None* *= None*

Path to XSD file which should be used to validate each row.
If row does not match XSD, it will be treated as error, behavior depends on [`mode`](#onetl.file.format.xml.XML.mode) value.

Default is no validation.

#### NOTE
If Spark session is created with `master=yarn` or `master=k8s`, XSD
file should be accessible from all Spark nodes. This can be achieved by calling:

```python
spark.addFile("/path/to/file.xsd")
```

And then by passing `rowValidationXSDPath=file.xsd` (relative path).

#### NOTE
Used only for reading files or by [`parse_column`](#onetl.file.format.xml.XML.parse_column) function.

<!-- !! processed by numpydoc !! -->

#### *field* declaration *: str | None* *= None*

Content of <?XML … ?> declaration.
Default is `version="1.0" encoding="UTF-8" standalone="yes"`.

#### NOTE
Used only for writing files.

<!-- !! processed by numpydoc !! -->

#### *field* arrayElementName *: str | None* *= None*

If DataFrame column is `ArrayType`, its content will be written to XML
inside `<arrayElementName>...</arrayElementName>` tag.
Default is `item`.

#### NOTE
Used only for writing files.

<!-- !! processed by numpydoc !! -->

#### *classmethod* get_packages(spark_version: str, scala_version: str | None = None, package_version: str | None = None) → list[str]

Get package names to be downloaded by Spark. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.9.5.

* **Parameters:**
  **spark_version**
  : Spark version in format `major.minor.patch`.

  **scala_version**
  : Scala version in format `major.minor`.
    <br/>
    If `None`, `spark_version` is used to determine Scala version.

  **package_version**
  : Package version in format `major.minor.patch`. Default is `0.18.0`.
    <br/>
    See [Maven index](https://mvnrepository.com/artifact/com.databricks/spark-xml)
    for list of available versions.
    <br/>
    #### WARNING
    Version `0.13` and below are not supported.
    <br/>
    #### NOTE
    It is not guaranteed that custom package versions are supported.
    Tests are performed only for default version.

### Examples

```python
from onetl.file.format import XML

XML.get_packages(spark_version="3.5.5")
XML.get_packages(spark_version="3.5.5", scala_version="2.12")
XML.get_packages(
    spark_version="3.5.5",
    scala_version="2.12",
    package_version="0.18.0",
)
```

<!-- !! processed by numpydoc !! -->

#### parse_column(column: str | Column, schema: StructType) → Column

Parses an XML string column into a structured Spark SQL column using the `from_xml` function
provided by the [Databricks Spark XML library](https://github.com/databricks/spark-xml#pyspark-notes)
based on the provided schema.

#### NOTE
This method assumes that the `spark-xml` package is installed: [`get_packages`](#onetl.file.format.xml.XML.get_packages).

#### NOTE
This method parses each DataFrame row individually. Therefore, for a specific column, each row must contain exactly one occurrence of the `rowTag` specified.
If your XML data includes a root tag that encapsulates multiple row tags, you can adjust the schema to use an `ArrayType` to keep all child elements under the single root.

```xml
<books>
    <book><title>Book One</title><author>Author A</author></book>
    <book><title>Book Two</title><author>Author B</author></book>
</books>
```

And the corresponding schema in Spark using an `ArrayType`:

```python
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
from onetl.file.format import XML

# each DataFrame row has exactly one <books> tag
xml = XML(rowTag="books")
# each <books> tag have multiple <book> tags, so using ArrayType for such field
schema = StructType(
    [
        StructField(
            "book",
            ArrayType(
                StructType(
                    [
                        StructField("title", StringType(), nullable=True),
                        StructField("author", StringType(), nullable=True),
                    ],
                ),
            ),
            nullable=True,
        ),
    ],
)
```

#### Versionadded
Added in version 0.11.0.

* **Parameters:**
  **column**
  : The name of the column or the column object containing XML strings/bytes to parse.

  **schema**
  : The schema to apply when parsing the XML data. This defines the structure of the output DataFrame column.
* **Returns:**
  Column with deserialized data, with the same structure as the provided schema. Column name is the same as input column.

### Examples

```pycon
>>> from pyspark.sql.types import StructType, StructField, IntegerType, StringType
>>> from onetl.file.format import XML
>>> df.show()
+--+------------------------------------------------+
|id|value                                           |
+--+------------------------------------------------+
|1 |<person><name>Alice</name><age>20</age></person>|
|2 |<person><name>Bob</name><age>25</age></person>  |
+--+------------------------------------------------+
>>> df.printSchema()
root
|-- id: integer (nullable = true)
|-- value: string (nullable = true)
>>> xml = XML(rowTag="person")
>>> xml_schema = StructType(
...     [
...         StructField("name", StringType(), nullable=True),
...         StructField("age", IntegerType(), nullable=True),
...     ],
... )
>>> parsed_df = df.select("key", xml.parse_column("value", xml_schema))
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
