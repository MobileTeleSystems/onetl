<a id="avro-file-format"></a>

# Avro

### *class* onetl.file.format.avro.Avro(\*, avroSchema: dict | None = None, avroSchemaUrl: str | None = None, recordName: str | None = None, recordNamespace: str | None = None, compression: str | Literal['uncompressed', 'snappy', 'deflate', 'bzip2', 'xz', 'zstandard'] | None = None, mode: Literal['PERMISSIVE', 'FAILFAST'] | None = None, datetimeRebaseMode: Literal['CORRECTED', 'LEGACY', 'EXCEPTION'] | None = None, positionalFieldMatching: bool | None = None, enableStableIdentifiersForUnionType: bool | None = None, \*\*kwargs)

Avro file format. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Based on [Spark Avro](https://spark.apache.org/docs/latest/sql-data-sources-avro.html) file format.

Supports reading/writing files with `.avro` extension.

### Version compatibility

* Spark versions: 2.4.x - 3.5.x
* Java versions: 8 - 20

See documentation from link above.

#### Versionadded
Added in version 0.9.0.

### Examples

#### NOTE
You can pass any option mentioned in
[official documentation](https://spark.apache.org/docs/latest/sql-data-sources-avro.html).
**Option names should be in** `camelCase`!

The set of supported options depends on Spark version.

Reading files

```py
from pyspark.sql import SparkSession
from onetl.file.format import Avro

# Create Spark session with Avro package loaded
maven_packages = Avro.get_packages(spark_version="3.5.5")
spark = (
    SparkSession.builder.appName("spark-app-name")
    .config("spark.jars.packages", ",".join(maven_packages))
    .getOrCreate()
)

schema = {
    "type": "record",
    "name": "Person",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"},
    ],
}
avro = Avro(avroSchema=schema)  # or avroSchemaUrl=...
```

Writing files

```py
# Create Spark session with Avro package loaded
spark = ...

from onetl.file.format import Avro

schema = {
    "type": "record",
    "name": "Person",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"},
    ],
}
avro = Avro(
    avroSchema=schema,  # or avroSchemaUrl=...
    compression="snappy",
)
```

<!-- !! processed by numpydoc !! -->

#### *field* schema_dict *: dict | None* *= None* *(alias 'avroSchema')*

Avro schema in JSON format representation.

```python
avro = Avro(
    avroSchema={
        "type": "record",
        "name": "Person",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"},
        ],
    },
)
```

If set, all records should match this schema.

#### WARNING
Mutually exclusive with [`schema_url`](#onetl.file.format.avro.Avro.schema_url).

<!-- !! processed by numpydoc !! -->

#### *field* schema_url *: str | None* *= None* *(alias 'avroSchemaUrl')*

URL to Avro schema in JSON format. Usually points to Schema Registry, like:

```python
schema_registry = "http://some.schema.registry.domain"
name = "MyAwesomeSchema"
version = "latest"

schema_url = f"{schema_registry}/subjects/{name}/versions/{version}/schema"
avro = Avro(avroSchemaUrl=schema_url)
```

If set, schema is fetched before any records are parsed, so all records should match this schema.

#### WARNING
Mutually exclusive with [`schema_dict`](#onetl.file.format.avro.Avro.schema_dict).

<!-- !! processed by numpydoc !! -->

#### *field* recordName *: str | None* *= None*

Record name in written Avro schema.
Default is `topLevelRecord`.

#### NOTE
Used only for writing files and by [`serialize_column`](#onetl.file.format.avro.Avro.serialize_column).

<!-- !! processed by numpydoc !! -->

#### *field* recordNamespace *: str | None* *= None*

Record namespace in written Avro schema. Default is not set.

#### NOTE
Used only for writing files and by [`serialize_column`](#onetl.file.format.avro.Avro.serialize_column).

<!-- !! processed by numpydoc !! -->

#### *field* compression *: str | Literal['uncompressed', 'snappy', 'deflate', 'bzip2', 'xz', 'zstandard'] | None* *= None*

Compression codec.
By default, Spark config value `spark.sql.avro.compression.codec `` (``snappy`) is used.

#### NOTE
Used only for writing files. Ignored by [`serialize_column`](#onetl.file.format.avro.Avro.serialize_column).

<!-- !! processed by numpydoc !! -->

#### *field* mode *: Literal['PERMISSIVE', 'FAILFAST'] | None* *= None*

How to handle parsing errors:
: * `PERMISSIVE` - set field value as `null`.
  * `FAILFAST` - throw an error immediately.

Default is `FAILFAST`.

#### NOTE
Used only by [`parse_column`](#onetl.file.format.avro.Avro.parse_column) method.

<!-- !! processed by numpydoc !! -->

#### *field* datetimeRebaseMode *: Literal['CORRECTED', 'LEGACY', 'EXCEPTION'] | None* *= None*

While converting dates/timestamps from Julian to Proleptic Gregorian calendar, handle value ambiguity:
: * `EXCEPTION` - fail if ancient dates/timestamps are ambiguous between the two calendars.
  * `CORRECTED` - load dates/timestamps without as-is.
  * `LEGACY` - rebase ancient dates/timestamps from the Julian to Proleptic Gregorian calendar.

By default, Spark config value `spark.sql.avro.datetimeRebaseModeInRead` (`CORRECTED`) is used.

#### NOTE
Used only for reading files and by [`parse_column`](#onetl.file.format.avro.Avro.parse_column).

<!-- !! processed by numpydoc !! -->

#### *field* positionalFieldMatching *: bool | None* *= None*

If `True`, match Avro schema field and DataFrame column by position.
If `False`, match by name.

Default is `False`.

<!-- !! processed by numpydoc !! -->

#### *field* enableStableIdentifiersForUnionType *: bool | None* *= None*

Avro schema may contain union types, which are not supported by Spark.
Different variants of union are split to separated DataFrame columns with respective type.

If option value is `True`, DataFrame column names are based on Avro variant names, e.g. `member_int`, `member_string`.
If `False`, DataFrame column names are generated using field position, e.g. `member0`, `member1`.

Default is `False`.

#### NOTE
Used only for reading files and by [`parse_column`](#onetl.file.format.avro.Avro.parse_column).

<!-- !! processed by numpydoc !! -->

#### *classmethod* get_packages(spark_version: str, scala_version: str | None = None) → list[str]

Get package names to be downloaded by Spark. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

See [Maven package index](https://mvnrepository.com/artifact/org.apache.spark/spark-avro)
for all available packages.

#### Versionadded
Added in version 0.9.0.

* **Parameters:**
  **spark_version**
  : Spark version in format `major.minor.patch`.

  **scala_version**
  : Scala version in format `major.minor`.
    <br/>
    If `None`, `spark_version` is used to determine Scala version.

### Examples

```python
from onetl.file.format import Avro

Avro.get_packages(spark_version="3.5.5")
Avro.get_packages(spark_version="3.5.5", scala_version="2.12")
```

<!-- !! processed by numpydoc !! -->

#### parse_column(column: str | Column) → Column

Parses an Avro binary column into a structured Spark SQL column using Spark’s
[from_avro](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.avro.functions.from_avro.html) function,
based on the schema provided within the class.

#### NOTE
Can be used only with Spark 3.x+

#### WARNING
If `schema_url` is provided, `requests` library is used to fetch the schema from the URL.
It should be installed manually, like this:

```bash
pip install requests
```

#### Versionadded
Added in version 0.11.0.

* **Parameters:**
  **column**
  : The name of the column or the column object containing Avro bytes to deserialize.
    Schema should match the provided Avro schema.
* **Returns:**
  Column with deserialized data. Schema is matching the provided Avro schema. Column name is the same as input column.
* **Raises:**
  ValueError
  : If the Spark version is less than 3.x or if neither `avroSchema` nor `avroSchemaUrl` are defined.

  ImportError
  : If `schema_url` is used and the `requests` library is not installed.

### Examples

```pycon
>>> from pyspark.sql.functions import decode
>>> from onetl.file.format import Avro
>>> df.show()
+----+----------------------+----------+---------+------+-----------------------+-------------+
|key |value                 |topic     |partition|offset|timestamp              |timestampType|
+----+----------------------+----------+---------+------+-----------------------+-------------+
|[31]|[0A 41 6C 69 63 65 28]|topicAvro |0        |0     |2024-04-24 13:02:25.911|0            |
|[32]|[06 42 6F 62 32]      |topicAvro |0        |1     |2024-04-24 13:02:25.922|0            |
+----+----------------------+----------+---------+------+-----------------------+-------------+
>>> df.printSchema()
root
|-- key: binary (nullable = true)
|-- value: binary (nullable = true)
|-- topic: string (nullable = true)
|-- partition: integer (nullable = true)
|-- offset: integer (nullable = true)
|-- timestamp: timestamp (nullable = true)
|-- timestampType: integer (nullable = true)
>>> avro = Avro(
...     avroSchema={  # or avroSchemaUrl=...
...         "type": "record",
...         "name": "Person",
...         "fields": [
...             {"name": "name", "type": "string"},
...             {"name": "age", "type": "int"},
...         ],
...     }
... )
>>> parsed_df = df.select(decode("key", "UTF-8").alias("key"), avro.parse_column("value"))
>>> parsed_df.show(truncate=False)
+---+-----------+
|key|value      |
+---+-----------+
|1  |{Alice, 20}|
|2  |{Bob, 25}  |
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

Serializes a structured Spark SQL column into an Avro binary column using Spark’s
[to_avro](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.avro.functions.to_avro.html#pyspark.sql.avro.functions.to_avro) function.

#### NOTE
Can be used only with Spark 3.x+

#### WARNING
If `schema_url` is provided, `requests` library is used to fetch the schema from the URL. It should be installed manually, like this:

```bash
pip install requests
```

#### Versionadded
Added in version 0.11.0.

* **Parameters:**
  **column**
  : The name of the column or the column object containing the data to serialize to Avro format.
* **Returns:**
  Column with binary Avro data. Column name is the same as input column.
* **Raises:**
  ValueError
  : If the Spark version is less than 3.x.

  ImportError
  : If `schema_url` is used and the `requests` library is not installed.

### Examples

```pycon
>>> from pyspark.sql.functions import decode
>>> from onetl.file.format import Avro
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
>>> # serializing data into Avro format
>>> avro = Avro(
...     avroSchema={  # or avroSchemaUrl=...
...         "type": "record",
...         "name": "Person",
...         "fields": [
...             {"name": "name", "type": "string"},
...             {"name": "age", "type": "int"},
...         ],
...     }
... )
>>> serialized_df = df.select("key", avro.serialize_column("value"))
>>> serialized_df.show(truncate=False)
+---+----------------------+
|key|value                 |
+---+----------------------+
|  1|[0A 41 6C 69 63 65 28]|
|  2|[06 42 6F 62 32]      |
+---+----------------------+
>>> serialized_df.printSchema()
root
|-- key: string (nullable = true)
|-- value: binary (nullable = true)
```

<!-- !! processed by numpydoc !! -->
