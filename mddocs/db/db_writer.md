<a id="db-writer"></a>

# DB Writer

| [`DBWriter`](#onetl.db.db_writer.db_writer.DBWriter)             | Class specifies schema and table where you can write your dataframe.   |
|------------------------------------------------------------------|------------------------------------------------------------------------|
| [`DBWriter.run`](#onetl.db.db_writer.db_writer.DBWriter.run)(df) | Method for writing your df to specified target.                        |

### *class* onetl.db.db_writer.db_writer.DBWriter(\*, connection: BaseDBConnection, table: str, options: GenericOptions | None = None)

Class specifies schema and table where you can write your dataframe. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.1.0.

#### Versionchanged
Changed in version 0.8.0: Moved `onetl.core.DBReader` → `onetl.db.DBReader`

* **Parameters:**
  **connection**
  : Class which contains DB connection properties. See [DB Connections](../connection/db_connection/index.md#db-connections) section.

  **target**
  : Table/collection/etc name to write data to.
    <br/>
    If connection has schema support, you need to specify the full name of the source
    including the schema, e.g. `schema.name`.
    <br/>
    #### Versionchanged
    Changed in version 0.7.0: Renamed `table` → `target`

  **options**
  : Spark write options. Can be in form of special `WriteOptions` object or a dict.
    <br/>
    For example:
    `{"if_exists": "replace_entire_table", "compression": "snappy"}`
    or
    `Hive.WriteOptions(if_exists="replace_entire_table", compression="snappy")`
    <br/>
    #### NOTE
    Some sources does not support writing options.

### Examples

Minimal example

```py
from onetl.connection import Postgres
from onetl.db import DBWriter

postgres = Postgres(...)

writer = DBWriter(
    connection=postgres,
    target="fiddle.dummy",
)
```

With custom write options

```py
from onetl.connection import Postgres
from onetl.db import DBWriter

postgres = Postgres(...)

options = Postgres.WriteOptions(if_exists="replace_entire_table", batchsize=1000)

writer = DBWriter(
    connection=postgres,
    target="fiddle.dummy",
    options=options,
)
```

<!-- !! processed by numpydoc !! -->

#### run(df: DataFrame) → None

Method for writing your df to specified target. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### NOTE
Method does support only **batching** DataFrames.

#### Versionadded
Added in version 0.1.0.

* **Parameters:**
  **df**
  : Spark dataframe

### Examples

Write dataframe to target:

```python
writer.run(df)
```

<!-- !! processed by numpydoc !! -->
