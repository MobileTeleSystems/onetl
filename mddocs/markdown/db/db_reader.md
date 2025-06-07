<a id="db-reader"></a>

# DB Reader

| [`DBReader`](#onetl.db.db_reader.db_reader.DBReader)                                     | Allows you to read data from a table with specified database connection and parameters, and return its content as Spark dataframe.   |
|------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| [`DBReader.run`](#onetl.db.db_reader.db_reader.DBReader.run)()                           | Reads data from source table and saves as Spark dataframe.                                                                           |
| [`DBReader.has_data`](#onetl.db.db_reader.db_reader.DBReader.has_data)()                 | Returns `True` if there is some data in the source, `False` otherwise.                                                               |
| [`DBReader.raise_if_no_data`](#onetl.db.db_reader.db_reader.DBReader.raise_if_no_data)() | Raises exception `NoDataError` if source does not contain any data.                                                                  |

### *class* onetl.db.db_reader.db_reader.DBReader(\*, connection: BaseDBConnection, table: str, columns: ConstrainedListValue[str] | None = None, where: Any | None = None, hint: Any | None = None, df_schema: StructType | None = None, hwm_column: str | tuple | None = None, hwm_expression: str | None = None, hwm: AutoDetectHWM | ColumnHWM | KeyValueHWM | None = None, options: GenericOptions | None = None)

Allows you to read data from a table with specified database connection
and parameters, and return its content as Spark dataframe. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### NOTE
DBReader can return different results depending on [Read Strategies](../strategy/index.md#strategy)

#### NOTE
This class operates with only one source at a time. It does NOT support executing queries
to multiple source, like `SELECT ... JOIN`.

#### Versionadded
Added in version 0.1.0.

#### Versionchanged
Changed in version 0.8.0: Moved `onetl.core.DBReader` → `onetl.db.DBReader`

* **Parameters:**
  **connection**
  : Class which contains DB connection properties. See [DB Connections](../connection/db_connection/index.md#db-connections) section

  **source**
  : Table/collection/etc name to read data from.
    <br/>
    If connection has schema support, you need to specify the full name of the source
    including the schema, e.g. `schema.name`.
    <br/>
    #### Versionchanged
    Changed in version 0.7.0: Renamed `table` → `source`

  **columns**
  : The list of columns to be read.
    <br/>
    If RDBMS supports any kind of expressions, you can pass them too.
    ```python
    columns = [
        "mycolumn",
        "another_column as alias",
        "count(*) over ()",
        "some(function) as alias2",
    ]
    ```
    <br/>
    #### NOTE
    Some sources does not have columns.
    <br/>
    #### NOTE
    It is recommended to pass column names explicitly to avoid selecting too many columns,
    and to avoid adding unexpected columns to dataframe if source DDL is changed.
    <br/>
    #### Deprecated
    Deprecated since version 0.10.0: Syntax `DBReader(columns="col1, col2")` (string instead of list) is not supported,
    and will be removed in v1.0.0

  **where**
  : Custom `where` for SQL query or MongoDB pipeline.
    <br/>
    `where` syntax depends on the source. For example, SQL sources
    accept `where` as a string, but MongoDB sources accept `where` as a dictionary.
    ```python
    # SQL database connection
    where = "column_1 > 2"
    <br/>
    # MongoDB connection
    where = {
        "col_1": {"$gt": 1, "$lt": 100},
        "col_2": {"$gt": 2},
        "col_3": {"$eq": "hello"},
    }
    ```
    <br/>
    #### NOTE
    Some sources does not support data filtering.

  **hwm**
  : HWM class to be used as [HWM](https://etl-entities.readthedocs.io/en/stable/hwm/index.html) value.
    ```python
    hwm = DBReader.AutoDetectHWM(
        name="some_unique_hwm_name",
        expression="hwm_column",
    )
    ```
    <br/>
    HWM value will be fetched using `hwm_column` SQL query.
    <br/>
    If you want to use some SQL expression as HWM value, you can use it as well:
    ```python
    hwm = DBReader.AutoDetectHWM(
        name="some_unique_hwm_name",
        expression="cast(hwm_column_orig as date)",
    )
    ```
    <br/>
    #### NOTE
    Some sources does not support passing expressions and can be used only with column/field
    names which present in the source.
    <br/>
    #### Versionchanged
    Changed in version 0.10.0: Replaces deprecated `hwm_column` and `hwm_expression`  attributes

  **hint**
  : Hint expression used for querying the data.
    <br/>
    `hint` syntax depends on the source. For example, SQL sources
    accept `hint` as a string, but MongoDB sources accept `hint` as a dictionary.
    ```python
    # SQL database connection
    hint = "index(myschema.mytable mycolumn)"
    <br/>
    # MongoDB connection
    hint = {
        "mycolumn": 1,
    }
    ```
    <br/>
    #### NOTE
    Some sources does not support hints.

  **df_schema**
  : Spark DataFrame schema, used for proper type casting of the rows.
    ```python
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    <br/>
    df_schema = StructType(
        [
            StructField("_id", IntegerType()),
            StructField("text_string", StringType()),
            StructField("hwm_int", IntegerType()),
            StructField("hwm_datetime", TimestampType()),
            StructField("float_value", DoubleType()),
        ],
    )
    <br/>
    reader = DBReader(
        connection=connection,
        source="fiddle.dummy",
        df_schema=df_schema,
    )
    ```
    <br/>
    #### NOTE
    Some sources does not support passing dataframe schema.

  **options**
  : Spark read options, like partitioning mode.
    ```python
    Postgres.ReadOptions(
        partitioningMode="hash",
        partitionColumn="some_column",
        numPartitions=20,
        fetchsize=1000,
    )
    ```
    <br/>
    #### NOTE
    Some sources does not support reading options.

### Examples

Minimal example

```py
from onetl.db import DBReader
from onetl.connection import Postgres

postgres = Postgres(...)

# create reader
reader = DBReader(connection=postgres, source="fiddle.dummy")

# read data from table "fiddle.dummy"
df = reader.run()
```

With custom reading options

```py
from onetl.connection import Postgres
from onetl.db import DBReader

postgres = Postgres(...)
options = Postgres.ReadOptions(sessionInitStatement="select 300", fetchsize="100")

# create reader and pass some options to the underlying connection object
reader = DBReader(connection=postgres, source="fiddle.dummy", options=options)

# read data from table "fiddle.dummy"
df = reader.run()
```

Full example

```py
from onetl.db import DBReader
from onetl.connection import Postgres

postgres = Postgres(...)
options = Postgres.ReadOptions(sessionInitStatement="select 300", fetchsize="100")

# create reader with specific columns, rows filter
reader = DBReader(
    connection=postgres,
    source="default.test",
    where="d_id > 100",
    hint="NOWAIT",
    columns=["d_id", "d_name", "d_age"],
    options=options,
)

# read data from table "fiddle.dummy"
df = reader.run()
```

Incremental reading

See [Read Strategies](../strategy/index.md#strategy) for more examples

```python
from onetl.strategy import IncrementalStrategy

...

reader = DBReader(
    connection=postgres,
    source="fiddle.dummy",
    hwm=DBReader.AutoDetectHWM(  # mandatory for IncrementalStrategy
        name="some_unique_hwm_name",
        expression="d_age",
    ),
)

# read data from table "fiddle.dummy"
# but only with new rows (`WHERE d_age > previous_hwm_value`)
with IncrementalStrategy():
    df = reader.run()
```

<!-- !! processed by numpydoc !! -->

#### has_data() → bool

Returns `True` if there is some data in the source, `False` otherwise. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### NOTE
This method can return different results depending on [Read Strategies](../strategy/index.md#strategy)

#### WARNING
If [hwm](https://etl-entities.readthedocs.io/en/stable/hwm/index.html) is used, then method should be called inside [Read Strategies](../strategy/index.md#strategy) context. And vise-versa, if HWM is not used, this method should not be called within strategy.

#### Versionadded
Added in version 0.10.0.

* **Raises:**
  RuntimeError
  : Current strategy is not compatible with HWM parameter.

### Examples

```python
reader = DBReader(...)

# handle situation when there is no data in the source
if reader.has_data():
    df = reader.run()
else:
    # implement your handling logic here
    ...
```

<!-- !! processed by numpydoc !! -->

#### raise_if_no_data() → None

Raises exception `NoDataError` if source does not contain any data. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### NOTE
This method can return different results depending on [Read Strategies](../strategy/index.md#strategy)

#### WARNING
If [hwm](https://etl-entities.readthedocs.io/en/stable/hwm/index.html) is used, then method should be called inside [Read Strategies](../strategy/index.md#strategy) context. And vise-versa, if HWM is not used, this method should not be called within strategy.

#### Versionadded
Added in version 0.10.0.

* **Raises:**
  RuntimeError
  : Current strategy is not compatible with HWM parameter.

  `onetl.exception.NoDataError`
  : There is no data in source.

### Examples

```python
reader = DBReader(...)

# ensure that there is some data in the source before reading it using Spark
reader.raise_if_no_data()
```

<!-- !! processed by numpydoc !! -->

#### run() → DataFrame

Reads data from source table and saves as Spark dataframe. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### NOTE
This method can return different results depending on [Read Strategies](../strategy/index.md#strategy)

#### WARNING
If [hwm](https://etl-entities.readthedocs.io/en/stable/hwm/index.html) is used, then method should be called inside [Read Strategies](../strategy/index.md#strategy) context. And vise-versa, if HWM is not used, this method should not be called within strategy.

#### Versionadded
Added in version 0.1.0.

* **Returns:**
  **df**
  : Spark dataframe

### Examples

Read data to Spark dataframe:

```python
df = reader.run()
```

<!-- !! processed by numpydoc !! -->
