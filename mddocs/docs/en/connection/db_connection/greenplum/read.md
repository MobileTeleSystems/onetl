(greenplum-read)=

# Reading from Greenplum using `DBReader`

Data can be read from Greenplum to Spark using {obj}`DBReader <onetl.db.db_reader.db_reader.DBReader>`.
It also supports {ref}`strategy` for incremental data reading.

```{eval-rst}
.. warning::

    Please take into account :ref:`greenplum-types`.
```

```{eval-rst}
.. note::

    Unlike JDBC connectors, *Greenplum connector for Spark* does not support
    executing **custom** SQL queries using `.sql` method. Connector can be used to only read data from a table or view.
```

## Supported DBReader features

- ✅︎ `columns` (see note below)
- ✅︎ `where` (see note below)
- ✅︎ `hwm` (see note below), supported strategies:
- - ✅︎ {ref}`snapshot-strategy`
- - ✅︎ {ref}`incremental-strategy`
- - ✅︎ {ref}`snapshot-batch-strategy`
- - ✅︎ {ref}`incremental-batch-strategy`
- ❌ `hint` (is not supported by Greenplum)
- ❌ `df_schema`
- ✅︎ `options` (see {obj}`Greenplum.ReadOptions <onetl.connection.db_connection.greenplum.options.GreenplumReadOptions>`)

```{eval-rst}
.. warning::

    In case of Greenplum connector, `DBReader` does not generate raw `SELECT` query. Instead it relies on Spark SQL syntax
    which in some cases (using column projection and predicate pushdown) can be converted to Greenplum SQL.

    So `columns`, `where` and `hwm.expression` should be specified in `Spark SQL <https://spark.apache.org/docs/latest/sql-ref-syntax.html>`_ syntax,
    not Greenplum SQL.

    This is OK:

    .. code-block:: python

        DBReader(
            columns=[
                "some_column",
                # this cast is executed on Spark side
                "CAST(another_column AS STRING)",
            ],
            # this predicate is parsed by Spark, and can be pushed down to Greenplum
            where="some_column LIKE 'val1%'",
        )

    This is will fail:

    .. code-block:: python

        DBReader(
            columns=[
                "some_column",
                # Spark does not have `text` type
                "CAST(another_column AS text)",
            ],
            # Spark does not support ~ syntax for regexp matching
            where="some_column ~ 'val1.*'",
        )
```

## Examples

Snapshot strategy:

```python
from onetl.connection import Greenplum
from onetl.db import DBReader

greenplum = Greenplum(...)

reader = DBReader(
    connection=greenplum,
    source="schema.table",
    columns=["id", "key", "CAST(value AS string) value", "updated_dt"],
    where="key = 'something'",
)
df = reader.run()
```

Incremental strategy:

```python
from onetl.connection import Greenplum
from onetl.db import DBReader
from onetl.strategy import IncrementalStrategy

greenplum = Greenplum(...)

reader = DBReader(
    connection=greenplum,
    source="schema.table",
    columns=["id", "key", "CAST(value AS string) value", "updated_dt"],
    where="key = 'something'",
    hwm=DBReader.AutoDetectHWM(name="greenplum_hwm", expression="updated_dt"),
)

with IncrementalStrategy():
    df = reader.run()
```

## Interaction schema

High-level schema is described in {ref}`greenplum-prerequisites`. You can find detailed interaction schema below.

```{eval-rst}
.. dropdown:: Spark <-> Greenplum interaction during DBReader.run()

    .. plantuml::

        @startuml
        title Greenplum master <-> Spark driver
                box "Spark"
                participant "Spark driver"
                participant "Spark executor1"
                participant "Spark executor2"
                participant "Spark executorN"
                end box

                box "Greenplum"
                participant "Greenplum master"
                participant "Greenplum segment1"
                participant "Greenplum segment2"
                participant "Greenplum segmentN"
                end box

                == Greenplum.check() ==

                activate "Spark driver"
                "Spark driver" -> "Greenplum master" ++ : CONNECT

                "Spark driver" --> "Greenplum master" : CHECK IF TABLE EXISTS gp_table
                "Greenplum master" --> "Spark driver" : TABLE EXISTS
                "Spark driver" -> "Greenplum master" : SHOW SCHEMA FOR gp_table
                "Greenplum master" --> "Spark driver" : (id bigint, col1 int, col2 text, ...)

                == DBReader.run() ==

                "Spark driver" -> "Spark executor1" ++ : START EXECUTOR FOR df(id bigint, col1 int, col2 text, ...) PARTITION 1
                "Spark driver" -> "Spark executor2" ++ : START EXECUTOR FOR df(id bigint, col1 int, col2 text, ...) PARTITION 2
                "Spark driver" -> "Spark executorN" ++ : START EXECUTOR FOR df(id bigint, col1 int, col2 text, ...) PARTITION N

                note right of "Spark driver" : This is done in parallel,\nexecutors are independent\n|\n|\n|\nV
                "Spark executor1" -> "Greenplum master" ++ : CREATE WRITABLE EXTERNAL TABLE spark_executor1 (id bigint, col1 int, col2 text, ...) USING address=executor1_host:executor1_port;\nINSERT INTO EXTERNAL TABLE spark_executor1 FROM gp_table WHERE gp_segment_id = 1
                note right of "Greenplum master" : Each white vertical line here is a opened connection to master.\nUsually, **N+1** connections are created from Spark to Greenplum master
                "Greenplum master" --> "Greenplum segment1" ++ : SELECT DATA FROM gp_table_data_on_segment1 TO spark_executor1
                note right of "Greenplum segment1" : No direct requests between Greenplum segments & Spark.\nData transfer is always initiated by Greenplum segments.

                "Spark executor2" -> "Greenplum master" ++ : CREATE WRITABLE EXTERNAL TABLE spark_executor2 (id bigint, col1 int, col2 text, ...) USING address=executor2_host:executor2_port;\nINSERT INTO EXTERNAL TABLE spark_executor2 FROM gp_table WHERE gp_segment_id = 2
                "Greenplum master" --> "Greenplum segment2" ++ : SELECT DATA FROM gp_table_data_on_segment2 TO spark_executor2

                "Spark executorN" -> "Greenplum master" ++ : CREATE WRITABLE EXTERNAL TABLE spark_executorN (id bigint, col1 int, col2 text, ...) USING address=executorN_host:executorN_port;\nINSERT INTO EXTERNAL TABLE spark_executorN FROM gp_table WHERE gp_segment_id = N
                "Greenplum master" --> "Greenplum segmentN" ++ : SELECT DATA FROM gp_table_data_on_segmentN TO spark_executorN

                "Greenplum segment1" ->o "Spark executor1" -- : INITIALIZE CONNECTION TO Spark executor1\nPUSH DATA TO Spark executor1
                note left of "Spark executor1" : Circle is an open GPFDIST port,\nlistened by executor

                "Greenplum segment2" ->o "Spark executor2" -- : INITIALIZE CONNECTION TO Spark executor2\nPUSH DATA TO Spark executor2
                "Greenplum segmentN" ->o "Spark executorN" -- : INITIALIZE CONNECTION TO Spark executorN\nPUSH DATA TO Spark executorN

                == Spark.stop() ==

                "Spark executor1" --> "Greenplum master" : DROP TABLE spark_executor1
                deactivate "Greenplum master"
                "Spark executor2" --> "Greenplum master" : DROP TABLE spark_executor2
                deactivate "Greenplum master"
                "Spark executorN" --> "Greenplum master" : DROP TABLE spark_executorN
                deactivate "Greenplum master"

                "Spark executor1" --> "Spark driver" -- : DONE
                "Spark executor2" --> "Spark driver" -- : DONE
                "Spark executorN" --> "Spark driver" -- : DONE

                "Spark driver" --> "Greenplum master" : CLOSE CONNECTION
                deactivate "Greenplum master"
                deactivate "Spark driver"
        @enduml
```

## Recommendations

### Select only required columns

Instead of passing `"*"` in `DBReader(columns=[...])` prefer passing exact column names. This reduces the amount of data passed from Greenplum to Spark.

### Pay attention to `where` value

Instead of filtering data on Spark side using `df.filter(df.column == 'value')` pass proper `DBReader(where="column = 'value'")` clause.
This both reduces the amount of data send from Greenplum to Spark, and may also improve performance of the query.
Especially if there are indexes or partitions for columns used in `where` clause.

### Read data in parallel

`DBReader` in case of Greenplum connector requires view or table to have a column which is used by Spark
for parallel reads.

Choosing proper column allows each Spark executor to read only part of data stored in the specified segment,
avoiding moving large amounts of data between segments, which improves reading performance.

#### Using `gp_segment_id`

By default, `DBReader` will use [gp_segment_id](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/troubleshooting.html#reading-from-a-view)
column for parallel data reading. Each DataFrame partition will contain data of a specific Greenplum segment.

This allows each Spark executor read only data from specific Greenplum segment, avoiding moving large amounts of data between segments.

If view is used, it is recommended to include `gp_segment_id` column to this view:

```{eval-rst}
.. dropdown:: Reading from view with gp_segment_id column

    .. code-block:: python

        from onetl.connection import Greenplum
        from onetl.db import DBReader

        greenplum = Greenplum(...)

        greenplum.execute(
            """
            CREATE VIEW schema.view_with_gp_segment_id AS
            SELECT
                id,
                some_column,
                another_column,
                gp_segment_id  -- IMPORTANT
            FROM schema.some_table
            """,
        )

        reader = DBReader(
            connection=greenplum,
            source="schema.view_with_gp_segment_id",
        )
        df = reader.run()
```

#### Using custom `partition_column`

Sometimes table or view is lack of `gp_segment_id` column, but there is some column
with value range correlated with Greenplum segment distribution.

In this case, custom column can be used instead:

```{eval-rst}
.. dropdown:: Reading from view with custom partition_column

    .. code-block:: python

        from onetl.connection import Greenplum
        from onetl.db import DBReader

        greenplum = Greenplum(...)

        greenplum.execute(
            """
            CREATE VIEW schema.view_with_partition_column AS
            SELECT
                id,
                some_column,
                part_column  -- correlated to greenplum segment ID
            FROM schema.some_table
            """,
        )

        reader = DBReader(
            connection=greenplum,
            source="schema.view_with_partition_column",
            options=Greenplum.ReadOptions(
                # parallelize data using specified column
                partitionColumn="part_column",
                # create 10 Spark tasks, each will read only part of table data
                partitions=10,
            ),
        )
        df = reader.run()
```

#### Reading `DISTRIBUTED REPLICATED` tables

Replicated tables do not have `gp_segment_id` column at all, so you need to set `partition_column` to some column name
of type integer/bigint/smallint.

### Parallel `JOIN` execution

In case of using views which require some data motion between Greenplum segments, like `JOIN` queries, another approach should be used.

Each Spark executor N will run the same query, so each of N query will start its own JOIN process, leading to really heavy load on Greenplum segments.
**This should be avoided**.

Instead is recommended to run `JOIN` query on Greenplum side, save the result to an intermediate table,
and then read this table using `DBReader`:

```{eval-rst}
.. dropdown:: Reading from view using intermediate table

    .. code-block:: python

        from onetl.connection import Greenplum
        from onetl.db import DBReader

        greenplum = Greenplum(...)

        greenplum.execute(
            """
            CREATE UNLOGGED TABLE schema.intermediate_table AS
            SELECT
                id,
                tbl1.col1,
                tbl1.data,
                tbl2.another_data
            FROM
                schema.table1 as tbl1
            JOIN
                schema.table2 as tbl2
            ON
                tbl1.col1 = tbl2.col2
            WHERE ...
            """,
        )

        reader = DBReader(
            connection=greenplum,
            source="schema.intermediate_table",
        )
        df = reader.run()

        # write dataframe somethere

        greenplum.execute(
            """
            DROP TABLE schema.intermediate_table
            """,
        )
```

```{eval-rst}
.. warning::

    **NEVER** do that:

    .. code-block:: python

        df1 = DBReader(connection=greenplum, target="public.table1", ...).run()
        df2 = DBReader(connection=greenplum, target="public.table2", ...).run()

        joined_df = df1.join(df2, on="col")

    This will lead to sending all the data from both `table1` and `table2` to Spark executor memory, and then `JOIN``
    will be performed on Spark side, not inside Greenplum. This is **VERY** inefficient.
```

#### `TEMPORARY` tables notice

Someone could think that writing data from view or result of `JOIN` to `TEMPORARY` table,
and then passing it to `DBReader`, is an efficient way to read data from Greenplum. This is because temp tables are not generating WAL files,
and are automatically deleted after finishing the transaction.

That will **NOT** work. Each Spark executor establishes its own connection to Greenplum.
And each connection starts its own transaction which means that every executor will read empty temporary table.

You should use [UNLOGGED](https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/ref_guide-sql_commands-CREATE_TABLE.html) tables
to write data to intermediate table without generating WAL logs.

## Options

```{eval-rst}
.. currentmodule:: onetl.connection.db_connection.greenplum.options
```

```{eval-rst}
.. autopydantic_model:: GreenplumReadOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
```
