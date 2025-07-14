# Reading from Postgres using `DBReader` { #postgres-read }

[DBReader][db-reader] supports [strategy][strategy] for incremental data reading, but does not support custom queries, like `JOIN`.

!!! warning

    Please take into account [Postgres types][postgres-types]

## Supported DBReader features

- ✅︎ `columns`
- ✅︎ `where`
- ✅︎ `hwm`, supported strategies:
  - ✅︎ [Snapshot strategy][snapshot-strategy-0]
  - ✅︎ [Incremental strategy][incremental-strategy-0]
  - ✅︎ [Snapshot batch strategy][snapshot-batch-strategy-0]
  - ✅︎ [Incremental batch strategy][incremental-batch-strategy-0]
- ❌ `hint` (is not supported by Postgres)
- ❌ `df_schema`
- ✅︎ `options` (see [Postgres.ReadOptions][onetl.connection.db_connection.postgres.options.PostgresReadOptions])

## Examples

Snapshot strategy:

    ```python
        from onetl.connection import Postgres
        from onetl.db import DBReader

        postgres = Postgres(...)

        reader = DBReader(
            connection=postgres,
            source="schema.table",
            columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
            where="key = 'something'",
            options=Postgres.ReadOptions(partitionColumn="id", numPartitions=10),
        )
        df = reader.run()
    ```

Incremental strategy:

    ```python
        from onetl.connection import Postgres
        from onetl.db import DBReader
        from onetl.strategy import IncrementalStrategy

        postgres = Postgres(...)

        reader = DBReader(
            connection=postgres,
            source="schema.table",
            columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
            where="key = 'something'",
            hwm=DBReader.AutoDetectHWM(name="postgres_hwm", expression="updated_dt"),
            options=Postgres.ReadOptions(partitionColumn="id", numPartitions=10),
        )

        with IncrementalStrategy():
            df = reader.run()
    ```

## Recommendations

### Select only required columns

Instead of passing `"*"` in `DBReader(columns=[...])` prefer passing exact column names. This reduces the amount of data passed from Postgres to Spark.

### Pay attention to `where` value

Instead of filtering data on Spark side using `df.filter(df.column == 'value')` pass proper `DBReader(where="column = 'value'")` clause.
This both reduces the amount of data send from Postgres to Spark, and may also improve performance of the query.
Especially if there are indexes or partitions for columns used in `where` clause.

## Options { #postgres-read-options }

<!-- 
    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.postgres.options
    ```

    ```{eval-rst}
    .. autopydantic_model:: PostgresReadOptions
        :inherited-members: GenericOptions
        :member-order: bysource
    ```
 -->

::: onetl.connection.db_connection.postgres.options.PostgresReadOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
