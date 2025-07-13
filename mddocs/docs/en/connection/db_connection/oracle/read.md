# Reading from Oracle using `DBReader` { #oracle-read }

[DBReader][db-reader] supports [strategy][strategy] for incremental data reading, but does not support custom queries, like `JOIN`.

!!! warning

    Please take into account [Oracle types][oracle-types]

## Supported DBReader features

- ✅︎ `columns`
- ✅︎ `where`
- ✅︎ `hwm`, supported strategies:
  - ✅︎ [Snapshot strategy][snapshot-strategy]
  - ✅︎ [Incremental strategy][incremental-strategy]
  - ✅︎ [Snapshot batch strategy][snapshot-batch-strategy]
  - ✅︎ [Incremental batch strategy][incremental-batch-strategy]
- ✅︎ `hint` (see [official documentation](https://docs.oracle.com/cd/B10500_01/server.920/a96533/hintsref.htm))
- ❌ `df_schema`
- ✅︎ `options` (see [Oracle.ReadOptions][onetl.connection.db_connection.oracle.options.OracleReadOptions])

## Examples

Snapshot strategy:

    ```python
        from onetl.connection import Oracle
        from onetl.db import DBReader

        oracle = Oracle(...)

        reader = DBReader(
            connection=oracle,
            source="schema.table",
            columns=["id", "key", "CAST(value AS VARCHAR2(4000)) value", "updated_dt"],
            where="key = 'something'",
            hint="INDEX(schema.table key_index)",
            options=Oracle.ReadOptions(partitionColumn="id", numPartitions=10),
        )
        df = reader.run()
    ```

Incremental strategy:

    ```python
        from onetl.connection import Oracle
        from onetl.db import DBReader
        from onetl.strategy import IncrementalStrategy

        oracle = Oracle(...)

        reader = DBReader(
            connection=oracle,
            source="schema.table",
            columns=["id", "key", "CAST(value AS VARCHAR2(4000)) value", "updated_dt"],
            where="key = 'something'",
            hint="INDEX(schema.table key_index)",
            hwm=DBReader.AutoDetectHWM(name="oracle_hwm", expression="updated_dt"),
            options=Oracle.ReadOptions(partitionColumn="id", numPartitions=10),
        )

        with IncrementalStrategy():
            df = reader.run()
    ```

## Recommendations

### Select only required columns

Instead of passing `"*"` in `DBReader(columns=[...])` prefer passing exact column names. This reduces the amount of data passed from Oracle to Spark.

### Pay attention to `where` value

Instead of filtering data on Spark side using `df.filter(df.column == 'value')` pass proper `DBReader(where="column = 'value'")` clause.
This both reduces the amount of data send from Oracle to Spark, and may also improve performance of the query.
Especially if there are indexes or partitions for columns used in `where` clause.

## Options

<!-- 
    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.oracle.options
    ```

    ```{eval-rst}
    .. autopydantic_model:: OracleReadOptions
        :inherited-members: GenericOptions
        :member-order: bysource
    ```
 -->

::: onetl.connection.db_connection.oracle.options.OracleReadOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
