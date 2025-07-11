# Writing to MSSQL using `DBWriter` { #mssql-write }

For writing data to MSSQL, use [DBWriter][db-writer].

!!! warning

    Please take into account [MSSQL types][mssql-types]

!!! warning

    It is always recommended to create table explicitly using [MSSQL.execute][mssql-execute]
    instead of relying on Spark's table DDL generation.

    This is because Spark's DDL generator can create columns with different precision and types than it is expected,
    causing precision loss or other issues.

## Examples

    ```python
        from onetl.connection import MSSQL
        from onetl.db import DBWriter

        mssql = MSSQL(...)

        df = ...  # data is here

        writer = DBWriter(
            connection=mssql,
            target="schema.table",
            options=MSSQL.WriteOptions(if_exists="append"),
        )

        writer.run(df)
    ```

## Options

Method above accepts [MSSQL.WriteOptions][onetl.connection.db_connection.mssql.options.MSSQLWriteOptions]

<!-- 
    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.mssql.options
    ```

    ```{eval-rst}
    .. autopydantic_model:: MSSQLWriteOptions
        :inherited-members: GenericOptions
        :member-order: bysource
        :model-show-field-summary: false
        :field-show-constraints: false
```
 -->

::: onetl.connection.db_connection.mssql.options.MSSQLWriteOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
