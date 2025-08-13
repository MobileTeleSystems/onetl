# Executing statements in MSSQL { #mssql-execute }

!!! warning

    Methods below **read all the rows** returned from DB **to Spark driver memory**, and then convert them to DataFrame.

    Do **NOT** use them to read large amounts of data. Use [DBReader][mssql-read] or [MSSQL.sql][mssql-sql] instead.

## How to

There are 2 ways to execute some statement in MSSQL

### Use `MSSQL.fetch`

Use this method to perform some `SELECT` query which returns **small number or rows**, like reading MSSQL config, or reading data from some reference table. Method returns Spark DataFrame.

Method accepts [MSSQL.FetchOptions][onetl.connection.db_connection.mssql.options.MSSQLFetchOptions].

Connection opened using this method should be then closed with `connection.close()` or `with connection:`.

!!! warning

    Please take into account [MSSQL types][mssql-types].

#### Syntax support in `MSSQL.fetch`

This method supports **any** query syntax supported by MSSQL, like:

- ✅︎ `SELECT ... FROM ...`
- ✅︎ `WITH alias AS (...) SELECT ...`
- ✅︎ `SELECT func(arg1, arg2) FROM DUAL` - call function
- ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples for `MSSQL.fetch`

    ```python
        from onetl.connection import MSSQL

        mssql = MSSQL(...)

        df = mssql.fetch(
            "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
            options=MSSQL.FetchOptions(queryTimeout=10),
        )
        mssql.close()
        value = df.collect()[0][0]  # get value from first row and first column
    ```

### Use `MSSQL.execute`

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts [MSSQL.ExecuteOptions][onetl.connection.db_connection.mssql.options.MSSQLExecuteOptions].

Connection opened using this method should be then closed with `connection.close()` or `with connection:`.

#### Syntax support in `MSSQL.execute`

This method supports **any** query syntax supported by MSSQL, like:

- ✅︎ `CREATE TABLE ...`, `CREATE VIEW ...`
- ✅︎ `ALTER ...`
- ✅︎ `INSERT INTO ... AS SELECT ...`
- ✅︎ `DROP TABLE ...`, `DROP VIEW ...`, `TRUNCATE TABLE`, and so on
- ✅︎ `EXEC procedure(arg1, arg2) ...` or `{call procedure(arg1, arg2)}` - special syntax for calling procedure
- ✅︎ `DECLARE ... BEGIN ... END` - execute PL/SQL statement
- ✅︎ other statements not mentioned here
- ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples for `MSSQL.execute`

    ```python
        from onetl.connection import MSSQL

        mssql = MSSQL(...)

        mssql.execute("DROP TABLE schema.table")
        mssql.execute(
            """
            CREATE TABLE schema.table (
                id bigint GENERATED ALWAYS AS IDENTITY,
                key VARCHAR2(4000),
                value NUMBER
            )
            """,
            options=MSSQL.ExecuteOptions(queryTimeout=10),
        )
    ```

## Options

<!-- 
    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.mssql.options
    ```

    ```{eval-rst}
    .. autopydantic_model:: MSSQLFetchOptions
        :inherited-members: GenericOptions
        :member-order: bysource
        :model-show-field-summary: false
        :field-show-constraints: false
    ```

    ```{eval-rst}
    .. autopydantic_model:: MSSQLExecuteOptions
        :inherited-members: GenericOptions
        :member-order: bysource
        :model-show-field-summary: false
        :field-show-constraints: false
    ```
 -->

::: onetl.connection.db_connection.mssql.options.MSSQLFetchOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true

::: onetl.connection.db_connection.mssql.options.MSSQLExecuteOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
