<a id="greenplum-write"></a>

# Writing to Greenplum using `DBWriter`

For writing data to Greenplum, use [`DBWriter`](../../../db/db_writer.md#onetl.db.db_writer.db_writer.DBWriter)
with [`GreenplumWriteOptions`](#onetl.connection.db_connection.greenplum.options.GreenplumWriteOptions).

#### WARNING
Please take into account [Greenplum <-> Spark type mapping](types.md#greenplum-types).

#### WARNING
It is always recommended to create table explicitly using [Greenplum.execute](execute.md#greenplum-execute)
instead of relying on Spark’s table DDL generation.

This is because Spark’s DDL generator can create columns with different types than it is expected.

## Examples

```python
from onetl.connection import Greenplum
from onetl.db import DBWriter

greenplum = Greenplum(...)

df = ...  # data is here

writer = DBWriter(
    connection=greenplum,
    target="schema.table",
    options=Greenplum.WriteOptions(
        if_exists="append",
        # by default distribution is random
        distributedBy="id",
        # partitionBy is not supported
    ),
)

writer.run(df)
```

## Interaction schema

High-level schema is described in [Prerequisites](prerequisites.md#greenplum-prerequisites). You can find detailed interaction schema below.

### Spark <-> Greenplum interaction during DBWriter.run()

## Options

### *pydantic model* onetl.connection.db_connection.greenplum.options.GreenplumWriteOptions

VMware’s Greenplum Spark connector writing options.

#### WARNING
Some options, like `url`, `dbtable`, `server.*`, `pool.*`, etc
are populated from connection attributes, and cannot be overridden by the user in `WriteOptions` to avoid issues.

### Examples

#### NOTE
You can pass any value
[supported by connector](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/write_to_gpdb.html),
even if it is not mentioned in this documentation. **Option names should be in** `camelCase`!

The set of supported options depends on connector version.

```python
from onetl.connection import Greenplum

options = Greenplum.WriteOptions(
    if_exists="append",
    truncate="false",
    distributedBy="mycolumn",
)
```

<!-- !! processed by numpydoc !! -->

#### *field* if_exists *: GreenplumTableExistBehavior* *= GreenplumTableExistBehavior.APPEND* *(alias 'mode')*

Behavior of writing data into existing table.

Possible values:
: * `append` (default)
    : Adds new rows into existing table.
      <br/>
      ### Behavior in details
      <br/>
      * Table does not exist
        : Table is created using options provided by user
          (`distributedBy` and others).
      * Table exists
        : Data is appended to a table. Table has the same DDL as before writing data.
          <br/>
          #### WARNING
          This mode does not check whether table already contains
          rows from dataframe, so duplicated rows can be created.
          <br/>
          Also Spark does not support passing custom options to
          insert statement, like `ON CONFLICT`, so don’t try to
          implement deduplication using unique indexes or constraints.
          <br/>
          Instead, write to staging table and perform deduplication
          using `execute` method.
  * `replace_entire_table`
    : **Table is dropped and then created**.
      <br/>
      ### Behavior in details
      <br/>
      * Table does not exist
        : Table is created using options provided by user
          (`distributedBy` and others).
      * Table exists
        : Table content is replaced with dataframe content.
          <br/>
          After writing completed, target table could either have the same DDL as
          before writing data (`truncate=True`), or can be recreated (`truncate=False`).
  * `ignore`
    : Ignores the write operation if the table already exists.
      <br/>
      ### Behavior in details
      <br/>
      * Table does not exist
        : Table is created using options provided by user
          (`distributedBy` and others).
      * Table exists
        : The write operation is ignored, and no data is written to the table.
  * `error`
    : Raises an error if the table already exists.
      <br/>
      ### Behavior in details
      <br/>
      * Table does not exist
        : Table is created using options provided by user
          (`distributedBy` and others).
      * Table exists
        : An error is raised, and no data is written to the table.

#### Versionchanged
Changed in version 0.9.0: Renamed `mode` → `if_exists`

<!-- !! processed by numpydoc !! -->
