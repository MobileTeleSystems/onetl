(oracle-write)=

# Writing to Oracle using `DBWriter`

For writing data to Oracle, use {obj}`DBWriter <onetl.db.db_writer.db_writer.DBWriter>`.

```{eval-rst}
.. warning::

    Please take into account :ref:`oracle-types`
```

```{eval-rst}
.. warning::

    It is always recommended to create table explicitly using :ref:`Oracle.execute <oracle-execute>`
    instead of relying on Spark's table DDL generation.

    This is because Spark's DDL generator can create columns with different precision and types than it is expected,
    causing precision loss or other issues.
```

## Examples

```python
from onetl.connection import Oracle
from onetl.db import DBWriter

oracle = Oracle(...)

df = ...  # data is here

writer = DBWriter(
    connection=oracle,
    target="schema.table",
    options=Oracle.WriteOptions(if_exists="append"),
)

writer.run(df)
```

## Options

Method above accepts {obj}`OracleWriteOptions <onetl.connection.db_connection.oracle.options.OracleWriteOptions>`

```{eval-rst}
.. currentmodule:: onetl.connection.db_connection.oracle.options
```

```{eval-rst}
.. autopydantic_model:: OracleWriteOptions
    :inherited-members: GenericOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
```
