<a id="hive-execute"></a>

# Executing statements in Hive

Use `Hive.execute(...)` to execute DDL and DML operations.

## Syntax support

This method supports **any** query syntax supported by Hive, like:

* ✅︎ `CREATE TABLE ...`, `CREATE VIEW ...`, and so on
* ✅︎ `LOAD DATA ...`, and so on
* ✅︎ `ALTER ...`
* ✅︎ `INSERT INTO ... SELECT ...`, and so on
* ✅︎ `DROP TABLE ...`, `DROP VIEW ...`, and so on
* ✅︎ `MSCK REPAIR TABLE ...`, and so on
* ✅︎ other statements not mentioned here
* ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### WARNING
Actually, query should be written using [SparkSQL](https://spark.apache.org/docs/latest/sql-ref-syntax.html#ddl-statements) syntax, not HiveQL.

## Examples

```python
from onetl.connection import Hive

hive = Hive(...)

hive.execute("DROP TABLE schema.table")
hive.execute(
    """
    CREATE TABLE schema.table (
        id NUMBER,
        key VARCHAR,
        value DOUBLE
    )
    PARTITION BY (business_date DATE)
    STORED AS orc
    """
)
```

### Details

#### Hive.execute(statement: str) → None

Execute DDL or DML statement. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.2.0.

* **Parameters:**
  **statement**
  : Statement to be executed.

<!-- !! processed by numpydoc !! -->
