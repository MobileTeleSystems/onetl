<a id="postgres-connection"></a>

# Postgres connection

### *class* onetl.connection.db_connection.postgres.connection.Postgres(\*, spark: SparkSession, user: str, password: SecretStr, host: Host, database: str, port: int = 5432, extra: PostgresExtra = PostgresExtra(stringtype='unspecified', tcpKeepAlive='true'))

PostgreSQL JDBC connection. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Based on Maven package [org.postgresql:postgresql:42.7.5](https://mvnrepository.com/artifact/org.postgresql/postgresql/42.7.5)
([official Postgres JDBC driver](https://jdbc.postgresql.org/)).

#### SEE ALSO
Before using this connector please take into account [Prerequisites](prerequisites.md#postgres-prerequisites)

#### Versionadded
Added in version 0.1.0.

* **Parameters:**
  **host**
  : Host of Postgres database. For example: `test.postgres.domain.com` or `193.168.1.11`

  **port**
  : Port of Postgres database

  **user**
  : User, which have proper access to the database. For example: `some_user`

  **password**
  : Password for database connection

  **database**
  : Database in RDBMS, NOT schema.
    <br/>
    See [this page](https://www.educba.com/postgresql-database-vs-schema/) for more details

  **spark**
  : Spark session.

  **extra**
  : Specifies one or more extra parameters by which clients can connect to the instance.
    <br/>
    For example: `{"ssl": "false"}`
    <br/>
    See [Postgres JDBC driver properties documentation](https://jdbc.postgresql.org/documentation/use/)
    for more details

### Examples

Create and check Postgres connection:

```python
from onetl.connection import Postgres
from pyspark.sql import SparkSession

# Create Spark session with Postgres driver loaded
maven_packages = Postgres.get_packages()
spark = (
    SparkSession.builder.appName("spark-app-name")
    .config("spark.jars.packages", ",".join(maven_packages))
    .getOrCreate()
)

# Create connection
postgres = Postgres(
    host="database.host.or.ip",
    user="user",
    password="*****",
    database="target_database",
    spark=spark,
)
```

Create read-only connection:

```python
...

# Create connection
postgres = Postgres(
    host="database.host.or.ip",
    user="user",
    password="*****",
    database="target_database",
    extra={"readOnly": True, "readOnlyMode": "always"},  # <--
    spark=spark,
).check()
```

<!-- !! processed by numpydoc !! -->

#### check()

Check source availability. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

If not, an exception will be raised.

* **Returns:**
  Connection itself
* **Raises:**
  RuntimeError
  : If the connection is not available

### Examples

```python
connection.check()
```

<!-- !! processed by numpydoc !! -->

#### *classmethod* get_packages(package_version: str | None = None) → list[str]

Get package names to be downloaded by Spark.  Allows specifying a custom JDBC driver version. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.9.0.

* **Parameters:**
  **package_version**
  : Specifies the version of the PostgreSQL JDBC driver to use.  Defaults to `42.7.5`.

### Examples

```python
from onetl.connection import Postgres

Postgres.get_packages()

# custom package version
Postgres.get_packages(package_version="42.6.0")
```

<!-- !! processed by numpydoc !! -->
