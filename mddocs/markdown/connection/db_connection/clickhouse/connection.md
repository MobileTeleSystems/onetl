<a id="clickhouse-connection"></a>

# Clickhouse connection

### *class* onetl.connection.db_connection.clickhouse.connection.Clickhouse(\*, spark: SparkSession, user: str, password: SecretStr, host: Host, port: int = 8123, database: str | None = None, extra: ClickhouseExtra = ClickhouseExtra())

Clickhouse JDBC connection. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Based on Maven package [com.clickhouse:clickhouse-jdbc:0.7.2](https://mvnrepository.com/artifact/com.clickhouse/clickhouse-jdbc/0.7.2)
([official Clickhouse JDBC driver](https://github.com/ClickHouse/clickhouse-jdbc)).

#### SEE ALSO
Before using this connector please take into account [Prerequisites](prerequisites.md#clickhouse-prerequisites)

#### Versionadded
Added in version 0.1.0.

* **Parameters:**
  **host**
  : Host of Clickhouse database. For example: `test.clickhouse.domain.com` or `193.168.1.11`

  **port**
  : Port of Clickhouse database

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
    For example: `{"continueBatchOnError": "false"}`.
    <br/>
    See:
    : * [Clickhouse JDBC driver properties documentation](https://clickhouse.com/docs/en/integrations/java#configuration)
      * [Clickhouse core settings documentation](https://clickhouse.com/docs/en/operations/settings/settings)
      * [Clickhouse query complexity documentation](https://clickhouse.com/docs/en/operations/settings/query-complexity)
      * [Clickhouse query level settings](https://clickhouse.com/docs/en/operations/settings/query-level)

### Examples

Create and check Clickhouse connection:

```python
from onetl.connection import Clickhouse
from pyspark.sql import SparkSession

# Create Spark session with Clickhouse driver loaded
maven_packages = Clickhouse.get_packages()
spark = (
    SparkSession.builder.appName("spark-app-name")
    .config("spark.jars.packages", ",".join(maven_packages))
    .getOrCreate()
)

# Create connection
clickhouse = Clickhouse(
    host="database.host.or.ip",
    user="user",
    password="*****",
    extra={"continueBatchOnError": "false"},
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

#### *classmethod* get_packages(package_version: str | None = None, apache_http_client_version: str | None = None) → list[str]

Get package names to be downloaded by Spark. Allows specifying custom JDBC and Apache HTTP Client versions. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.9.0.

* **Parameters:**
  **package_version**
  : ClickHouse JDBC version client packages. Defaults to `0.7.2`.
    <br/>
    #### Versionadded
    Added in version 0.11.0.

  **apache_http_client_version**
  : Apache HTTP Client version package. Defaults to `5.4.2`.
    <br/>
    Used only if `package_version` is in range `0.5.0-0.7.0`.
    <br/>
    #### Versionadded
    Added in version 0.11.0.

### Examples

```python
from onetl.connection import Clickhouse

Clickhouse.get_packages(package_version="0.6.0", apache_http_client_version="5.4.2")
```

<!-- !! processed by numpydoc !! -->
