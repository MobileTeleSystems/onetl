<a id="mysql-connection"></a>

# MySQL connection

### *class* onetl.connection.db_connection.mysql.connection.MySQL(\*, spark: SparkSession, user: str, password: SecretStr, host: Host, port: int = 3306, database: str | None = None, extra: MySQLExtra = MySQLExtra(useUnicode='yes', characterEncoding='UTF-8'))

MySQL JDBC connection. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Based on Maven package [com.mysql:mysql-connector-j:9.2.0](https://mvnrepository.com/artifact/com.mysql/mysql-connector-j/9.2.0)
([official MySQL JDBC driver](https://dev.mysql.com/doc/connector-j/en/)).

#### SEE ALSO
Before using this connector please take into account [Prerequisites](prerequisites.md#mysql-prerequisites)

#### Versionadded
Added in version 0.1.0.

* **Parameters:**
  **host**
  : Host of MySQL database. For example: `mysql0012.domain.com` or `192.168.1.11`

  **port**
  : Port of MySQL database

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
    For example: `{"useSSL": "false", "allowPublicKeyRetrieval": "true"}`
    <br/>
    See [MySQL JDBC driver properties documentation](https://dev.mysql.com/doc/connector-j/en/connector-j-reference-configuration-properties.html)
    for more details

### Examples

Create and check MySQL connection:

```python
from onetl.connection import MySQL
from pyspark.sql import SparkSession

# Create Spark session with MySQL driver loaded
maven_packages = MySQL.get_packages()
spark = (
    SparkSession.builder.appName("spark-app-name")
    .config("spark.jars.packages", ",".join(maven_packages))
    .getOrCreate()
)

# Create connection
mysql = MySQL(
    host="database.host.or.ip",
    user="user",
    password="*****",
    extra={"useSSL": "false", "allowPublicKeyRetrieval": "true"},
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

Get package names to be downloaded by Spark. Allows specifying a custom JDBC driver version for MySQL. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.9.0.

* **Parameters:**
  **package_version**
  : Specifies the version of the MySQL JDBC driver to use. Defaults to `9.2.0`.
    <br/>
    #### Versionadded
    Added in version 0.11.0.

### Examples

```python
from onetl.connection import MySQL

MySQL.get_packages()

# specify a custom package version
MySQL.get_packages(package_version="8.2.0")
```

<!-- !! processed by numpydoc !! -->
