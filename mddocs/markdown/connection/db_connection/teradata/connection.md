<a id="teradata-connection"></a>

# Teradata connection

### *class* onetl.connection.db_connection.teradata.connection.Teradata(\*, spark: SparkSession, user: str, password: SecretStr, host: Host, port: int = 1025, database: str | None = None, extra: TeradataExtra = TeradataExtra(CHARSET='UTF8', COLUMN_NAME='ON', FLATTEN='ON', MAYBENULL='ON', STRICT_NAMES='OFF'))

Teradata JDBC connection. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Based on package [com.teradata.jdbc:terajdbc:17.20.00.15](https://central.sonatype.com/artifact/com.teradata.jdbc/terajdbc/17.20.00.15)
([official Teradata JDBC driver](https://downloads.teradata.com/download/connectivity/jdbc-driver)).

#### SEE ALSO
Before using this connector please take into account [Prerequisites](prerequisites.md#teradata-prerequisites)

#### Versionadded
Added in version 0.1.0.

* **Parameters:**
  **host**
  : Host of Teradata database. For example: `test.teradata.domain.com` or `193.168.1.12`

  **port**
  : Port of Teradata database

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
  : Specifies one or more extra parameters which should be appended to a connection string.
    <br/>
    For example: `{"TMODE": "TERA", "MAYBENULL": "ON", "CHARSET": "UTF8", "LOGMECH":"LDAP"}`
    <br/>
    See [Teradata JDBC driver documentation](https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html#BABJIHBJ)
    for more details
    <br/>
    #### NOTE
    By default, these options are added to extra:
    > * `CHARSET = "UTF8"`
    > * `COLUMN_NAME = "ON"` - allow reading column title from a table
    > * `FLATTEN = "ON"` - improves error messages
    > * `MAYBENULL = "ON"`
    > * `STRICT_NAMES = "OFF"` - ignore Spark options passed to JDBC URL
    <br/>
    It is possible to override default values, for example set `extra={"FLATTEN": "OFF"}`

### Examples

Create Teradata connection with LDAP auth:

```python
from onetl.connection import Teradata
from pyspark.sql import SparkSession

# Create Spark session with Teradata driver loaded
maven_packages = Teradata.get_packages()
spark = (
    SparkSession.builder.appName("spark-app-name")
    .config("spark.jars.packages", ",".join(maven_packages))
    .getOrCreate()
)

# Create connection
teradata = Teradata(
    host="database.host.or.ip",
    user="user",
    password="*****",
    extra={
        "TMODE": "TERA",  # "TERA" or "ANSI"
        "LOGMECH": "LDAP",
        "LOG": "TIMING",  # increase log level
    },
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

Get package names to be downloaded by Spark. Allows specifying custom JDBC driver versions for Teradata. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.9.0.

* **Parameters:**
  **package_version**
  : Specifies the version of the Teradata JDBC driver to use. Defaults to `17.20.00.15`.
    <br/>
    #### Versionadded
    Added in version 0.11.0.

### Examples

```python
from onetl.connection import Teradata

Teradata.get_packages()

# specify custom driver version
Teradata.get_packages(package_version="20.00.00.18")
```

<!-- !! processed by numpydoc !! -->
