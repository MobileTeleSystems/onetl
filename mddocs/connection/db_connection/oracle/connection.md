<a id="oracle-connection"></a>

# Oracle connection

### *class* onetl.connection.db_connection.oracle.connection.Oracle(\*, spark: SparkSession, user: str, password: SecretStr, host: Host, port: int = 1521, sid: str | None = None, service_name: str | None = None, extra: OracleExtra = OracleExtra())

Oracle JDBC connection. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Based on Maven package [com.oracle.database.jdbc:ojdbc8:23.7.0.25.01](https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8/23.7.0.25.01)
([official Oracle JDBC driver](https://www.oracle.com/cis/database/technologies/appdev/jdbc-downloads.html)).

#### SEE ALSO
Before using this connector please take into account [Prerequisites](prerequisites.md#oracle-prerequisites)

#### Versionadded
Added in version 0.1.0.

* **Parameters:**
  **host**
  : Host of Oracle database. For example: `test.oracle.domain.com` or `193.168.1.10`

  **port**
  : Port of Oracle database

  **user**
  : User, which have proper access to the database. For example: `SOME_USER`

  **password**
  : Password for database connection

  **sid**
  : Sid of oracle database. For example: `XE`
    <br/>
    #### WARNING
    You should provide either `sid` or `service_name`, not both of them

  **service_name**
  : Specifies one or more names by which clients can connect to the instance.
    <br/>
    For example: `PDB1`.
    <br/>
    #### WARNING
    You should provide either `sid` or `service_name`, not both of them

  **spark**
  : Spark session.

  **extra**
  : Specifies one or more extra parameters by which clients can connect to the instance.
    <br/>
    For example: `{"remarksReporting": "false"}`
    <br/>
    See official documentation:
    : * [Connection parameters](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleDriver.html)
      * [Connection properties](https://docs.oracle.com/cd/A97335_02/apps.102/a83724/basic1.htm#1024018)

### Examples

Create and check Oracle connection with `sid`:

```python
from onetl.connection import Oracle
from pyspark.sql import SparkSession

# Create Spark session with Oracle driver loaded
maven_packages = Oracle.get_packages()
spark = (
    SparkSession.builder.appName("spark-app-name")
    .config("spark.jars.packages", ",".join(maven_packages))
    .getOrCreate()
)

# Create connection
oracle = Oracle(
    host="database.host.or.ip",
    user="user",
    password="*****",
    sid="XE",
    extra={"remarksReporting": "false"},
    spark=spark,
).check()
```

or with `service_name`:

```python
...

oracle = Oracle(
    host="database.host.or.ip",
    user="user",
    password="*****",
    service_name="PDB1",  # <--- instead of SID
    extra={"remarksReporting": "false"},
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

#### *classmethod* get_packages(java_version: str | None = None, package_version: str | None = None) → list[str]

Get package names to be downloaded by Spark. Allows specifying custom JDBC driver versions for Oracle. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

* **Parameters:**
  **java_version**
  : Java major version, defaults to “8”. Must be “8” or “11”.

  **package_version**
  : Specifies the version of the Oracle JDBC driver to use. Defaults to “23.7.0.25.01”.

### Examples

```python
from onetl.connection import Oracle

Oracle.get_packages()

# specify Java and package versions
Oracle.get_packages(java_version="8", package_version="23.7.0.25.01")
```

<!-- !! processed by numpydoc !! -->
