<a id="mssql-connection"></a>

# MSSQL connection

### *class* onetl.connection.db_connection.mssql.connection.MSSQL(\*, spark: SparkSession, user: str, password: SecretStr, database: str, host: Host, port: int | None = None, extra: MSSQLExtra = MSSQLExtra())

MSSQL JDBC connection. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Based on Maven package [com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre8](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc/12.8.1.jre8)
([official MSSQL JDBC driver](https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server)).

#### SEE ALSO
Before using this connector please take into account [Prerequisites](prerequisites.md#mssql-prerequisites)

* **Parameters:**
  **host**
  : Host of MSSQL database. For example: `test.mssql.domain.com` or `192.168.1.14`

  **port**
  : Port of MSSQL database
    <br/>
    #### Versionchanged
    Changed in version 0.11.1: Default value was changed from `1433` to `None`,
    to allow automatic port discovery with `instanceName`.

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
    For example: `{"connectRetryCount": 3, "connectRetryInterval": 10}`
    <br/>
    See [MSSQL JDBC driver properties documentation](https://learn.microsoft.com/en-us/sql/connect/jdbc/setting-the-connection-properties#properties)
    for more details

### Examples

Create MSSQL connection with plain auth

```py
from onetl.connection import MSSQL
from pyspark.sql import SparkSession

# Create Spark session with MSSQL driver loaded
maven_packages = MSSQL.get_packages()
spark = (
    SparkSession.builder.appName("spark-app-name")
    .config("spark.jars.packages", ",".join(maven_packages))
    .getOrCreate()
)

# Create connection
mssql = MSSQL(
    host="database.host.or.ip",
    port=1433,
    user="user",
    password="*****",
    extra={
        "trustServerCertificate": "true",  # add this to avoid SSL certificate issues
    },
    spark=spark,
)
```

Create MSSQL connection with domain auth

```py
# Create Spark session with MSSQL driver loaded
...

# Create connection
mssql = MSSQL(
    host="database.host.or.ip",
    port=1433,
    user="user",
    password="*****",
    extra={
        "domain": "some.domain.com",  # add here your domain
        "integratedSecurity": "true",
        "authenticationScheme": "NTLM",
        "trustServerCertificate": "true",  # add this to avoid SSL certificate issues
    },
    spark=spark,
)
```

Create MSSQL connection with instance name

```py
# Create Spark session with MSSQL driver loaded
...

# Create connection
mssql = MSSQL(
    host="database.host.or.ip",
    # !!! no port !!!
    user="user",
    password="*****",
    extra={
        "instanceName": "myinstance",  # add here your instance name
        "trustServerCertificate": "true",  # add this to avoid SSL certificate issues
    },
    spark=spark,
)
```

Create MSSQL read-only connection

```py
# Create Spark session with MSSQL driver loaded
...

# Create connection
mssql = MSSQL(
    host="database.host.or.ip",
    port=1433,
    user="user",
    password="*****",
    extra={
        "applicationIntent": "ReadOnly",  # driver will open read-only connection, to avoid writing to the database
        "trustServerCertificate": "true",  # add this to avoid SSL certificate issues
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

#### *classmethod* get_packages(java_version: str | None = None, package_version: str | None = None) → list[str]

Get package names to be downloaded by Spark. Allows specifying custom JDBC driver versions for MSSQL.  [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.9.0.

* **Parameters:**
  **java_version**
  : Java major version, defaults to `8`. Must be `8` or `11`.

  **package_version**
  : Specifies the version of the MSSQL JDBC driver to use. Defaults to `12.8.1.`.

### Examples

```python
from onetl.connection import MSSQL

MSSQL.get_packages()

# specify Java and package versions
MSSQL.get_packages(java_version="8", package_version="12.8.1.jre11")
```

<!-- !! processed by numpydoc !! -->
