<a id="greenplum-connection"></a>

# Greenplum connection

### *class* onetl.connection.db_connection.greenplum.connection.Greenplum(\*, spark: SparkSession, host: Host, user: str, password: SecretStr, database: str, port: int = 5432, extra: GreenplumExtra = GreenplumExtra(tcpKeepAlive='true'))

Greenplum connection. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Based on package `io.pivotal:greenplum-spark:2.2.0`
([VMware Greenplum connector for Spark](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/index.html)).

#### SEE ALSO
Before using this connector please take into account [Prerequisites](prerequisites.md#greenplum-prerequisites)

#### Versionadded
Added in version 0.5.0.

* **Parameters:**
  **host**
  : Host of Greenplum master. For example: `test.greenplum.domain.com` or `193.168.1.17`

  **port**
  : Port of Greenplum master

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
    For example: `{"tcpKeepAlive": "true", "server.port": "50000-65535"}`
    <br/>
    Supported options are:
    : * All [Postgres JDBC driver properties](https://jdbc.postgresql.org/documentation/use/)
      * Properties from [Greenplum connector for Spark documentation](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/options.html) page, but only starting with `server.` or `pool.`

### Examples

Create and check Greenplum connection:

```python
from onetl.connection import Greenplum
from pyspark.sql import SparkSession

# Create Spark session with Greenplum connector loaded
maven_packages = Greenplum.get_packages(spark_version="3.2")
spark = (
    SparkSession.builder.appName("spark-app-name")
    .config("spark.jars.packages", ",".join(maven_packages))
    .config("spark.executor.allowSparkContext", "true")
    # IMPORTANT!!!
    # Set number of executors according to "Prerequisites" -> "Number of executors"
    .config("spark.dynamicAllocation.maxExecutors", 10)
    .config("spark.executor.cores", 1)
    .getOrCreate()
)

# IMPORTANT!!!
# Set port range of executors according to "Prerequisites" -> "Network ports"
extra = {
    "server.port": "41000-42000",
}

# Create connection
greenplum = Greenplum(
    host="master.host.or.ip",
    user="user",
    password="*****",
    database="target_database",
    extra=extra,
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

#### *classmethod* get_packages(\*, scala_version: str | None = None, spark_version: str | None = None, package_version: str | None = None) → list[str]

Get package names to be downloaded by Spark. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### WARNING
You should pass either `scala_version` or `spark_version`.

#### Versionadded
Added in version 0.9.0.

* **Parameters:**
  **scala_version**
  : Scala version in format `major.minor`.
    <br/>
    If `None`, `spark_version` is used to determine Scala version.

  **spark_version**
  : Spark version in format `major.minor`.
    <br/>
    Used only if `scala_version=None`.

  **package_version**
  : Package version in format `major.minor.patch`
    <br/>
    #### Versionadded
    Added in version 0.10.1.

### Examples

```python
from onetl.connection import Greenplum

Greenplum.get_packages(scala_version="2.12")
Greenplum.get_packages(spark_version="3.2", package_version="2.3.0")
```

<!-- !! processed by numpydoc !! -->
