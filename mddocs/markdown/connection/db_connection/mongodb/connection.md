<a id="mongodb-connection"></a>

# MongoDB Connection

### *class* onetl.connection.db_connection.mongodb.connection.MongoDB(\*, spark: SparkSession, database: str, host: Host, user: str, password: SecretStr, port: int = 27017, extra: MongoDBExtra = MongoDBExtra())

MongoDB connection. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Based on package [org.mongodb.spark:mongo-spark-connector:10.4.1](https://mvnrepository.com/artifact/org.mongodb.spark/mongo-spark-connector_2.12/10.4.1)
([MongoDB connector for Spark](https://www.mongodb.com/docs/spark-connector/current/))

#### SEE ALSO
Before using this connector please take into account [Prerequisites](prerequisites.md#mongodb-prerequisites)

#### Versionadded
Added in version 0.7.0.

* **Parameters:**
  **host**
  : Host of MongoDB. For example: `test.mongodb.com` or `193.168.1.17`.

  **port**
  : Port of MongoDB

  **user**
  : User, which have proper access to the database. For example: `some_user`.

  **password**
  : Password for database connection.

  **database**
  : Database in MongoDB.

  **extra**
  : Specifies one or more extra parameters by which clients can connect to the instance.
    <br/>
    For example: `{"tls": "false"}`
    <br/>
    See [Connection string options documentation](https://www.mongodb.com/docs/manual/reference/connection-string/#std-label-connections-connection-options)
    for more details

  **spark**
  : Spark session.

### Examples

```python
from onetl.connection import MongoDB
from pyspark.sql import SparkSession

# Create Spark session with MongoDB connector loaded
maven_packages = MongoDB.get_packages(spark_version="3.4")
spark = (
    SparkSession.builder.appName("spark-app-name")
    .config("spark.jars.packages", ",".join(maven_packages))
    .getOrCreate()
)

# Create connection
mongo = MongoDB(
    host="master.host.or.ip",
    user="user",
    password="*****",
    database="target_database",
    spark=spark,
).check()
```

<!-- !! processed by numpydoc !! -->

#### *classmethod* get_packages(scala_version: str | None = None, spark_version: str | None = None, package_version: str | None = None) → list[str]

Get package names to be downloaded by Spark. Allows specifying custom MongoDB Spark connector versions. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.9.0.

* **Parameters:**
  **scala_version**
  : Scala version in format `major.minor`.
    <br/>
    If `None`, `spark_version` is used to determine Scala version.

  **spark_version**
  : Spark version in format `major.minor`. Used only if `scala_version=None`.

  **package_version**
  : Specifies the version of the MongoDB Spark connector to use. Defaults to `10.4.1`.
    <br/>
    #### Versionadded
    Added in version 0.11.0.

### Examples

```python
from onetl.connection import MongoDB

MongoDB.get_packages(scala_version="2.12")

# specify custom connector version
MongoDB.get_packages(scala_version="2.12", package_version="10.4.1")
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
