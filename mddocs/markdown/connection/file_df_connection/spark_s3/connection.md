<a id="spark-s3-connection"></a>

# Spark S3 Connection

### *class* onetl.connection.file_df_connection.spark_s3.connection.SparkS3(\*, spark: SparkSession, host: Host, port: int | None = None, bucket: str, protocol: Literal['http', 'https'] = 'https', access_key: str | None = None, secret_key: SecretStr | None = None, session_token: SecretStr | None = None, region: str | None = None, extra: SparkS3Extra = SparkS3Extra())

Spark connection to S3 filesystem. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Based on [Hadoop-AWS module](https://hadoop.apache.org/docs/current3/hadoop-aws/tools/hadoop-aws/index.html)
and [Spark integration with Cloud Infrastructures](https://spark.apache.org/docs/latest/cloud-integration.html).

#### SEE ALSO
Before using this connector please take into account [Prerequisites](prerequisites.md#spark-s3-prerequisites)

#### NOTE
Supports only reading files as Spark DataFrame and writing DataFrame to files.

Does NOT support file operations, like create, delete, rename, etc. For these operations,
use [`S3`](../../file_connection/s3.md#onetl.connection.file_connection.s3.S3) connection.

#### Versionadded
Added in version 0.9.0.

* **Parameters:**
  **host**
  : Host of S3 source. For example: `domain.com`

  **port**
  : Port of S3 source

  **bucket**
  : Bucket name in the S3 file source

  **protocol**
  : Connection protocol. Allowed values: `https` or `http`

  **access_key**
  : Access key (aka user ID) of an account in the S3 service

  **secret_key**
  : Secret key (aka password) of an account in the S3 service

  **session_token**
  : Session token of your account in S3 service

  **region**
  : Region name of bucket in S3 service

  **extra**
  : A dictionary of additional properties to be used when connecting to S3.
    <br/>
    These are Hadoop AWS specific properties, see links below:
    * [Hadoop AWS](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#General_S3A_Client_configuration)
    * [Hadoop AWS committers options](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/committers.html)
    <br/>
    Options are passed without prefixes `spark.hadoop.`, `fs.s3a.` and `fs.s3a.bucket.$BUCKET.`, for example:
    ```python
    extra = {
        "path.style.access": True,
        "committer.magic.enabled": True,
        "committer.name": "magic",
        "connection.timeout": 300000,
    }
    ```
    <br/>
    #### WARNING
    Options that populated from connection
    attributes (like `endpoint`, `access.key`) are not allowed to override.
    <br/>
    But you may override `aws.credentials.provider` and pass custom credential options.

  **spark**
  : Spark session

### Examples

Create S3 connection with bucket as subdomain (`my-bucket.domain.com`):

```py
from onetl.connection import SparkS3
from pyspark.sql import SparkSession

# Create Spark session with Hadoop AWS libraries loaded
maven_packages = SparkS3.get_packages(spark_version="3.5.5")
# Some packages are not used, but downloading takes a lot of time. Skipping them.
excluded_packages = SparkS3.get_exclude_packages()
spark = (
    SparkSession.builder.appName("spark-app-name")
    .config("spark.jars.packages", ",".join(maven_packages))
    .config("spark.jars.excludes", ",".join(excluded_packages))
    .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
    .config("spark.hadoop.fs.s3a.committer.name", "magic")
    .config(
        "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
        "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory",
    )
    .config(
        "spark.sql.parquet.output.committer.class",
        "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
    )
    .config(
        "spark.sql.sources.commitProtocolClass",
        "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol",
    )
    .getOrCreate()
)

# Create connection
s3 = SparkS3(
    host="domain.com",
    protocol="http",
    bucket="my-bucket",
    access_key="ACCESS_KEY",
    secret_key="SECRET_KEY",
    spark=spark,
).check()
```

Create S3 connection with bucket as subpath (`domain.com/my-bucket`)

```py
# Create Spark session with Hadoop AWS libraries loaded
...

# Create connection
s3 = SparkS3(
    host="domain.com",
    protocol="http",
    bucket="my-bucket",
    access_key="ACCESS_KEY",
    secret_key="SECRET_KEY",
    extra={
        "path.style.access": True,  # <---
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

#### close()

Close all connections created to S3. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Also resets all `fs.s3a.bucket.$BUCKET.*` properties of Hadoop configuration.

#### NOTE
Connection can be used again after it was closed.

* **Returns:**
  Connection itself

### Examples

Close connection automatically:

```python
with connection:
    ...
```

Close connection manually:

```python
connection.close()
```

<!-- !! processed by numpydoc !! -->

#### *classmethod* get_exclude_packages() → list[str]

Get package names to be excluded by Spark. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.13.0.

### Examples

```python
from onetl.connection import SparkS3

SparkS3.get_exclude_packages()
```

<!-- !! processed by numpydoc !! -->

#### *classmethod* get_packages(spark_version: str, scala_version: str | None = None) → list[str]

Get package names to be downloaded by Spark. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.9.0.

* **Parameters:**
  **spark_version**
  : Spark version in format `major.minor.patch`.

  **scala_version**
  : Scala version in format `major.minor`.
    <br/>
    If `None`, `spark_version` is used to determine Scala version.

### Examples

```python
from onetl.connection import SparkS3

SparkS3.get_packages(spark_version="3.5.5")
SparkS3.get_packages(spark_version="3.5.5", scala_version="2.12")
```

<!-- !! processed by numpydoc !! -->
