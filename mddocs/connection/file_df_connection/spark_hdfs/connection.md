<a id="spark-hdfs-connection"></a>

# Spark HDFS Connection

### *class* onetl.connection.file_df_connection.spark_hdfs.connection.SparkHDFS(\*, spark: SparkSession, cluster: Cluster, host: Host | None = None, port: int = 8020)

Spark connection to HDFS. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Based on [Spark Generic File Data Source](https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html).

#### SEE ALSO
Before using this connector please take into account [Prerequisites](prerequisites.md#spark-hdfs-prerequisites)

#### NOTE
Supports only reading files as Spark DataFrame and writing DataFrame to files.

Does NOT support file operations, like create, delete, rename, etc. For these operations,
use `HDFS` connection.

#### Versionadded
Added in version 0.9.0.

* **Parameters:**
  **cluster**
  : Cluster name.
    <br/>
    Used for:
    : * HWM and lineage (as instance name for file paths)
      * Validation of `host` value,
        : if latter is passed and if some hooks are bound to
          [`Slots.get_cluster_namenodes`](slots.md#onetl.connection.file_df_connection.spark_hdfs.slots.SparkHDFSSlots.get_cluster_namenodes).

  **host**
  : Hadoop namenode host. For example: `namenode1.domain.com`.
    <br/>
    Should be an active namenode (NOT standby).
    <br/>
    If value is not set, but there are some hooks bound to
    [`Slots.get_cluster_namenodes`](slots.md#onetl.connection.file_df_connection.spark_hdfs.slots.SparkHDFSSlots.get_cluster_namenodes)
    and
    [`Slots.is_namenode_active`](slots.md#onetl.connection.file_df_connection.spark_hdfs.slots.SparkHDFSSlots.is_namenode_active),
    onETL will iterate over cluster namenodes to detect which one is active.

  **ipc_port**
  : Port of Hadoop namenode (IPC protocol).
    <br/>
    If omitted, but there are some hooks bound to
    [`Slots.get_ipc_port`](slots.md#onetl.connection.file_df_connection.spark_hdfs.slots.SparkHDFSSlots.get_ipc_port),
    onETL will try to detect port number for a specific `cluster`.

  **spark**
  : Spark session

### Examples

Create SparkHDFS connection with Kerberos auth

Execute `kinit` consome command before creating Spark Session

```bash
$ kinit -kt /path/to/keytab user
```

```python
from onetl.connection import SparkHDFS
from pyspark.sql import SparkSession

# Create Spark session.
# Use names "spark.yarn.access.hadoopFileSystems", "spark.yarn.principal"
# and "spark.yarn.keytab" for Spark 2

spark = (
    SparkSession.builder.appName("spark-app-name")
    .option(
        "spark.kerberos.access.hadoopFileSystems",
        "hdfs://namenode1.domain.com:8020",
    )
    .option("spark.kerberos.principal", "user")
    .option("spark.kerberos.keytab", "/path/to/keytab")
    .enableHiveSupport()
    .getOrCreate()
)

# Create connection
hdfs = SparkHDFS(
    host="namenode1.domain.com",
    cluster="rnd-dwh",
    spark=spark,
).check()
```

Create SparkHDFS connection with anonymous auth

```py
from onetl.connection import SparkHDFS
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.master("local").appName("spark-app-name").getOrCreate()

# Create connection
hdfs = SparkHDFS(
    host="namenode1.domain.com",
    cluster="rnd-dwh",
    spark=spark,
).check()
```

Use cluster name to detect active namenode

Can be used only if some third-party plugin provides [Spark HDFS Slots](slots.md#spark-hdfs-slots) implementation

```python
# Create Spark session
...

# Create connection
hdfs = SparkHDFS(cluster="rnd-dwh", spark=spark).check()
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

#### *classmethod* get_current(spark: SparkSession)

Create connection for current cluster. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Automatically sets up current cluster name as `cluster`.

#### NOTE
Can be used only if there are a some hooks bound to
[`Slots.get_current_cluster`](slots.md#onetl.connection.file_df_connection.spark_hdfs.slots.SparkHDFSSlots.get_current_cluster).

#### Versionadded
Added in version 0.9.0.

* **Parameters:**
  **spark**
  : See [`SparkHDFS`](#onetl.connection.file_df_connection.spark_hdfs.connection.SparkHDFS) constructor documentation.

### Examples

```python
from onetl.connection import SparkHDFS

# injecting current cluster name via hooks mechanism
hdfs = SparkHDFS.get_current(spark=spark)
```

<!-- !! processed by numpydoc !! -->
