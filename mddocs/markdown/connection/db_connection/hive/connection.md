<a id="hive-connection"></a>

# Hive Connection

### *class* onetl.connection.db_connection.hive.connection.Hive(\*, spark: SparkSession, cluster: Cluster)

Spark connection with Hive MetaStore support. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### SEE ALSO
Before using this connector please take into account [Prerequisites](prerequisites.md#hive-prerequisites)

#### Versionadded
Added in version 0.1.0.

* **Parameters:**
  **cluster**
  : Cluster name. Used for HWM and lineage.
    <br/>
    #### Versionadded
    Added in version 0.7.0.

  **spark**
  : Spark session with Hive metastore support enabled

### Examples

Create Hive connection with Kerberos auth

Execute `kinit` consome command before creating Spark Session

```bash
$ kinit -kt /path/to/keytab user
```

```python
from onetl.connection import Hive
from pyspark.sql import SparkSession

# Create Spark session
# Use names "spark.yarn.access.hadoopFileSystems", "spark.yarn.principal"
# and "spark.yarn.keytab" for Spark 2

spark = (
    SparkSession.builder.appName("spark-app-name")
    .option("spark.kerberos.access.hadoopFileSystems", "hdfs://cluster.name.node:8020")
    .option("spark.kerberos.principal", "user")
    .option("spark.kerberos.keytab", "/path/to/keytab")
    .enableHiveSupport()
    .getOrCreate()
)

# Create connection
hive = Hive(cluster="rnd-dwh", spark=spark).check()
```

Create Hive connection with anonymous auth

```py
from onetl.connection import Hive
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("spark-app-name").enableHiveSupport().getOrCreate()

# Create connection
hive = Hive(cluster="rnd-dwh", spark=spark).check()
```

<!-- !! processed by numpydoc !! -->

#### *classmethod* get_current(spark: SparkSession)

Create connection for current cluster. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### NOTE
Can be used only if there are some hooks bound to
[`Slots.get_current_cluster`](slots.md#onetl.connection.db_connection.hive.slots.HiveSlots.get_current_cluster) slot.

#### Versionadded
Added in version 0.7.0.

* **Parameters:**
  **spark**
  : Spark session

### Examples

```python
from onetl.connection import Hive
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("spark-app-name").enableHiveSupport().getOrCreate()

# injecting current cluster name via hooks mechanism
hive = Hive.get_current(spark=spark)
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
