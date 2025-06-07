<a id="spark-local-fs"></a>

# Spark LocalFS

### *class* onetl.connection.file_df_connection.spark_local_fs.SparkLocalFS(\*, spark: SparkSession)

Spark connection to local filesystem. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Based on [Spark Generic File Data Source](https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html).

#### WARNING
To use SparkHDFS connector you should have PySpark installed (or injected to `sys.path`)
BEFORE creating the connector instance.

See [Spark](../../install/spark.md#install-spark) installation instruction for more details.

#### WARNING
Currently supports only Spark sessions created with option `spark.master: local`.

#### NOTE
Supports only reading files as Spark DataFrame and writing DataFrame to files.

Does NOT support file operations, like create, delete, rename, etc.

#### Versionadded
Added in version 0.9.0.

* **Parameters:**
  **spark**
  : Spark session

### Examples

```python
from onetl.connection import SparkLocalFS
from pyspark.sql import SparkSession

# create Spark session
spark = SparkSession.builder.master("local").appName("spark-app-name").getOrCreate()

# create connection
local_fs = SparkLocalFS(spark=spark).check()
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
