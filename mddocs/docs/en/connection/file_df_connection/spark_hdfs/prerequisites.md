# Prerequisites { #spark-hdfs-prerequisites }

## Version Compatibility

- Hadoop versions: 2.x, 3.x (only with Hadoop 3.x libraries)
- Spark versions: 2.3.x - 3.5.x
- Java versions: 8 - 20

## Installing PySpark

To use SparkHDFS connector you should have PySpark installed (or injected to `sys.path`)
BEFORE creating the connector instance.

See [installation instruction][install-spark] for more details.

## Using Kerberos

Some of Hadoop managed clusters use Kerberos authentication. In this case, you should call [kinit](https://web.mit.edu/kerberos/krb5-1.12/doc/user/user_commands/kinit.html) command
**BEFORE** starting Spark session to generate Kerberos ticket. See [Kerberos installation instructions][install-kerberos].

Sometimes it is also required to pass keytab file to Spark config, allowing Spark executors to generate own Kerberos tickets:

=== "Spark 3"

    ```python

        SparkSession.builder
            .option("spark.kerberos.access.hadoopFileSystems", "hdfs://namenode1.domain.com:9820,hdfs://namenode2.domain.com:9820")
            .option("spark.kerberos.principal", "user")
            .option("spark.kerberos.keytab", "/path/to/keytab")
            .gerOrCreate()
    ```

=== "Spark 2"

    ```python 

        SparkSession.builder
            .option("spark.yarn.access.hadoopFileSystems", "hdfs://namenode1.domain.com:9820,hdfs://namenode2.domain.com:9820")
            .option("spark.yarn.principal", "user")
            .option("spark.yarn.keytab", "/path/to/keytab")
            .gerOrCreate()
    ```

See [Spark security documentation](https://spark.apache.org/docs/latest/security.html#kerberos)
for more details.
