.. _hive-prerequisites:

Prerequisites
=============

.. note::

    onETL's Hive connection is actually SparkSession with access to `Hive Thrift Metastore <https://docs.cloudera.com/cdw-runtime/1.5.0/hive-hms-overview/topics/hive-hms-introduction.html>`_
    and HDFS/S3.
    All data motion is made using Spark. Hive Metastore is used only to store tables and partitions metadata.

    This connector does **NOT** require Hive server. It also does **NOT** use Hive JDBC connector.

Version Compatibility
---------------------

* Hive Metastore version:
    * Officially declared: 0.12 - 3.1.3 (may require to add proper .jar file explicitly)
    * Actually tested: 1.2.100, 2.3.10, 3.1.3
* Spark versions: 2.3.x - 3.5.x
* Java versions: 8 - 20

See `official documentation <https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html>`_.


Installing PySpark
------------------

To use Hive connector you should have PySpark installed (or injected to ``sys.path``)
BEFORE creating the connector instance.

See :ref:`install-spark` installation instruction for more details.

Connecting to Hive Metastore
----------------------------

.. note::

    If you're using managed Hadoop cluster, skip this step, as all Spark configs are should already present on the host.

Create ``$SPARK_CONF_DIR/hive-site.xml`` with Hive Metastore URL:

.. code:: xml

    <?xml version="1.0"?>
    <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
    <configuration>
        <property>
            <name>hive.metastore.uris</name>
            <value>thrift://metastore.host.name:9083</value>
        </property>
    </configuration>

Create ``$SPARK_CONF_DIR/core-site.xml`` with warehouse location ,e.g. HDFS IPC port of Hadoop namenode, or S3 bucket address & credentials:

.. tabs::

    .. code-tab:: xml HDFS

        <?xml version="1.0" encoding="UTF-8"?>
        <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
        <configuration>
            <property>
                <name>fs.defaultFS</name>
                <value>hdfs://myhadoopcluster:9820</value>
            </property>
        </configuration>

    .. code-tab:: xml S3

        <?xml version="1.0" encoding="UTF-8"?>
        <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
        <configuration>
            !-- See https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#General_S3A_Client_configuration
            <property>
                <name>fs.defaultFS</name>
                <value>s3a://mys3bucket/</value>
            </property>
            <property>
                <name>fs.s3a.bucket.mybucket.endpoint</name>
                <value>http://s3.somain</value>
            </property>
            <property>
                <name>fs.s3a.bucket.mybucket.connection.ssl.enabled</name>
                <value>false</value>
            </property>
            <property>
                <name>fs.s3a.bucket.mybucket.path.style.access</name>
                <value>true</value>
            </property>
            <property>
                <name>fs.s3a.bucket.mybucket.aws.credentials.provider</name>
                <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
            </property>
            <property>
                <name>fs.s3a.bucket.mybucket.access.key</name>
                <value>some-user</value>
            </property>
            <property>
                <name>fs.s3a.bucket.mybucket.secret.key</name>
                <value>mysecrettoken</value>
            </property>
        </configuration>

Using Kerberos
--------------

Some of Hadoop managed clusters use Kerberos authentication. In this case, you should call `kinit <https://web.mit.edu/kerberos/krb5-1.12/doc/user/user_commands/kinit.html>`_ command
**BEFORE** starting Spark session to generate Kerberos ticket. See :ref:`install-kerberos`.

Sometimes it is also required to pass keytab file to Spark config, allowing Spark executors to generate own Kerberos tickets:

.. tabs::

    .. code-tab:: python Spark 3

        SparkSession.builder
            .option("spark.kerberos.access.hadoopFileSystems", "hdfs://namenode1.domain.com:9820,hdfs://namenode2.domain.com:9820")
            .option("spark.kerberos.principal", "user")
            .option("spark.kerberos.keytab", "/path/to/keytab")
            .gerOrCreate()

    .. code-tab:: python Spark 2

        SparkSession.builder
            .option("spark.yarn.access.hadoopFileSystems", "hdfs://namenode1.domain.com:9820,hdfs://namenode2.domain.com:9820")
            .option("spark.yarn.principal", "user")
            .option("spark.yarn.keytab", "/path/to/keytab")
            .gerOrCreate()

See `Spark security documentation <https://spark.apache.org/docs/latest/security.html#kerberos>`_
for more details.
