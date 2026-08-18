# Prerequisites { #DBR-onetl-connection-db-connection-iceberg-prerequisites }

!!! note

    onETL's Iceberg connection is actually a `SparkSession` configured to work with [Apache Iceberg](https://iceberg.apache.org/docs/latest/) tables. All data motion is made using Spark. Iceberg catalog (REST, Hadoop, etc.) is used only to store tables metadata, while data itself is stored in a warehouse location (HDFS, S3, or another supported filesystem).

## Version Compatibility { #DBR-onetl-connection-db-connection-iceberg-prerequisites-version-compatibility }

-   Iceberg catalog: depends on chosen implementation (e.g. REST, Hadoop)
-   Spark versions: 3.2.x -- 4.1.x
-   Java versions: 8 -- 25

See [official documentation](https://iceberg.apache.org/docs/latest/spark-getting-started/) for details on catalog and warehouse configuration.

## Installing PySpark { #DBR-onetl-connection-db-connection-iceberg-prerequisites-installing-pyspark }

To use Iceberg connector you should have PySpark installed (or injected to `sys.path`) BEFORE creating the connector instance.

See [installation instruction][DBR-onetl-install-spark] for more details.

## Popular Metastore Implementations { #DBR-onetl-connection-db-connection-iceberg-prerequisites-popular-metastore-implementations }

Iceberg supports multiple catalog implementations. Here are some popular options:

-   [Apache Iceberg Hadoop Catalog](https://iceberg.apache.org/docs/latest/spark-configuration/)
-   [Lakekeeper](https://docs.lakekeeper.io/getting-started/)
-   [Polaris](https://polaris.apache.org/in-dev/unreleased/getting-started/)
-   [Apache Gravitino](https://gravitino.apache.org/docs/)
-   [Databricks Unity Catalog](https://docs.databricks.com/aws/en/external-access/iceberg/)
