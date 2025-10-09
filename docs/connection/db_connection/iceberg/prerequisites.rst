.. _iceberg-prerequisites:

Prerequisites
=============

.. note::

    onETL's Iceberg connection is actually a ``SparkSession`` configured to work with
    `Apache Iceberg <https://iceberg.apache.org/docs/latest/>`_ tables.
    All data motion is made using Spark.
    Iceberg catalog (REST, Hadoop, etc.) is used only to store tables metadata,
    while data itself is stored in a warehouse location (HDFS, S3, or another supported filesystem).

Version Compatibility
---------------------

* Iceberg catalog: depends on chosen implementation (e.g. REST, Hadoop)
* Spark versions: 3.2.x – 4.0.x
* Java versions: 8 – 22

See `official documentation <https://iceberg.apache.org/docs/latest/spark-getting-started/>`_
for details on catalog and warehouse configuration.

Installing PySpark
------------------

To use Iceberg connector you should have PySpark installed (or injected to ``sys.path``)
BEFORE creating the connector instance.

See :ref:`install-spark` installation instruction for more details.

Popular Metastore Implementations
---------------------------------

Iceberg supports multiple catalog implementations. Here are some popular options:

* **Apache Iceberg Hadoop Catalog** — `File-based catalog using Hadoop filesystem <https://iceberg.apache.org/docs/latest/spark-configuration/>`_
* **Apache Iceberg REST Catalog** — `Iceberg REST catalog <https://iceberg.apache.org/rest-catalog-spec/>`_
* **Polaris** — `Сatalog service enabling multi-engine table access <https://polaris.apache.org/in-dev/unreleased/getting-started/>`_
* **Lakekeeper** — `Rust native сatalog service <https://docs.lakekeeper.io/getting-started/>`_
* **Gravitino** — `Apache Gravitino unified catalog <https://gravitino.apache.org/docs/>`_
* **Unity Catalog** — `Databricks Unity Catalog <https://docs.databricks.com/aws/en/external-access/iceberg/>`_
