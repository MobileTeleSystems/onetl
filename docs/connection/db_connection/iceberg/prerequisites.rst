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

Configuring Catalog and Warehouse
---------------------------------

To work with Iceberg tables you must configure a **catalog** and a **warehouse location**.
All configuration parameters are passed down as SparkSession options, see
`Iceberg Spark configuration <https://iceberg.apache.org/docs/latest/spark-configuration/>`_.

For now, these options can be specified using the ``extra`` field in :ref:`iceberg-connection`.

.. danger::

    This will be changed in future onETL versions.
