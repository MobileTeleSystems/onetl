.. _iceberg-execute:

Executing statements in Iceberg
===============================

Use ``Iceberg.execute(...)`` to execute DDL and DML operations.

Syntax support
^^^^^^^^^^^^^^

This method supports **any** query syntax supported by Iceberg (Spark SQL), like:

* ✅︎ ``CREATE TABLE ...``, ``CREATE VIEW ...``
* ✅︎ ``INSERT INTO ... SELECT ...``, ``MERGE INTO ...``
* ✅︎ ``ALTER TABLE ... ADD COLUMN``, ``ALTER TABLE ... DROP COLUMN``
* ✅︎ ``DROP TABLE ...``, ``DROP VIEW ...``
* ✅︎ ``REPLACE TABLE ...``
* ✅︎ other statements supported by Iceberg
* ❌ ``SET ...; SELECT ...;`` - multiple statements not supported

.. warning::

    Queries must use `SparkSQL <https://spark.apache.org/docs/latest/sql-ref-syntax.html#ddl-statements>`_ syntax.
    When using Iceberg, **table names must include catalog prefix**.

Examples
^^^^^^^^

.. code-block:: python

    from onetl.connection import Iceberg

    iceberg = Iceberg(catalog_name="catalog", ...)

    iceberg.execute("DROP TABLE catalog.schema.table")
    iceberg.execute(
        """
        CREATE TABLE catalog.schema.table (
            id BIGINT,
            key STRING,
            value DOUBLE
        )
        USING iceberg
        """
    )

Details
-------

.. currentmodule:: onetl.connection.db_connection.iceberg.connection

.. automethod:: Iceberg.execute
