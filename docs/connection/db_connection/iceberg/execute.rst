.. _iceberg-execute:

Executing statements in Iceberg
===============================

Use ``Iceberg.execute(...)`` to execute DDL and DML operations.

.. warning::

    In DML/DDL queries **table names must include catalog prefix**.

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

Examples
^^^^^^^^

.. code-block:: python

    from onetl.connection import Iceberg

    iceberg = Iceberg(catalog_name="my_catalog", ...)

    iceberg.execute("DROP TABLE my_catalog.my_schema.my_table")
    iceberg.execute(
        """
        CREATE TABLE my_catalog.my_schema.my_table (
            id BIGINT,
            key STRING,
            value DOUBLE
        )
        USING iceberg
        """,
    )

Details
-------

.. currentmodule:: onetl.connection.db_connection.iceberg.connection

.. automethod:: Iceberg.execute
