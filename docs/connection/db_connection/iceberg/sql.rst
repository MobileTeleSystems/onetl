.. _iceberg-sql:

Reading from Iceberg using ``Iceberg.sql``
==========================================

``Iceberg.sql`` allows passing custom SQL query, but does not support incremental strategies.

Syntax support
--------------

Only queries with the following syntax are supported:

* ✅︎ ``SELECT ... FROM ...``
* ✅︎ ``WITH alias AS (...) SELECT ...``
* ❌ ``SET ...; SELECT ...;`` - multiple statements not supported

.. warning::

    Query should be written using `SparkSQL <https://spark.apache.org/docs/latest/sql-ref-syntax.html#data-retrieval-statements>`_ syntax.
    When using Iceberg, **table names must include catalog prefix**.

Examples
--------

.. code-block:: python

    from onetl.connection import Iceberg

    iceberg = Iceberg(catalog_name="catalog", ...)
    df = iceberg.sql(
        """
        SELECT
            id,
            key,
            CAST(value AS string) value,
            updated_at
        FROM
            catalog.schema.table
        WHERE
            key = 'something'
        """
    )

Recommendations
---------------

Select only required columns
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Avoid ``SELECT *``. List only required columns to minimize I/O and improve query performance.

Use filters
~~~~~~~~~~~

Include ``WHERE`` clauses on columns to allow Spark to prune unnecessary data, e.g. operators ``=``, ``>``, ``<``, ``BETWEEN``.
