.. _iceberg-read:

Reading from Iceberg using ``DBReader``
=======================================

:obj:`DBReader <onetl.db.db_reader.db_reader.DBReader>` supports :ref:`strategy` for incremental data reading,
but does not support custom queries, like ``JOIN``.

Supported DBReader features
---------------------------

* ✅︎ ``columns``
* ✅︎ ``where``
* ✅︎ ``hwm``, supported strategies:
* * ✅︎ :ref:`snapshot-strategy`
* * ✅︎ :ref:`incremental-strategy`
* * ✅︎ :ref:`snapshot-batch-strategy`
* * ✅︎ :ref:`incremental-batch-strategy`
* ✅︎ ``hint``
* ❌ ``df_schema``
* ❌ ``options`` (only Spark config params are used)

.. warning::

    ``columns``, ``where`` and  ``hwm.expression`` should be written using `SparkSQL <https://spark.apache.org/docs/latest/sql-ref-syntax.html#data-retrieval-statements>`_ syntax.

Examples
--------

Snapshot strategy:

.. code-block:: python

    from onetl.connection import Iceberg
    from onetl.db import DBReader

    iceberg = Iceberg(catalog_name="my_catalog", ...)

    reader = DBReader(
        connection=iceberg,
        source="my_schema.table",  # catalog is already defined in connection
        columns=["id", "key", "value", "updated_dt"],
        where="key = 'something'",
    )
    df = reader.run()

Incremental strategy:

.. code-block:: python

    from onetl.connection import Iceberg
    from onetl.db import DBReader
    from onetl.strategy import IncrementalStrategy

    iceberg = Iceberg(catalog_name="my_catalog", ...)

    reader = DBReader(
        connection=iceberg,
        source="my_schema.table",  # catalog is already defined in connection
        columns=["id", "key", "value", "updated_dt"],
        where="key = 'something'",
        hwm=DBReader.AutoDetectHWM(name="iceberg_hwm", expression="updated_dt"),
    )

    with IncrementalStrategy():
        df = reader.run()

Recommendations
---------------

Select only required columns
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Instead of passing ``"*"`` in ``DBReader(columns=[...])`` prefer passing exact column names.
This drastically reduces the amount of data read by Spark.
