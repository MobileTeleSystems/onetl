.. _iceberg-write:

Writing to Iceberg using ``DBWriter``
=====================================

For writing data to Iceberg, use :obj:`DBWriter <onetl.db.db_writer.db_writer.DBWriter>`.

Examples
--------

.. code-block:: python

    from onetl.connection import Iceberg
    from onetl.db import DBWriter

    iceberg = Iceberg(catalog_name="my_catalog", ...)

    df = ...  # data is here

    writer = DBWriter(
        connection=iceberg,
        target="schema.my_table",  # catalog is already defined in connection
        options=Iceberg.WriteOptions(
            if_exists="append",
        ),
    )

    writer.run(df)
