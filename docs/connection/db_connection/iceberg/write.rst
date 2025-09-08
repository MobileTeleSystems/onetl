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
        target="my_schema.my_table",  # catalog name is already defined in connection
        options=Iceberg.WriteOptions(
            if_exists="append",
        ),
    )

    writer.run(df)

Options
-------

.. currentmodule:: onetl.connection.db_connection.iceberg.options

.. autopydantic_model:: IcebergWriteOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
