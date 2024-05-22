.. _mongodb-write:

Writing to MongoDB using ``DBWriter``
=====================================

For writing data to MongoDB, use :obj:`DBWriter <onetl.db.db_writer.db_writer.DBWriter>`.

.. warning::

    Please take into account :ref:`mongodb-types`

Examples
--------

.. code-block:: python

    from onetl.connection import MongoDB
    from onetl.db import DBWriter

    mongodb = MongoDB(...)

    df = ...  # data is here

    writer = DBWriter(
        connection=mongodb,
        target="schema.table",
        options=MongoDB.WriteOptions(
            if_exists="append",
        ),
    )

    writer.run(df)


Write options
-------------

Method above accepts  :obj:`MongoDB.WriteOptions <onetl.connection.db_connection.mongodb.options.MongoDBWriteOptions>`

.. currentmodule:: onetl.connection.db_connection.mongodb.options

.. autopydantic_model:: MongoDBWriteOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
