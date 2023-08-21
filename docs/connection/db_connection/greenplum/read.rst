.. _greenplum-read:

Reading from Greenplum
=======================

For reading data from Greenplum, use :obj:`DBReader <onetl.db.db_reader.db_reader.DBReader>` with options below.

.. note::

    Unlike JDBC connectors, *Greenplum connector for Spark* does not support
    executing **custom** SQL queries using ``.sql`` method, because this leads to sending
    the result through *master* node which is really bad for cluster performance.

    To make distributed queries like ``JOIN`` **on Greenplum side**, you should create a staging table,
    populate it with the data you need (using ``.execute`` method to call ``INSERT INTO ... AS SELECT ...``),
    then read the data from this table using :obj:`DBReader <onetl.db.db_reader.db_reader.DBReader>`,
    and drop staging table after reading is finished.

    In this case data will be read directly from Greenplum segment nodes in a distributed way.

.. warning::

    Greenplum connection does **NOT** support reading data from views which does not have ``gp_segment_id`` column.
    Either add this column to a view, or use stating table solution (see above).

.. currentmodule:: onetl.connection.db_connection.greenplum.options

.. autopydantic_model:: GreenplumReadOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
