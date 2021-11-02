.. hwm

HWM
=====

Sometimes it is need to read only changed rows from a table.

For example, there is a table with the following schema:

.. code:: sql

    CREATE TABLE mydata (
        id    integer PRIMARY KEY DEFAULT nextval('serial'),
        data  varchar(40)
    );

``id`` column is a autoincrement field, so its values are increasing all the time,
e.g. ``1000``, ``1001``, ``1002`` and so on.

It is possible to read only a part of the data that appeared
in the source after the previous load was performed.

Firstly we need to the maximum value of ``id`` column:

.. code:: sql

    SELECT max(id) FROM mydata;

And then save it to be used in the future reads.
It is called ``High watermark`` or ``HWM`` for short.

Then it is possible to use saved values to get only new data:

.. code:: sql

    SELECT id, data FROM mydata WHERE id > 1000; -- since previous value hwm column

HWM column is always ``NOT NULL`` and only could be one of the following types:

1. ``NUMERIC`` (a.k.a. ``INT``, ``DECIMAL``, ``NUMBER`` and so on)

2. ``DATE``

3. ``DATETIME`` (a.k.a. ``TIMESTAMP``)
