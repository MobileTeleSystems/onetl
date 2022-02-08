.. _hwm:

HWM
=====

What is it
-----------

Sometimes it is need to read only changed rows from a table.

For example, there is a table with the following schema:

.. code:: sql

    CREATE TABLE mydata (
        id    integer PRIMARY KEY DEFAULT nextval('serial'),
        data  varchar(40)
    );

``id`` column is a autoincrement field, so its values are increasing all the time,
e.g. ``1000``, ``1001``, ``1002`` and so on.

New rows are constantly added into the table.

For example,
yesterday the maximum ``id`` value in the table was 1010.
Today all values have ``id`` above ``1010`` and below ``1020``.

So we can use value of this column to mark rows we already read.
Whose which value is above specified are new ones.

Such a column is called ``High watermark`` or ``HWM`` for short.


How it is used
---------------

Firstly we need to get the maximum value of ``id`` column:

.. code:: sql

    SELECT max(id) FROM public.mydata; -- returned 1000

Then it is possible to use this HWM value to get only new data:

.. code:: sql

    SELECT id, data FROM public.mydata WHERE id > 1000; -- since previous read attempt

.. note::

    It is highly recommended for HWM column values to be unique, like table primary key.

    Otherwise new rows with the same column value will be skipped
    because they will not satisfy the condition.


HWM types
---------

HWM column could be one of the following types:

1. Integer (of any length, like ``INTEGER``, ``SHORT``, ``LONG``).

2. Decimal **without** fractional part (Oracle specific, because ``INTEGER`` type here is ``NUMERIC``)

3. Date

4. Datetime (same as ``TIMESTAMP``)


HWM column restrictions
-----------------------

- HWM column values cannot decrease over time, they can only increase
- HWM column cannot contain ``NULL`` values because they cannot be tracked properly, and thus will be skipped
