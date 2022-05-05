.. _hwm:

HWM
=====

What is it ?
-------------

Sometimes it's necessary to read only changed rows from a table.

For example, there is a table with the following schema:

.. code:: sql

    CREATE TABLE mydata (
        id    integer PRIMARY KEY DEFAULT nextval('serial'),
        data  varchar(40)
    );

``id`` column is an autoincremental field, that's why its values are increasing all the time,
e.g. ``1000``, ``1001``, ``1002`` and etc.

New rows are constantly added into the table.

For example,
in the table yesterday's ``id`` maximum value was ``1010``.
Each today's ``id`` values is more than ``1010``, but less than ``1020``.

So we can use value of this column to mark rows we already read.
Value, that's bigger than specified one, is considered to be the new one.

This column is called ``High WaterMark`` or ``HWM`` for short.


How it is used
---------------

Firstly, we need to get the maximum value of ``id`` column:

.. code:: sql

    SELECT max(id) FROM public.mydata; -- returned 1000

Then it is possible to use this HWM value to get only new data:

.. code:: sql

    SELECT id, data FROM public.mydata WHERE id > 1000; -- since previous read attempt

.. note::

    It is highly recommended for HWM column values to be unique, like table primary key.

    Otherwise, new rows with the same column value will be skipped
    because they will not satisfy the condition.


HWM types
---------

HWM column have to be one of the following types:

1. Integer (of any length, like ``INTEGER``, ``SHORT``, ``LONG``).

2. Decimal **without** fractional part (Oracle specific, because ``INTEGER`` type here is ``NUMERIC``)

3. Date

4. Datetime (same as ``TIMESTAMP``)


HWM column restrictions
-----------------------

- HWM column values cannot decrease over time, they can only increase
- HWM column cannot contain ``NULL`` values because they cannot be tracked properly, and thus will be skipped
