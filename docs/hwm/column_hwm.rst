.. _column-hwm:

Column HWM
==========

What is HWM?
-------------

Sometimes it's necessary to read only changed rows from a table.

For example, there is a table with the following schema:

.. code:: sql

    CREATE TABLE mydata (
        id    integer PRIMARY KEY DEFAULT nextval('serial'),
        data  varchar(40)
    );

``id`` column is an autoincremental field, that's why its values are increasing all the time
when new rows are added into the table.

For example, yesterday table contained rows:

.. code::

    1000, "some data"
    1001, "some data1"
    1002, "some data2"
    ...
    1009, "some data9"
    1010, "some data10"

Maximum value in ``id`` column is ``1010``.

Today it contains some new rows:

.. code::

    ...
    1009, "some data9"
    1010, "some data10"
    ----- new rows ----
    1011, "some data11"
    1012, "some data12"
    ...
    1020, "some data20"

So we can use value of ``id`` column to detect rows added since yesterday, e.g.
using query like ```SELECT * FROM table WHERE id > 1010``.

The ``id`` column here is called ``High WaterMark`` or ``HWM`` for short.
It is used by different :ref:`strategy` to implement some complex logic
of filtering source data.


Supported types
---------------

HWM column have to be one of the following types:

1. Integer (of any length, like ``INTEGER``, ``SHORT``, ``LONG``).

2. Decimal **without** fractional part (Oracle specific, because ``INTEGER`` type here is ``NUMERIC``)

3. Date

4. Datetime (a.k.a ``TIMESTAMP``)

Other column types (like ``DOUBLE`` or ``VARCHAR``) are **not** supported.
But it is possible to use some expression as HWM, like ``CAST(column as TYPE)`` or ``func(column) as hwm``.

See strategies and :ref:`db-reader` documentation for examples.


Restrictions
------------

- HWM column values cannot decrease over time, they can only increase
- HWM column cannot contain ``NULL`` values because they cannot be tracked properly, and thus will be skipped


Recommendations
---------------

- It is highly recommended for HWM column values to be unique, like a table primary key.

  Otherwise, new rows with the same column value as stored in HWM will be skipped,
  because they will not match the WHERE clause.

- It is recommended to add an index for HWM column.

  Filtering by non-indexed rows requires sequential scan for all rows in a table, with complexity ``O(n)``,
  which can take *a lot* of time on large tables, especially if RDBMS is not distributed.

  Filtering by row index has complexity ``O(log n)`` (for B-tree), which is very effective.

  .. note::

    Filtering performance depends on the index implementation and internal RDBMS optimization engine.

  .. note::

    Use indexes which support ``<`` and ``>`` operations.
    Hash-based indexes, which support only ``=`` and ``IN`` operations, cannot be used
