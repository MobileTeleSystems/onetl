.. _db-reader:

DB Reader
=========

.. currentmodule:: onetl.db.db_reader.db_reader

.. autosummary::

    DBReader
    DBReader.run

.. autoclass:: DBReader
    :members:

.. automethod:: DBReader.run

.. automethod:: DBReader.has_data

    .. warning::
       This method should be used within the context of :ref:`strategy` if you use them. It may not work as expected when used outside of a strategy context. This does not apply to :ref:`snapshot-strategy`, where the method functions normally.

.. automethod:: DBReader.raise_error_if_no_data

    .. warning::
       This method should be used within the context of :ref:`strategy` if you use them. It may not work as expected when used outside of a strategy context. This does not apply to :ref:`snapshot-strategy`, where the method functions normally.
