.. _mongo:

MongoDB connection
=====================

.. currentmodule:: onetl.connection.db_connection.mongodb

.. autosummary::

    MongoDB
    MongoDB.ReadOptions
    MongoDB.WriteOptions
    MongoDB.PipelineOptions

.. autoclass:: MongoDB
    :members: check, pipeline

.. currentmodule:: onetl.connection.db_connection.mongodb.MongoDB

.. autopydantic_model:: ReadOptions

.. autopydantic_model:: WriteOptions
    :members: mode
    :member-order: bysource

.. autopydantic_model:: PipelineOptions
