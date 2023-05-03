.. _mongo:

MongoDB connection
=====================

.. currentmodule:: onetl.connection.db_connection.mongo

.. autosummary::

    MongoDB
    MongoDB.ReadOptions
    MongoDB.WriteOptions
    MongoDB.PipelineOptions

.. autoclass:: MongoDB
    :members: check, pipeline

.. currentmodule:: onetl.connection.db_connection.mongo.MongoDB

.. autopydantic_model:: ReadOptions

.. autopydantic_model:: WriteOptions
    :members: mode
    :member-order: bysource

.. autopydantic_model:: PipelineOptions
