.. _mongodb-sql:

Reading from MongoDB using ``MongoDB.pipeline``
===============================================

.. warning::

    Please take into account :ref:`mongodb-types`

:obj:`MongoDB.sql <onetl.connection.db_connection.mongodb.connection.MongoDB.pipeline>` allows passing custom pipeline,
but does not support incremental strategies.

References
----------

.. currentmodule:: onetl.connection.db_connection.mongodb.connection

.. automethod:: MongoDB.pipeline

.. currentmodule:: onetl.connection.db_connection.mongodb.options

.. autopydantic_model:: MongoDBPipelineOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
