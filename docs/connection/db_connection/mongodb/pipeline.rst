.. _mongodb-sql:

Reading from MongoDB using ``MongoDB.pipeline``
===============================================

:obj:`MongoDB.sql <onetl.connection.db_connection.mongodb.connection.MongoDB.pipeline>` allows passing custom pipeline,
but does not support incremental strategies.

.. warning::

    Please take into account :ref:`mongodb-types`

Recommendations
---------------

Pay attention to ``pipeline`` value
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Instead of filtering data on Spark side using ``df.filter(df.column == 'value')`` pass proper ``mongodb.pipeline(..., pipeline={"$match": {"column": {"$eq": "value"}}})`` value.
This both reduces the amount of data send from MongoDB to Spark, and may also improve performance of the query.
Especially if there are indexes for columns used in ``pipeline`` value.

References
----------

.. currentmodule:: onetl.connection.db_connection.mongodb.connection

.. automethod:: MongoDB.pipeline

.. currentmodule:: onetl.connection.db_connection.mongodb.options

.. autopydantic_model:: MongoDBPipelineOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
