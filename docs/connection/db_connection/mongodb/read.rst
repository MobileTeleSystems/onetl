.. _mongodb-read:

Reading from MongoDB
====================

There are 2 ways of distributed data reading from MongoDB:

* Using :obj:`DBReader <onetl.db.db_reader.db_reader.DBReader>` with different :ref:`strategy` and :obj:`MongoDBReadOptions <onetl.connection.db_connection.mongodb.options.MongoDBReadOptions>`
* Using :obj:`MongoDB.pipeline <onetl.connection.db_connection.mongodb.connection.MongoDB.pipeline>` with :obj:`MongoDBPipelineOptions <onetl.connection.db_connection.mongodb.options.MongoDBPipelineOptions>`

.. currentmodule:: onetl.connection.db_connection.mongodb.connection

.. automethod:: MongoDB.pipeline

.. currentmodule:: onetl.connection.db_connection.mongodb.options

.. autopydantic_model:: MongoDBReadOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false

.. autopydantic_model:: MongoDBPipelineOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
