.. _mongodb-prerequisites:

Prerequisites
=============

Version Compatibility
---------------------

* MongoDB server versions:
    * Officially declared: 4.0 or higher
    * Actually tested: 4.0.0, 8.0.4
* Spark versions: 3.2.x - 4.0.x
* Java versions: 8 - 22

See `official documentation <https://www.mongodb.com/docs/spark-connector/>`_.

Installing PySpark
------------------

To use MongoDB connector you should have PySpark installed (or injected to ``sys.path``)
BEFORE creating the connector instance.

See :ref:`install-spark` installation instruction for more details.

Connecting to MongoDB
---------------------

Connection host
~~~~~~~~~~~~~~~

It is possible to connect to MongoDB host by using either DNS name of host or it's IP address.

It is also possible to connect to MongoDB shared cluster:

.. code-block:: python


    mongo = MongoDB(
        host="master.host.or.ip",
        user="user",
        password="*****",
        database="target_database",
        spark=spark,
        extra={
            # read data from secondary cluster node, switch to primary if not available
            "readPreference": "secondaryPreferred",
        },
    )

Supported ``readPreference`` values are described in `official documentation <https://www.mongodb.com/docs/manual/core/read-preference/>`_.

Connection port
~~~~~~~~~~~~~~~

Connection is usually performed to port ``27017``. Port may differ for different MongoDB instances.
Please ask your MongoDB administrator to provide required information.

Required grants
~~~~~~~~~~~~~~~

Ask your MongoDB cluster administrator to set following grants for a user,
used for creating a connection:

.. tabs::

    .. code-tab:: js Read + Write

        // allow writing data to specific database
        db.grantRolesToUser("username", [{db: "somedb", role: "readWrite"}])

    .. code-tab:: js Read only

        // allow reading data from specific database
        db.grantRolesToUser("username", [{db: "somedb", role: "read"}])

See:
    * `db.grantRolesToUser documentation <https://www.mongodb.com/docs/manual/reference/method/db.grantRolesToUser>`_
    * `MongoDB builtin roles <https://www.mongodb.com/docs/manual/reference/built-in-roles>`_
