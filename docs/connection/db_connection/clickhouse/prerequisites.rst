.. _clickhouse-prerequisites:

Prerequisites
=============

Version Compatibility
---------------------

* Clickhouse server versions:
    * Officially declared: 22.8 or higher
    * Actually supported: 21.1 or higher
* Spark versions: 2.3.x - 3.5.x
* Java versions: 8 - 20

See `official documentation <https://clickhouse.com/docs/en/integrations/java#jdbc-driver>`_.

Installing PySpark
------------------

To use Clickhouse connector you should have PySpark installed (or injected to ``sys.path``)
BEFORE creating the connector instance.

See :ref:`install-spark` installation instruction for more details.

Connecting to Clickhouse
------------------------

Connection port
~~~~~~~~~~~~~~~

Connector can only use **HTTP** (usually ``8123`` port) or **HTTPS** (usually ``8443`` port) protocol.

TCP and GRPC protocols are NOT supported.

Connecting to cluster
~~~~~~~~~~~~~~~~~~~~~

It is possible to connect to Clickhouse cluster, and use it's load balancing capabilities to read or write data in parallel.
Each Spark executor can connect to random Clickhouse nodes, instead of sending all the data to a node specified in connection params.

This requires all Clickhouse servers to run on different hosts, and **listen the same HTTP port**.
Set ``auto_discovery=True`` to enable this feature (disabled by default):

.. code:: python

    Clickhouse(
        host="node1.of.cluster",
        port=8123,
        extra={
            "auto_discovery": True,
            "load_balancing_policy": "roundRobin",
        },
    )

See `official documentation <https://clickhouse.com/docs/en/integrations/java#configuring-node-discovery-load-balancing-and-failover>`_.

Required grants
~~~~~~~~~~~~~~~

Ask your Clickhouse cluster administrator to set following grants for a user,
used for creating a connection:

.. tabs::

    .. code-tab:: sql Read + Write

        -- allow creating tables in the target schema
        GRANT CREATE TABLE ON myschema.* TO username;

        -- allow read & write access to specific table
        GRANT SELECT, INSERT ON myschema.mytable TO username;

    .. code-tab:: sql Read only

        -- allow read access to specific table
        GRANT SELECT ON myschema.mytable TO username;

More details can be found in `official documentation <https://clickhouse.com/docs/en/sql-reference/statements/grant>`_.
