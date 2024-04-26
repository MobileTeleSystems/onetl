.. _clickhouse-prerequisites:

Prerequisites
=============

Version Compatibility
---------------------

* Clickhouse server versions: 21.1 or higher
* Spark versions: 2.3.x - 3.5.x
* Java versions: 8 - 20

See `official documentation <https://clickhouse.com/docs/en/integrations/java#jdbc-driver>`_.

Installing PySpark
------------------

To use Clickhouse connector you should have PySpark installed (or injected to ``sys.path``)
BEFORE creating the connector instance.

See :ref:`install-spark` installation instruction for more details.

Connecting to Clickhouse
-----------------------

Connection port
~~~~~~~~~~~~~~~

Connector can only use **HTTP** (usually ``8123`` port) or **HTTPS** (usually ``8443`` port) protocol.

TCP and GRPC protocols are NOT supported.

Clickhouse cluster interaction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you're using Clickhouse cluster, it is currently possible to connect only to one specific cluster node.
Connecting to multiple nodes simultaneously is not supported.

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
