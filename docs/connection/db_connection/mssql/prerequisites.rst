.. _mssql-prerequisites:

Prerequisites
=============

Version Compatibility
---------------------

* SQL Server versions: 2014 - 2022
* Spark versions: 2.3.x - 3.5.x
* Java versions: 8 - 20

See `official documentation <https://learn.microsoft.com/en-us/sql/connect/jdbc/system-requirements-for-the-jdbc-driver>`_
and `official compatibility matrix <https://learn.microsoft.com/en-us/sql/connect/jdbc/microsoft-jdbc-driver-for-sql-server-support-matrix>`_.

Installing PySpark
------------------

To use MSSQL connector you should have PySpark installed (or injected to ``sys.path``)
BEFORE creating the connector instance.

See :ref:`install-spark` installation instruction for more details.

Connecting to MSSQL
--------------------

Connection port
~~~~~~~~~~~~~~~

Connection is usually performed to port 1433. Port may differ for different MSSQL instances.
Please ask your MSSQL administrator to provide required information.

For named MSSQL instances (``instanceName`` option), `port number is optional <https://learn.microsoft.com/en-us/sql/connect/jdbc/building-the-connection-url?view=sql-server-ver16#named-and-multiple-sql-server-instances>`_, and could be omitted.

Connection host
~~~~~~~~~~~~~~~

It is possible to connect to MSSQL by using either DNS name of host or it's IP address.

If you're using MSSQL cluster, it is currently possible to connect only to **one specific node**.
Connecting to multiple nodes to perform load balancing, as well as automatic failover to new master/replica are not supported.

Required grants
~~~~~~~~~~~~~~~

Ask your MSSQL cluster administrator to set following grants for a user,
used for creating a connection:

.. tabs::

    .. code-tab:: sql Read + Write (schema is owned by user)

        -- allow creating tables for user
        GRANT CREATE TABLE TO username;

        -- allow read & write access to specific table
        GRANT SELECT, INSERT ON username.mytable TO username;

        -- only if if_exists="replace_entire_table" is used:
        -- allow dropping/truncating tables in any schema
        GRANT ALTER ON username.mytable TO username;

    .. code-tab:: sql Read + Write (schema is not owned by user)

        -- allow creating tables for user
        GRANT CREATE TABLE TO username;

        -- allow managing tables in specific schema, and inserting data to tables
        GRANT ALTER, SELECT, INSERT ON SCHEMA::someschema TO username;

    .. code-tab:: sql Read only

        -- allow read access to specific table
        GRANT SELECT ON someschema.mytable TO username;

More details can be found in official documentation:
    * `GRANT ON DATABASE <https://learn.microsoft.com/en-us/sql/t-sql/statements/grant-database-permissions-transact-sql>`_
    * `GRANT ON OBJECT <https://learn.microsoft.com/en-us/sql/t-sql/statements/grant-object-permissions-transact-sql>`_
    * `GRANT ON SCHEMA <https://learn.microsoft.com/en-us/sql/t-sql/statements/grant-schema-permissions-transact-sql>`_
