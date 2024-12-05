.. _postgres-prerequisites:

Prerequisites
=============

Version Compatibility
---------------------

* PostgreSQL server versions:
    * Officially declared: 8.2  - 17
    * Actually tested: 9.4, 17
* Spark versions: 2.3.x - 3.5.x
* Java versions: 8 - 20

See `official documentation <https://jdbc.postgresql.org/>`_.

Installing PySpark
------------------

To use Postgres connector you should have PySpark installed (or injected to ``sys.path``)
BEFORE creating the connector instance.

See :ref:`install-spark` installation instruction for more details.

Connecting to Postgres
-----------------------

Allowing connection to Postgres instance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ask your Postgres administrator to allow your user (and probably IP) to connect to instance,
e.g. by updating ``pg_hba.conf`` file.

See `official documentation <https://www.postgresql.org/docs/current/auth-pg-hba-conf.html>`_.

Connection port
~~~~~~~~~~~~~~~

Connection is usually performed to port 5432. Port may differ for different Postgres instances.
Please ask your Postgres administrator to provide required information.

Connection host
~~~~~~~~~~~~~~~

It is possible to connect to Postgres by using either DNS name of host or it's IP address.

If you're using Postgres cluster, it is currently possible to connect only to **one specific node**.
Connecting to multiple nodes to perform load balancing, as well as automatic failover to new master/replica are not supported.

Required grants
~~~~~~~~~~~~~~~

Ask your Postgres cluster administrator to set following grants for a user,
used for creating a connection:

.. tabs::

    .. code-tab:: sql Read + Write

        -- allow creating tables in specific schema
        GRANT USAGE, CREATE ON SCHEMA myschema TO username;

        -- allow read & write access to specific table
        GRANT SELECT, INSERT ON myschema.mytable TO username;

        -- only if if_exists="replace_entire_table" is used:
        GRANT TRUNCATE ON myschema.mytable TO username;

    .. code-tab:: sql Read only

        -- allow creating tables in specific schema
        GRANT USAGE ON SCHEMA myschema TO username;

        -- allow read access to specific table
        GRANT SELECT ON myschema.mytable TO username;

More details can be found in `official documentation <https://www.postgresql.org/docs/current/sql-grant.html>`_.
