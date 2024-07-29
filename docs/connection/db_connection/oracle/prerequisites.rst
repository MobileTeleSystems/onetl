.. _oracle-prerequisites:

Prerequisites
=============

Version Compatibility
---------------------

* Oracle Server versions:
    * Officially declared: 19c, 21c, 23ai
    * Actually tested: 11.2, 23.5
* Spark versions: 3.2.x - 4.0.x
* Java versions: 8 - 22

See `official documentation <https://www.oracle.com/cis/database/technologies/appdev/jdbc-downloads.html>`_.

Installing PySpark
------------------

To use Oracle connector you should have PySpark installed (or injected to ``sys.path``)
BEFORE creating the connector instance.

See :ref:`install-spark` installation instruction for more details.

Connecting to Oracle
--------------------

Connection port
~~~~~~~~~~~~~~~

Connection is usually performed to port 1521. Port may differ for different Oracle instances.
Please ask your Oracle administrator to provide required information.

Connection host
~~~~~~~~~~~~~~~

It is possible to connect to Oracle by using either DNS name of host or it's IP address.

If you're using Oracle cluster, it is currently possible to connect only to **one specific node**.
Connecting to multiple nodes to perform load balancing, as well as automatic failover to new master/replica are not supported.

Connect as proxy user
~~~~~~~~~~~~~~~~~~~~~

It is possible to connect to database as another user without knowing this user password.

This can be enabled by granting user a special ``CONNECT THROUGH`` permission:

.. code-block:: sql

    ALTER USER schema_owner GRANT CONNECT THROUGH proxy_user;

Then you can connect to Oracle using credentials of ``proxy_user`` but specify that you need permissions of ``schema_owner``:

.. code-block:: python

    oracle = Oracle(
        ...,
        user="proxy_user[schema_owner]",
        password="proxy_user password",
    )

See `official documentation <https://oracle-base.com/articles/misc/proxy-users-and-connect-through>`_.

Required grants
~~~~~~~~~~~~~~~

Ask your Oracle cluster administrator to set following grants for a user,
used for creating a connection:

.. tabs::

    .. code-tab:: sql Read + Write (schema is owned by user)

        -- allow user to log in
        GRANT CREATE SESSION TO username;

        -- allow creating tables in user schema
        GRANT CREATE TABLE TO username;

        -- allow read & write access to specific table
        GRANT SELECT, INSERT ON username.mytable TO username;

    .. code-tab:: sql Read + Write (schema is not owned by user)

        -- allow user to log in
        GRANT CREATE SESSION TO username;

        -- allow creating tables in any schema,
        -- as Oracle does not support specifying exact schema name
        GRANT CREATE ANY TABLE TO username;

        -- allow read & write access to specific table
        GRANT SELECT, INSERT ON someschema.mytable TO username;

        -- only if if_exists="replace_entire_table" is used:
        -- allow dropping/truncating tables in any schema,
        -- as Oracle does not support specifying exact schema name
        GRANT DROP ANY TABLE TO username;

    .. code-tab:: sql Read only

        -- allow user to log in
        GRANT CREATE SESSION TO username;

        -- allow read access to specific table
        GRANT SELECT ON someschema.mytable TO username;

More details can be found in official documentation:
    * `GRANT <https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/GRANT.html>`_
    * `SELECT <https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/SELECT.html>`_
    * `CREATE TABLE <https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/SELECT.html>`_
    * `INSERT <https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/INSERT.html>`_
    * `TRUNCATE TABLE <https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/TRUNCATE-TABLE.html>`_
