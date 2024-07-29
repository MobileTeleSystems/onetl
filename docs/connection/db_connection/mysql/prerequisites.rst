.. _mysql-prerequisites:

Prerequisites
=============

Version Compatibility
---------------------

* MySQL server versions: 5.7 - 9.0
* Spark versions: 2.3.x - 3.5.x
* Java versions: 8 - 20

See `official documentation <https://dev.mysql.com/doc/relnotes/connector-j/en/news-8-0-33.html>`_.

Installing PySpark
------------------

To use MySQL connector you should have PySpark installed (or injected to ``sys.path``)
BEFORE creating the connector instance.

See :ref:`install-spark` installation instruction for more details.

Connecting to MySQL
-----------------------

Connection host
~~~~~~~~~~~~~~~

It is possible to connect to MySQL by using either DNS name of host or it's IP address.

If you're using MySQL cluster, it is currently possible to connect only to **one specific node**.
Connecting to multiple nodes to perform load balancing, as well as automatic failover to new master/replica are not supported.

Connection port
~~~~~~~~~~~~~~~

Connection is usually performed to port 3306. Port may differ for different MySQL instances.
Please ask your MySQL administrator to provide required information.

Required grants
~~~~~~~~~~~~~~~

Ask your MySQL cluster administrator to set following grants for a user,
used for creating a connection:

.. tabs::

    .. code-tab:: sql Read + Write

        -- allow creating tables in the target schema
        GRANT CREATE ON myschema.* TO username@'192.168.1.%';

        -- allow read & write access to specific table
        GRANT SELECT, INSERT ON myschema.mytable TO username@'192.168.1.%';

    .. code-tab:: sql Read only

        -- allow read access to specific table
        GRANT SELECT ON myschema.mytable TO username@'192.168.1.%';

In example above ``'192.168.1.%''`` is a network subnet ``192.168.1.0 - 192.168.1.255``
where Spark driver and executors are running. To allow connecting user from any IP, use ``'%'`` (not secure!).

More details can be found in `official documentation <https://dev.mysql.com/doc/refman/en/grant.html>`_.
