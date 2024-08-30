.. _teradata-prerequisites:

Prerequisites
=============

Version Compatibility
---------------------

* Teradata server versions:
    * Officially declared: 16.10 - 20.0
    * Actually tested: 16.10
* Spark versions: 2.3.x - 3.5.x
* Java versions: 8 - 20

See `official documentation <https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/platformMatrix.html>`_.

Installing PySpark
------------------

To use Teradata connector you should have PySpark installed (or injected to ``sys.path``)
BEFORE creating the connector instance.

See :ref:`install-spark` installation instruction for more details.

Connecting to Teradata
-----------------------

Connection host
~~~~~~~~~~~~~~~

It is possible to connect to Teradata by using either DNS name Parsing Engine (PE) host, or it's IP address.

Connection port
~~~~~~~~~~~~~~~

Connection is usually performed to port ``1025``. Port may differ for different Teradata instances.
Please ask your Teradata administrator to provide required information.

Required grants
~~~~~~~~~~~~~~~

Ask your Teradata cluster administrator to set following grants for a user,
used for creating a connection:

.. tabs::

    .. code-tab:: sql Read + Write

        -- allow creating tables in the target schema
        GRANT CREATE TABLE ON database TO username;

        -- allow read & write access to specific table
        GRANT SELECT, INSERT ON database.mytable TO username;

    .. code-tab:: sql Read only

        -- allow read access to specific table
        GRANT SELECT ON database.mytable TO username;

See:
    * `Teradata access rights <https://www.dwhpro.com/teradata-access-rights/>`_
    * `GRANT documentation <https://teradata.github.io/presto/docs/0.167-t/sql/grant.html>`_
