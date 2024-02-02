.. _greenplum-prerequisites:

Prerequisites
=============

Version Compatibility
---------------------

* Greenplum server versions: 5.x, 6.x, 7.x
* Spark versions: 2.3.x - 3.2.x (Spark 3.3+ is not supported yet)
* Java versions: 8 - 11

See `official documentation <https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/release_notes.html>`_.

Installing PySpark
------------------

To use Greenplum connector you should have PySpark installed (or injected to ``sys.path``)
BEFORE creating the connector instance.

See :ref:`install-spark` installation instruction for more details.

Downloading VMware package
--------------------------

To use Greenplum connector you should download connector ``.jar`` file from
`VMware website <https://network.tanzu.vmware.com/products/vmware-greenplum#/releases/1457167/file_groups/18338>`_
and then pass it to Spark session.

.. warning::

    Please pay attention to :ref:`Spark & Scala version compatibility <spark-compatibility-matrix>`.

There are several ways to do that. See :ref:`java-packages` for details.

.. note::

    If you're uploading package to private package repo, use ``groupId=io.pivotal`` and ``artifactoryId=greenplum-spark_2.12``
    (``2.12`` is Scala version) to give uploaded package a proper name.

Connecting to Greenplum
-----------------------

Interaction schema
~~~~~~~~~~~~~~~~~~

Spark executors open ports to listen incoming requests.
Greenplum segments are initiating connections to Spark executors using `EXTERNAL TABLE <https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/ref_guide-sql_commands-CREATE_EXTERNAL_TABLE.html>`_
functionality, and send/read data using `gpfdist protocol <https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/admin_guide-external-g-using-the-greenplum-parallel-file-server--gpfdist-.html#about-gpfdist-setup-and-performance-1>`_.

Data is **not** send through Greenplum master.
Greenplum master only receives commands to start reading/writing process, and manages all the metadata (external table location, schema and so on).

More details can be found in `official documentation <https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/overview.html>`_.

Number of parallel connections
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. warning::

    This is very important!!!

    If you don't limit number of connections, you can exceed the `max_connections <https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/admin_guide-client_auth.html#limiting-concurrent-connections#limiting-concurrent-connections-2>`_
    limit set on the Greenplum side. It's usually not so high, e.g. 500-1000 connections max,
    depending on your Greenplum instance settings and using connection balancers like ``pgbouncer``.

    Consuming all available connections means **nobody** (even admin users) can connect to Greenplum.

Each job on the Spark executor makes its own connection to Greenplum master node,
so you need to limit number of connections to avoid opening too many of them.

* Reading about ``5-10Gb`` of data requires about ``3-5`` parallel connections.
* Reading about ``20-30Gb`` of data requires about ``5-10`` parallel connections.
* Reading about ``50Gb`` of data requires ~ ``10-20`` parallel connections.
* Reading about ``100+Gb`` of data requires ``20-30`` parallel connections.
* Opening more than ``30-50`` connections is not recommended.

Number of connections can be limited by 2 ways:

* By limiting number of Spark executors and number of cores per-executor. Max number of parallel jobs is ``executors * cores``.

.. tabs::

    .. code-tab:: py Spark with master=local

        (
            SparkSession.builder
            # Spark will start EXACTLY 10 executors with 1 core each, so max number of parallel jobs is 10
            .config("spark.master", "local[10]")
            .config("spark.executor.cores", 1)
        )

    .. code-tab:: py Spark with master=yarn or master=k8s, dynamic allocation

        (
            SparkSession.builder
            .config("spark.master", "yarn")
            # Spark will start MAX 10 executors with 1 core each (dynamically), so max number of parallel jobs is 10
            .config("spark.dynamicAllocation.maxExecutors", 10)
            .config("spark.executor.cores", 1)
        )

    .. code-tab:: py Spark with master=yarn or master=k8s, static allocation

        (
            SparkSession.builder
            .config("spark.master", "yarn")
            # Spark will start EXACTLY 10 executors with 1 core each, so max number of parallel jobs is 10
            .config("spark.executor.instances", 10)
            .config("spark.executor.cores", 1)
        )

* By limiting connection pool size user by Spark (**only** for Spark with ``master=local``):

.. code:: python

        spark = SparkSession.builder.config("spark.master", "local[*]").getOrCreate()

        # No matter how many executors are started and how many cores they have,
        # number of connections cannot exceed pool size:
        Greenplum(
            ...,
            extra={
                "pool.maxSize": 10,
            },
        )

See `connection pooling <https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/using_the_connector.html#jdbcconnpool>`_
documentation.


* By setting :obj:`num_partitions <onetl.connection.db_connection.greenplum.options.GreenplumReadOptions.num_partitions>`
  and :obj:`partition_column <onetl.connection.db_connection.greenplum.options.GreenplumReadOptions.partition_column>` (not recommended).

Allowing connection to Greenplum master
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ask your Greenplum cluster administrator to allow your user to connect to Greenplum master node,
e.g. by updating ``pg_hba.conf`` file.

More details can be found in `official documentation <https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/admin_guide-client_auth.html#limiting-concurrent-connections#allowing-connections-to-greenplum-database-0>`_.

Network ports
~~~~~~~~~~~~~

Spark with ``master=k8s``
^^^^^^^^^^^^^^^^^^^^^^^^^

Please follow `the official documentation <https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/configure.html#k8scfg>`_

Spark with ``master=yarn`` or ``master=local``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To read data from Greenplum using Spark, following ports should be opened in firewall between Spark and Greenplum:

* Spark driver and all Spark executors -> port ``5432`` on Greenplum master node.

  This port number should be set while connecting to Greenplum:

  .. code:: python

        Greenplum(host="master.host", port=5432, ...)

* Greenplum segments -> some port range (e.g. ``41000-42000``) **listened by Spark executors**.

  This range should be set in ``extra`` option:

  .. code:: python

        Greenplum(
            ...,
            extra={
                "server.port": "41000-42000",
            },
        )

  Number of ports in this range is ``number of parallel running Spark sessions`` * ``number of parallel connections per session``.

  Number of connections per session (see below) is usually less than ``30`` (see above).

  Number of session depends on your environment:
    * For ``master=local`` only few ones-tens sessions can be started on the same host, depends on available RAM and CPU.

    * For ``master=yarn`` hundreds or thousands of sessions can be started simultaneously,
      but they are executing on different cluster nodes, so one port can be opened on different nodes at the same time.

More details can be found in official documentation:
    * `port requirements <https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/sys_reqs.html#network-port-requirements>`_
    * `format of server.port value <https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/options.html#server.port>`_
    * `port troubleshooting <https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/troubleshooting.html#port-errors>`_

Required grants
~~~~~~~~~~~~~~~

Ask your Greenplum cluster administrator to set following grants for a user,
used for creating a connection:

.. tabs::

    .. code-tab:: sql Read + write

        -- get access to get tables metadata & cluster information
        GRANT SELECT ON information_schema.tables TO username;
        GRANT SELECT ON pg_attribute TO username;
        GRANT SELECT ON pg_class TO username;
        GRANT SELECT ON pg_namespace TO username;
        GRANT SELECT ON pg_settings TO username;
        GRANT SELECT ON pg_stats TO username;
        GRANT SELECT ON gp_distributed_xacts TO username;
        GRANT SELECT ON gp_segment_configuration TO username;
        -- Greenplum 5.x only
        GRANT SELECT ON gp_distribution_policy TO username;

        -- allow creating external tables in the same schema as source/target table
        GRANT USAGE ON SCHEMA myschema TO username;
        GRANT CREATE ON SCHEMA myschema TO username;
        ALTER USER username CREATEEXTTABLE(type = 'readable', protocol = 'gpfdist') CREATEEXTTABLE(type = 'writable', protocol = 'gpfdist');

        -- allow read access to specific table (to get column types)
        -- allow write access to specific table
        GRANT SELECT, INSERT ON myschema.mytable TO username;

    .. code-tab:: sql Read only

        -- get access to get tables metadata & cluster information
        GRANT SELECT ON information_schema.tables TO username;
        GRANT SELECT ON pg_attribute TO username;
        GRANT SELECT ON pg_class TO username;
        GRANT SELECT ON pg_namespace TO username;
        GRANT SELECT ON pg_settings TO username;
        GRANT SELECT ON pg_stats TO username;
        GRANT SELECT ON gp_distributed_xacts TO username;
        GRANT SELECT ON gp_segment_configuration TO username;
        -- Greenplum 5.x only
        GRANT SELECT ON gp_distribution_policy TO username;

        -- allow creating external tables in the same schema as source table
        GRANT USAGE ON SCHEMA schema_to_read TO username;
        GRANT CREATE ON SCHEMA schema_to_read TO username;
        -- yes, ``writable``, because data is written from Greenplum to Spark executor.
        ALTER USER username CREATEEXTTABLE(type = 'writable', protocol = 'gpfdist');

        -- allow read access to specific table
        GRANT SELECT ON schema_to_read.table_to_read TO username;

    .. code-tab:: sql Write only

        -- get access to get tables metadata & cluster information
        GRANT SELECT ON information_schema.tables TO username;
        GRANT SELECT ON pg_attribute TO username;
        GRANT SELECT ON pg_class TO username;
        GRANT SELECT ON pg_namespace TO username;
        GRANT SELECT ON pg_settings TO username;
        GRANT SELECT ON pg_stats TO username;
        GRANT SELECT ON gp_distributed_xacts TO username;
        GRANT SELECT ON gp_segment_configuration TO username;
        -- Greenplum 5.x only
        GRANT SELECT ON gp_distribution_policy TO username;

        -- allow creating external tables in the same schema as target table
        GRANT USAGE ON SCHEMA schema_to_write TO username;
        GRANT CREATE ON SCHEMA schema_to_write TO username;
        -- yes, ``readable``, because data is read from Spark executor to Greenplum.
        ALTER USER username CREATEEXTTABLE(type = 'readable', protocol = 'gpfdist');

        -- allow read access to specific table (to get column types)
        -- allow write access to specific table
        GRANT SELECT, INSERT ON schema_to_write.table_to_write TO username;

More details can be found in `official documentation <https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/install_cfg.html#role-privileges>`_.
