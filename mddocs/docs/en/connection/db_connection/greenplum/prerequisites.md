# Prerequisites { #greenplum-prerequisites }

## Version Compatibility

- Greenplum server versions:
  - Officially declared: 5.x, 6.x, and 7.x (which requires `Greenplum.get_packages(package_version="2.3.0")` or higher)
  - Actually tested: 6.23, 7.0
- Spark versions: 2.3.x - 3.2.x (Spark 3.3+ is not supported yet)
- Java versions: 8 - 11

See [official documentation](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.2/greenplum-connector-spark/release_notes.html).

## Installing PySpark

To use Greenplum connector you should have PySpark installed (or injected to `sys.path`)
BEFORE creating the connector instance.

See [installation instruction][install-spark] for more details.

## Downloading VMware package

To use Greenplum connector you should download connector `.jar` file from
[VMware website](https://network.tanzu.vmware.com/products/vmware-greenplum#/releases/1413479/file_groups/16966)
and then pass it to Spark session.

!!! warning

    Please pay attention to [Spark & Scala version compatibility][spark-compatibility-matrix].

!!! warning

    There are issues with using package of version 2.3.0/2.3.1 with Greenplum 6.x - connector can
    open transaction with `SELECT * FROM table LIMIT 0` query, but does not close it, which leads to deadlocks
    during write.

There are several ways to do that. See [install Java packages][java-packages] for details.

!!! note

    If you're uploading package to private package repo, use `groupId=io.pivotal` and `artifactoryId=greenplum-spark_2.12`
    (`2.12` is Scala version) to give uploaded package a proper name.

## Connecting to Greenplum

### Interaction schema

Spark executors open ports to listen incoming requests.
Greenplum segments are initiating connections to Spark executors using [EXTERNAL TABLE](https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/ref_guide-sql_commands-CREATE_EXTERNAL_TABLE.html)
functionality, and send/read data using [gpfdist protocol](https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/admin_guide-external-g-using-the-greenplum-parallel-file-server--gpfdist-.html#about-gpfdist-setup-and-performance-1).

Data is **not** send through Greenplum master.
Greenplum master only receives commands to start reading/writing process, and manages all the metadata (external table location, schema and so on).

More details can be found in [official documentation](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/overview.html).

### Set number of connections

!!! warning

    This is very important!!!

    If you don't limit number of connections, you can exceed the [max_connections](https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/admin_guide-client_auth.html#limiting-concurrent-connections#limiting-concurrent-connections-2)
    limit set on the Greenplum side. It's usually not so high, e.g. 500-1000 connections max,
    depending on your Greenplum instance settings and using connection balancers like `pgbouncer`.

    Consuming all available connections means **nobody** (even admin users) can connect to Greenplum.

Each job on the Spark executor makes its own connection to Greenplum master node,
so you need to limit number of connections to avoid opening too many of them.

- Reading about `5-10Gb` of data requires about `3-5` parallel connections.
- Reading about `20-30Gb` of data requires about `5-10` parallel connections.
- Reading about `50Gb` of data requires ~ `10-20` parallel connections.
- Reading about `100+Gb` of data requires `20-30` parallel connections.
- Opening more than `30-50` connections is not recommended.

Number of connections can be limited by 2 ways:

- By limiting number of Spark executors and number of cores per-executor. Max number of parallel jobs is `executors * cores`.

=== "Spark with master=local"

    ```python 

        spark = (
            SparkSession.builder
            # Spark will run with 5 threads in local mode, allowing up to 5 parallel tasks
            .config("spark.master", "local[5]")
            .config("spark.executor.cores", 1)
        ).getOrCreate() 
    ```

=== "Spark with master=yarn or master=k8s, dynamic allocation"

    ```python 

        spark = (
            SparkSession.builder
            .config("spark.master", "yarn")
            # Spark will start MAX 10 executors with 1 core each (dynamically), so max number of parallel jobs is 10
            .config("spark.dynamicAllocation.maxExecutors", 10)
            .config("spark.executor.cores", 1)
        ).getOrCreate() 
    ```

=== "Spark with master=yarn or master=k8s, static allocation"

    ```python

        spark = (
            SparkSession.builder
            .config("spark.master", "yarn")
            # Spark will start EXACTLY 10 executors with 1 core each, so max number of parallel jobs is 10
            .config("spark.executor.instances", 10)
            .config("spark.executor.cores", 1)
        ).getOrCreate() 
    ```

- By limiting connection pool size user by Spark (**only** for Spark with `master=local`):

    ```python
        spark = SparkSession.builder.config("spark.master", "local[*]").getOrCreate()

        # No matter how many executors are started and how many cores they have,
        # number of connections cannot exceed pool size:
        Greenplum(
            ...,
            extra={
                "pool.maxSize": 10,
            },
        ) 
    ```

See [connection pooling](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/using_the_connector.html#jdbcconnpool)
documentation.

- By setting [num_partitions][onetl.connection.db_connection.greenplum.options.GreenplumReadOptions.num_partitions]
  and [partition_column][onetl.connection.db_connection.greenplum.options.GreenplumReadOptions.partition_column] (not recommended).

### Allowing connection to Greenplum master

Ask your Greenplum cluster administrator to allow your user to connect to Greenplum master node,
e.g. by updating `pg_hba.conf` file.

More details can be found in [official documentation](https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/admin_guide-client_auth.html#limiting-concurrent-connections#allowing-connections-to-greenplum-database-0).

### Set connection port

#### Connection port for Spark with `master=k8s`

Please follow [the official documentation](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/configure.html#k8scfg)

#### Connection port for Spark with `master=yarn` or `master=local`

To read data from Greenplum using Spark, following ports should be opened in firewall between Spark and Greenplum:

- Spark driver and all Spark executors -> port `5432` on Greenplum master node.

  This port number should be set while connecting to Greenplum:

        ```python
        greenplum = Greenplum(host="master.host", port=5432, ...)
        ```

- Greenplum segments -> some port range (e.g. `41000-42000`) **listened by Spark executors**.

  This range should be set in `extra` option:

        ```python
            greenplum = Greenplum(
                ...,
                extra={
                    "server.port": "41000-42000",
                },
            )
        ```

  Number of ports in this range is `number of parallel running Spark sessions` * `number of parallel connections per session`.

  Number of connections per session (see below) is usually less than `30` (see above).

  Number of session depends on your environment:

- For `master=local` only few ones-tens sessions can be started on the same host, depends on available RAM and CPU.
- For `master=yarn` hundreds or thousands of sessions can be started simultaneously, but they are executing on different cluster nodes, so one port can be opened on different nodes at the same time.

More details can be found in official documentation:

- [port requirements](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/sys_reqs.html#network-port-requirements)
- [format of server.port value](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/options.html#server.port)
- [port troubleshooting](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/troubleshooting.html#port-errors)

### Set connection host

#### Connection host for Spark with `master=k8s`

Please follow [the official documentation](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/configure.html#k8scfg)

#### Connection host for Spark with `master=local`

By default, Greenplum connector tries to resolve IP of current host, and then pass it as `gpfdist` URL to Greenplum segment.
This may fail in some cases.

For example, IP can be resolved using `/etc/hosts` content like this:

    ```text
        127.0.0.1 localhost real-host-name
    ```

    ```bash
        $ hostname -f
        localhost

        $ hostname -i
        127.0.0.1
    ```

Reading/writing data to Greenplum will fail with following exception:

    ```text
        org.postgresql.util.PSQLException: ERROR: connection with gpfdist failed for
        "gpfdist://127.0.0.1:49152/local-1709739764667/exec/driver",
        effective url: "http://127.0.0.1:49152/local-1709739764667/exec/driver":
        error code = 111 (Connection refused);  (seg3 slice1 12.34.56.78:10003 pid=123456)
    ```

There are 2 ways to fix that:

- Explicitly pass your host IP address to connector, like this

        ```python
            import os

            # pass here real host IP (accessible from GP segments)
            os.environ["HOST_IP"] = "192.168.1.1"

            greenplum = Greenplum(
                ...,
                extra={
                    # connector will read IP from this environment variable
                    "server.hostEnv": "env.HOST_IP",
                },
                spark=spark,
            )
        ```

  More details can be found in [official documentation](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/options.html#server.hostenv).

- Update `/etc/hosts` file to include real host IP:

        ```text
        127.0.0.1 localhost
        # this IP should be accessible from GP segments
        192.168.1.1 driver-host-name
        ```

  So Greenplum connector will properly resolve host IP.

#### Connection host for Spark with `master=yarn`

The same issue with resolving IP address can occur on Hadoop cluster node, but it's tricky to fix, because each node has a different IP.

There are 3 ways to fix that:

- Pass node hostname to `gpfdist` URL. So IP will be resolved on segment side:

        ```python
        greenplum = Greenplum(
            ...,
            extra={
                "server.useHostname": "true",
            },
        )
        ```

  But this may fail if Hadoop cluster node hostname cannot be resolved from Greenplum segment side.

  More details can be found in [official documentation](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/options.html#server.usehostname).

- Set specific network interface to get IP address from:

        ```python
        greenplum = Greenplum(
            ...,
            extra={
                "server.nic": "eth0",
            },
        ) 
        ```

  You can get list of network interfaces using this command.

!!! note

    This command should be executed on Hadoop cluster node, **not** Spark driver host!

    ```bash
        $ ip address
        1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 qdisc noqueue state UNKNOWN group default qlen 1000
            inet 127.0.0.1/8 scope host lo
            valid_lft forever preferred_lft forever
        2: eth0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc fq_codel state UP group default qlen 1000
            inet 192.168.1.1/24 brd 192.168.1.255 scope global dynamic noprefixroute eth0
            valid_lft 83457sec preferred_lft 83457sec
    ```

  Note that in this case **each** Hadoop cluster node node should have network interface with name `eth0`.

  More details can be found in [official documentation](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/options.html#server.nic).

- Update `/etc/hosts` on each Hadoop cluster node to include real node IP:

        ```text
            127.0.0.1 localhost
            # this IP should be accessible from GP segments
            192.168.1.1 cluster-node-name
        ```

  So Greenplum connector will properly resolve node IP.

### Set required grants

Ask your Greenplum cluster administrator to set following grants for a user,
used for creating a connection:

=== "Read + Write"

    ```sql 

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
    ```

=== "Read only"

    ```sql

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
        -- yes, `writable` for reading from GP, because data is written from Greenplum to Spark executor.
        ALTER USER username CREATEEXTTABLE(type = 'writable', protocol = 'gpfdist');

        -- allow read access to specific table
        GRANT SELECT ON schema_to_read.table_to_read TO username;
    ```

More details can be found in [official documentation](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/install_cfg.html#role-privileges).
