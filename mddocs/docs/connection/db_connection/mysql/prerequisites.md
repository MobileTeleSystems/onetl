# Prerequisites { #DBR-onetl-connection-db-connection-mysql-prerequisites }

## Version Compatibility { #DBR-onetl-connection-db-connection-mysql-prerequisites-version-compatibility }

- MySQL server versions:
    - Officially declared: 8.0 - 9.7
    - Actually tested: 5.7.16, 9.5.0
- Spark versions: 3.2.x -- 4.2.x
- Java versions: 8 -- 25

See [official documentation](https://dev.mysql.com/doc/connector-j/en/connector-j-versions.html).

## Installing PySpark { #DBR-onetl-connection-db-connection-mysql-prerequisites-installing-pyspark }

To use MySQL connector you should have PySpark installed (or injected to `sys.path`) BEFORE creating the connector instance.

See [installation instruction][DBR-onetl-install-spark] for more details.

## Connecting to MySQL { #DBR-onetl-connection-db-connection-mysql-prerequisites-connecting-to-mysql }

### Connection host { #DBR-onetl-connection-db-connection-mysql-prerequisites-connection-host }

It is possible to connect to MySQL by using either DNS name of host or it's IP address.

If you're using MySQL cluster, it is currently possible to connect only to **one specific node**.
Connecting to multiple nodes to perform load balancing, as well as automatic failover to new master/replica are not supported.

### Connection port { #DBR-onetl-connection-db-connection-mysql-prerequisites-connection-port }

Connection is usually performed to port 3306. Port may differ for different MySQL instances.
Please ask your MySQL administrator to provide required information.

### Required grants { #DBR-onetl-connection-db-connection-mysql-prerequisites-required-grants }

Ask your MySQL cluster administrator to set following grants for a user, used for creating a connection:

=== "Read + Write"

    ```sql
    -- allow creating tables in the target schema
    GRANT CREATE ON myschema.* TO username@'192.168.1.%';

    -- allow read & write access to specific table
    GRANT SELECT, INSERT ON myschema.mytable TO username@'192.168.1.%';
    ```

=== "Read only"

    ```sql
    -- allow read access to specific table
    GRANT SELECT ON myschema.mytable TO username@'192.168.1.%';
    ```

In example above `'192.168.1.%''` is a network subnet `192.168.1.0 - 192.168.1.255`
where Spark driver and executors are running. To allow connecting user from any IP, use `'%'` (not secure!).

More details can be found in [official documentation](https://dev.mysql.com/doc/refman/en/grant.html).
