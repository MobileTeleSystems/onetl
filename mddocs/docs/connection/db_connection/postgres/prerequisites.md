# Prerequisites { #DBR-onetl-connection-db-connection-postgres-prerequisites }

## Version Compatibility { #DBR-onetl-connection-db-connection-postgres-prerequisites-version-compatibility }

- PostgreSQL server versions:
    - Officially declared: 9.1 - 18
    - Actually tested: 9.4.26, 18.3
- Spark versions: 3.2.x -- 4.2.x
- Java versions: 8 -- 25

See [official documentation](https://jdbc.postgresql.org/download/).

## Installing PySpark { #DBR-onetl-connection-db-connection-postgres-prerequisites-installing-pyspark }

To use Postgres connector you should have PySpark installed (or injected to `sys.path`) **BEFORE** creating the connector instance.

See [installation instruction][DBR-onetl-install-spark] for more details.

## Connecting to Postgres { #DBR-onetl-connection-db-connection-postgres-prerequisites-connecting-to-postgres }

### Allowing connection to Postgres instance { #DBR-onetl-connection-db-connection-postgres-prerequisites-allowing-connection-to-postgres-instance }

Ask your Postgres administrator to allow your user (and probably IP) to connect to instance, e.g. by updating `pg_hba.conf` file.

See [official documentation](https://www.postgresql.org/docs/current/auth-pg-hba-conf.html).

### Connection port { #DBR-onetl-connection-db-connection-postgres-prerequisites-connection-port }

Connection is usually performed to port 5432. Port may differ for different Postgres instances.
Please ask your Postgres administrator to provide required information.

### Connection host { #DBR-onetl-connection-db-connection-postgres-prerequisites-connection-host }

It is possible to connect to Postgres by using either DNS name of host or it's IP address.

If you're using Postgres cluster, it is currently possible to connect only to **one specific node**.
Connecting to multiple nodes to perform load balancing, as well as automatic failover to new master/replica are not supported.

### Required grants { #DBR-onetl-connection-db-connection-postgres-prerequisites-required-grants }

Ask your Postgres cluster administrator to set following grants for a user, used for creating a connection:

=== "Read + Write"

    ```sql
    -- allow creating tables in specific schema
    GRANT USAGE, CREATE ON SCHEMA myschema TO username;

    -- allow read & write access to specific table
    GRANT SELECT, INSERT ON myschema.mytable TO username;

    -- only if if_exists="replace_entire_table" is used:
    GRANT TRUNCATE ON myschema.mytable TO username;
    ```

=== "Read only"

    ```sql
    -- allow creating tables in specific schema
    GRANT USAGE ON SCHEMA myschema TO username;

    -- allow read access to specific table
    GRANT SELECT ON myschema.mytable TO username;
    ```

More details can be found in [official documentation](https://www.postgresql.org/docs/current/sql-grant.html).
