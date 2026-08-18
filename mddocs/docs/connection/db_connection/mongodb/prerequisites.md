# Prerequisites { #DBR-onetl-connection-db-connection-mongodb-prerequisites }

## Version Compatibility { #DBR-onetl-connection-db-connection-mongodb-prerequisites-version-compatibility }

- MongoDB server versions:
    - Officially declared: 4.0 or higher
    - Actually tested: 4.0.0, 8.3.1
- Spark versions: 3.2.x -- 4.2.x
- Java versions: 8 -- 25

See [official documentation](https://www.mongodb.com/docs/spark-connector/).

## Installing PySpark { #DBR-onetl-connection-db-connection-mongodb-prerequisites-installing-pyspark }

To use MongoDB connector you should have PySpark installed (or injected to `sys.path`)
BEFORE creating the connector instance.

See [installation instruction][DBR-onetl-install-spark] for more details.

## Connecting to MongoDB { #DBR-onetl-connection-db-connection-mongodb-prerequisites-connecting-to-mongodb }

### Connection host { #DBR-onetl-connection-db-connection-mongodb-prerequisites-connection-host }

It is possible to connect to MongoDB host by using either DNS name of host or it's IP address.

It is also possible to connect to MongoDB shared cluster:

```python
mongo = MongoDB(
    host="master.host.or.ip",
    user="user",
    password="*****",
    database="target_database",
    spark=spark,
    extra={
        # read data from secondary cluster node, switch to primary if not available
        "readPreference": "secondaryPreferred",
    },
)
```

Supported `readPreference` values are described in [official documentation](https://www.mongodb.com/docs/manual/core/read-preference/).

### Connection port { #DBR-onetl-connection-db-connection-mongodb-prerequisites-connection-port }

Connection is usually performed to port `27017`. Port may differ for different MongoDB instances.
Please ask your MongoDB administrator to provide required information.

### Required grants { #DBR-onetl-connection-db-connection-mongodb-prerequisites-required-grants }

Ask your MongoDB cluster administrator to set following grants for a user, used for creating a connection:

=== "Read + Write"

    ```js
    // allow writing data to specific database
    db.grantRolesToUser("username", [{db: "somedb", role: "readWrite"}])
    ```

=== "Read only"

    ```js
    // allow reading data from specific database
    db.grantRolesToUser("username", [{db: "somedb", role: "read"}])
    ```

See:

- [db.grantRolesToUser documentation](https://www.mongodb.com/docs/manual/reference/method/db.grantRolesToUser)
- [MongoDB builtin roles](https://www.mongodb.com/docs/manual/reference/built-in-roles)
