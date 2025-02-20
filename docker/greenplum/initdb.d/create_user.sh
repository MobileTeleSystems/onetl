#!/bin/bash

psql -d postgres -c "CREATE DATABASE $POSTGRESQL_DATABASE WITH owner = gpadmin;"

psql -d onetl <<SQL
    CREATE USER $POSTGRESQL_USERNAME WITH PASSWORD '$POSTGRESQL_PASSWORD';

-- allow creating schemas, tables and so on
GRANT CREATE ON DATABASE onetl TO $POSTGRESQL_USERNAME;

-- required by connector
GRANT SELECT ON information_schema.tables TO $POSTGRESQL_USERNAME;
GRANT SELECT ON pg_attribute TO $POSTGRESQL_USERNAME;
GRANT SELECT ON pg_class TO $POSTGRESQL_USERNAME;
GRANT SELECT ON pg_namespace TO $POSTGRESQL_USERNAME;
GRANT SELECT ON pg_settings TO $POSTGRESQL_USERNAME;
GRANT SELECT ON pg_stats TO $POSTGRESQL_USERNAME;
GRANT SELECT ON gp_distributed_xacts TO $POSTGRESQL_USERNAME;
GRANT SELECT ON gp_segment_configuration TO $POSTGRESQL_USERNAME;

-- allow creating external tables
ALTER USER $POSTGRESQL_USERNAME CREATEEXTTABLE(type = 'readable', protocol = 'gpfdist') CREATEEXTTABLE(type = 'writable', protocol = 'gpfdist');
SQL
