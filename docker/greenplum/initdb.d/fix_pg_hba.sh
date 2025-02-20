#!/bin/bash

# wat!?
sed "/host all  all/d" -i /data/master/gpsne-1/pg_hba.conf
echo "host all  all 0.0.0.0/0 md5" >> /data/master/gpsne-1/pg_hba.conf
echo "host all  all ::0/0 md5" >> /data/master/gpsne-1/pg_hba.conf

psql -d postgres -c "SELECT pg_reload_conf();"
