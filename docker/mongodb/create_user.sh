#!/bin/bash

$(which mongosh || which mongo) \
    "${MONGO_INITDB_ROOT_DB}" \
    --host localhost \
    --port 27017 \
    -u "${MONGO_INITDB_ROOT_USERNAME}" \
    -p "${MONGO_INITDB_ROOT_PASSWORD}" \
    --eval "db.createUser({user: 'onetl', pwd: '123UsedForTestOnly@!', roles:[{role:'dbOwner', db: 'onetl'}]});"
