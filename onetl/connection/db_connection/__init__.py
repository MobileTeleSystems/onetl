#  Copyright 2022 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from onetl.connection.db_connection.clickhouse import Clickhouse
from onetl.connection.db_connection.db_connection import DBConnection
from onetl.connection.db_connection.greenplum import Greenplum
from onetl.connection.db_connection.hive import Hive
from onetl.connection.db_connection.mssql import MSSQL
from onetl.connection.db_connection.mysql import MySQL
from onetl.connection.db_connection.oracle import Oracle
from onetl.connection.db_connection.postgres import Postgres
from onetl.connection.db_connection.teradata import Teradata
