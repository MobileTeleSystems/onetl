# Logging

<a id="logging"></a>

Logging is quite important to understand what’s going on under the hood of onETL.

Default logging level for Python interpreters is `WARNING`, but most of onETL logs are in `INFO` level, so users usually don’t see much.

To change logging level, there is a function [`setup_logging`](#setup_logging) which should be called at the top of the script:

```python
from onetl.log import setup_logging
from other.lib import some, more, imports

setup_logging()

# rest of code
...
```

This changes both log level and log formatting to something like this:

## See logs

```text
2024-04-12 10:12:10,834 [INFO    ] MainThread: |onETL| Using IncrementalStrategy as a strategy
2024-04-12 10:12:10,835 [INFO    ] MainThread: =================================== DBReader.run() starts ===================================
2024-04-12 10:12:10,835 [INFO    ] MainThread: |DBReader| Getting Spark type for HWM expression: 'updated_at'
2024-04-12 10:12:10,836 [INFO    ] MainThread: |MSSQL| Fetching schema of table 'source_schema.table' ...
2024-04-12 10:12:11,636 [INFO    ] MainThread: |MSSQL| Schema fetched.
2024-04-12 10:12:11,642 [INFO    ] MainThread: |DBReader| Got Spark field: StructField('updated_at', TimestampType(), True)
2024-04-12 10:12:11,642 [INFO    ] MainThread: |DBReader| Detected HWM type: 'ColumnDateTimeHWM'
2024-04-12 10:12:11,643 [INFO    ] MainThread: |IncrementalStrategy| Fetching HWM from HorizonHWMStore:
2024-04-12 10:12:11,643 [INFO    ] MainThread:         name = 'updated_at#source_schema.table@mssql:/mssql.host:1433/somedb'
2024-04-12 10:12:12,181 [INFO    ] MainThread: |IncrementalStrategy| Fetched HWM:
2024-04-12 10:12:12,182 [INFO    ] MainThread:         hwm = ColumnDateTimeHWM(
2024-04-12 10:12:12,182 [INFO    ] MainThread:             name = 'updated_at#source_schema.table@mssql:/mssql.host:1433/somedb',
2024-04-12 10:12:12,182 [INFO    ] MainThread:             entity = 'source_schema.table',
2024-04-12 10:12:12,182 [INFO    ] MainThread:             expression = 'updated_at',
2024-04-12 10:12:12,184 [INFO    ] MainThread:             value = datetime.datetime(2024, 4, 11, 18, 10, 2, 120000),
2024-04-12 10:12:12,184 [INFO    ] MainThread:         )
2024-04-12 10:12:12,184 [INFO    ] MainThread: |MSSQL| -> |Spark| Reading DataFrame from source using parameters:
2024-04-12 10:12:12,185 [INFO    ] MainThread:         source = 'source_schema.table'
2024-04-12 10:12:12,185 [INFO    ] MainThread:         columns = [
2024-04-12 10:12:12,185 [INFO    ] MainThread:             'id',
2024-04-12 10:12:12,186 [INFO    ] MainThread:             'new_value',
2024-04-12 10:12:12,186 [INFO    ] MainThread:             'old_value',
2024-04-12 10:12:12,186 [INFO    ] MainThread:             'updated_at',
2024-04-12 10:12:12,186 [INFO    ] MainThread:         ]
2024-04-12 10:12:12,187 [INFO    ] MainThread:         where = "field = 'some'"
2024-04-12 10:12:12,187 [INFO    ] MainThread:         hwm = AutoDetectHWM(
2024-04-12 10:12:12,187 [INFO    ] MainThread:             name = 'updated_at#source_schema.table@mssql:/mssql.host:1433/somedb',
2024-04-12 10:12:12,187 [INFO    ] MainThread:             entity = 'source_schema.table',
2024-04-12 10:12:12,187 [INFO    ] MainThread:             expression = 'updated_at',
2024-04-12 10:12:12,188 [INFO    ] MainThread:         )
2024-04-12 10:12:12,188 [INFO    ] MainThread:         options = {
2024-04-12 10:12:12,188 [INFO    ] MainThread:             'fetchsize': 100000,
2024-04-12 10:12:12,188 [INFO    ] MainThread:             'numPartitions': 1,
2024-04-12 10:12:12,189 [INFO    ] MainThread:             'partitioningMode': 'range',
2024-04-12 10:12:12,189 [INFO    ] MainThread:         }
2024-04-12 10:12:12,189 [INFO    ] MainThread: |MSSQL| Checking connection availability...
2024-04-12 10:12:12,189 [INFO    ] MainThread: |MSSQL| Using connection parameters:
2024-04-12 10:12:12,190 [INFO    ] MainThread:         user = 'db_user'
2024-04-12 10:12:12,190 [INFO    ] MainThread:         password = SecretStr('**********')
2024-04-12 10:12:12,190 [INFO    ] MainThread:         host = 'mssql.host'
2024-04-12 10:12:12,190 [INFO    ] MainThread:         port = 1433
2024-04-12 10:12:12,191 [INFO    ] MainThread:         database = 'somedb'
2024-04-12 10:12:12,191 [INFO    ] MainThread:         extra = {'applicationIntent': 'ReadOnly', 'trustServerCertificate': 'true'}
2024-04-12 10:12:12,191 [INFO    ] MainThread:         jdbc_url = 'jdbc:sqlserver:/mssql.host:1433'
2024-04-12 10:12:12,579 [INFO    ] MainThread: |MSSQL| Connection is available.
2024-04-12 10:12:12,581 [INFO    ] MainThread: |MSSQL| Executing SQL query (on driver):
2024-04-12 10:12:12,581 [INFO    ] MainThread:         SELECT
2024-04-12 10:12:12,581 [INFO    ] MainThread:                MIN(updated_at) AS "min",
2024-04-12 10:12:12,582 [INFO    ] MainThread:                MAX(updated_at) AS "max"
2024-04-12 10:12:12,582 [INFO    ] MainThread:         FROM
2024-04-12 10:12:12,582 [INFO    ] MainThread:                source_schema.table
2024-04-12 10:12:12,582 [INFO    ] MainThread:         WHERE
2024-04-12 10:12:12,582 [INFO    ] MainThread:                (field = 'some')
2024-04-12 10:12:12,583 [INFO    ] MainThread:           AND
2024-04-12 10:12:12,583 [INFO    ] MainThread:                (updated_at >= CAST('2024-04-11T18:10:02.120000' AS datetime2))
2024-04-12 10:16:22,537 [INFO    ] MainThread: |MSSQL| Received values:
2024-04-12 10:16:22,538 [INFO    ] MainThread:         MIN(updated_at) = datetime.datetime(2024, 4, 11, 21, 10, 7, 397000)
2024-04-12 10:16:22,538 [INFO    ] MainThread:         MAX(updated_at) = datetime.datetime(2024, 4, 12, 13, 12, 2, 123000)
2024-04-12 10:16:22,540 [INFO    ] MainThread: |MSSQL| Executing SQL query (on executor):
2024-04-12 10:16:22,540 [INFO    ] MainThread:         SELECT
2024-04-12 10:16:22,540 [INFO    ] MainThread:                id,
2024-04-12 10:16:22,541 [INFO    ] MainThread:                new_value,
2024-04-12 10:16:22,541 [INFO    ] MainThread:                old_value,
2024-04-12 10:16:22,541 [INFO    ] MainThread:                updated_at
2024-04-12 10:16:22,541 [INFO    ] MainThread:         FROM
2024-04-12 10:16:22,541 [INFO    ] MainThread:                source_schema.table
2024-04-12 10:16:22,542 [INFO    ] MainThread:         WHERE
2024-04-12 10:16:22,542 [INFO    ] MainThread:                (field = 'some')
2024-04-12 10:16:22,542 [INFO    ] MainThread:           AND
2024-04-12 10:16:22,542 [INFO    ] MainThread:                (updated_at >  CAST('2024-04-11T18:10:02.120000' AS datetime2))
2024-04-12 10:16:22,542 [INFO    ] MainThread:           AND
2024-04-12 10:16:22,542 [INFO    ] MainThread:                (updated_at <= CAST('2024-04-12T13:12:02.123000' AS datetime2))
2024-04-12 10:16:22,892 [INFO    ] MainThread: |Spark| DataFrame successfully created from SQL statement
2024-04-12 10:16:22,892 [INFO    ] MainThread: ------------------------------------ DBReader.run() ends ------------------------------------
2024-04-12 10:40:42,409 [INFO    ] MainThread: =================================== DBWriter.run() starts ===================================
2024-04-12 10:40:42,409 [INFO    ] MainThread: |Spark| -> |Hive| Writing DataFrame to target using parameters:
2024-04-12 10:40:42,410 [INFO    ] MainThread:         target = 'target_source_schema.table'
2024-04-12 10:40:42,410 [INFO    ] MainThread:         options = {
2024-04-12 10:40:42,410 [INFO    ] MainThread:             'mode': 'append',
2024-04-12 10:40:42,410 [INFO    ] MainThread:             'format': 'orc',
2024-04-12 10:40:42,410 [INFO    ] MainThread:             'partitionBy': 'part_dt',
2024-04-12 10:40:42,410 [INFO    ] MainThread:         }
2024-04-12 10:40:42,411 [INFO    ] MainThread:         df_schema:
2024-04-12 10:40:42,412 [INFO    ] MainThread:             root
2024-04-12 10:40:42,412 [INFO    ] MainThread:              |-- id: integer (nullable = true)
2024-04-12 10:40:42,413 [INFO    ] MainThread:              |-- new_value: string (nullable = true)
2024-04-12 10:40:42,413 [INFO    ] MainThread:              |-- old_value: string (nullable = true)
2024-04-12 10:40:42,413 [INFO    ] MainThread:              |-- updated_at: timestamp (nullable = true)
2024-04-12 10:40:42,413 [INFO    ] MainThread:              |-- part_dt: date (nullable = true)
2024-04-12 10:40:42,414 [INFO    ] MainThread:
2024-04-12 10:40:42,421 [INFO    ] MainThread: |Hive| Checking connection availability...
2024-04-12 10:40:42,421 [INFO    ] MainThread: |Hive| Using connection parameters:
2024-04-12 10:40:42,421 [INFO    ] MainThread:         cluster = 'dwh'
2024-04-12 10:40:42,475 [INFO    ] MainThread: |Hive| Connection is available.
2024-04-12 10:40:42,476 [INFO    ] MainThread: |Hive| Fetching schema of table 'target_source_schema.table' ...
2024-04-12 10:40:43,518 [INFO    ] MainThread: |Hive| Schema fetched.
2024-04-12 10:40:43,521 [INFO    ] MainThread: |Hive| Table 'target_source_schema.table' already exists
2024-04-12 10:40:43,521 [WARNING ] MainThread: |Hive| User-specified options {'partitionBy': 'part_dt'} are ignored while inserting into existing table. Using only table parameters from Hive metastore
2024-04-12 10:40:43,782 [INFO    ] MainThread: |Hive| Inserting data into existing table 'target_source_schema.table' ...
2024-04-12 11:06:07,396 [INFO    ] MainThread: |Hive| Data is successfully inserted into table 'target_source_schema.table'.
2024-04-12 11:06:07,397 [INFO    ] MainThread: ------------------------------------ DBWriter.run() ends ------------------------------------
2024-04-12 11:06:07,397 [INFO    ] MainThread: |onETL| Exiting IncrementalStrategy
2024-04-12 11:06:07,397 [INFO    ] MainThread: |IncrementalStrategy| Saving HWM to 'HorizonHWMStore':
2024-04-12 11:06:07,397 [INFO    ] MainThread:         hwm = ColumnDateTimeHWM(
2024-04-12 11:06:07,397 [INFO    ] MainThread:             name = 'updated_at#source_schema.table@mssql:/mssql.host:1433/somedb',
2024-04-12 11:06:07,397 [INFO    ] MainThread:             entity = 'source_source_schema.table',
2024-04-12 11:06:07,397 [INFO    ] MainThread:             expression = 'updated_at',
2024-04-12 11:06:07,397 [INFO    ] MainThread:             value = datetime.datetime(2024, 4, 12, 13, 12, 2, 123000),
2024-04-12 11:06:07,397 [INFO    ] MainThread:         )
2024-04-12 11:06:07,495 [INFO    ] MainThread: |IncrementalStrategy| HWM has been saved
```

Each step performed by onETL is extensively logged, which should help with debugging.

You can make logs even more verbose by changing level to `DEBUG`:

```python
from onetl.log import setup_logging

setup_logging(level="DEBUG", enable_clients=True)

# rest of code
...
```

This also changes log level for all underlying Python libraries, e.g. showing each HTTP request being made, and so on.

<a id="setup_logging"></a>
**onetl.log.setup_logging**(level: int | str = 20, enable_clients: bool = False) → None

Set up onETL logging.

What this function does:

* Adds stderr logging handler
* Changes root logger format to `2023-05-31 11:22:33.456 [INFO] MainThread: message`
* Changes root logger level to `level`
* Changes onETL logger level to `level`
* Sets up logging level of underlying client modules

<!-- TODO: admonition doesn't works -->

```{admonition} note
Should be used only in IDEs (like Jupyter notebooks or PyCharm),
or scripts (ETL pipelines).
```

<!-- TODO: versionchanged doesn't works -->
<!-- TODO: nested blocks doesn't works -->

````{versionchanged}
Changed in version 0.5.0: Renamed `setup_notebook_logging` → `setup_logging`

* **Parameters:**
  **level**
  Log level for onETL module

  **enable_clients**
  If `True`, enable logging of underlying client modules.
    Otherwise, set client modules log level to `DISABLED`.

```{admonition} note
For `level="DEBUG"` it is recommended to use `enable_clients=True`
```

```{versionadded}
Added in version 0.9.0.
```
````
<!-- !! processed by numpydoc !! -->

**onetl.log.setup_clients_logging**(level: int | str = 9999) → None

Set logging of underlying client modules used by onETL.

Affected modules:

* `ftputil`
* `hdfs`
* `minio`
* `paramiko`
* `py4j`
* `pyspark`
* `webdav3`

```{admonition} note
Can be used in applications, but it is recommended to set up these loggers
according to your framework documentation.
```

````{versionchanged}
Changed in version 0.9.0: Renamed `disable_clients_logging` → `setup_clients_logging`

* **Parameters:**
  **level**
  Log level for client modules
    <br/>
```{admonition} note
For `py4j`, logging level with maximum verbosity is `INFO` because `DEBUG` logs are
totally unreadable.
```
<br/>
```{versionadded}
Added in version 0.9.0.
```
````
<!-- !! processed by numpydoc !! -->

**onetl.log.set_default_logging_format()** → None

Sets default logging format to preferred by onETL.

Example log message: `2023-05-31 11:22:33.456 [INFO] MainThread: message`

```{admonition} note
Should be used only in IDEs (like Jupyter notebooks or PyCharm),
or scripts (ETL pipelines).
```

```{admonition} warning
Should **NOT** be used in applications, you should set up logging settings manually,
according to your framework documentation.
```
<!-- !! processed by numpydoc !! -->
