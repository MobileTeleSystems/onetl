********
Concepts
********

Here you can find detailed documentation about each one of the onETL concepts and how to use them.

Connection
==========

Basics
------

onETL is used to pull and push data into other systems, and so it has a first-class ``Connection`` concept for storing credentials that are used to communicate with external systems.

A ``Connection`` is essentially a set of parameters - such as username, password, hostname.

To create a connection to a specific storage type, you must use a **unique class** that matches the storage type. The class name is the same as the storage type name (``Oracle``, ``MSSQL``, ``SFTP``, etc):


.. code:: python

    from onetl.connection import SFTP

    sftp = SFTP(
        host="sftp.test.com",
        user="onetl",
        password="onetl",
    )

All connection types are inherited from the parent class ``BaseConnection``.

Class diagram
-------------

.. image:: static/connections.svg

DBConnection
------------

Classes inherited from ``DBConnection`` could be used for accessing databases.

A ``DBConnection`` could be instantiated as follows:

.. code:: python

    from onetl.connection import MSSQL

    mssql = MSSQL(
        host="mssqldb.demo.com",
        user="onetl",
        password="onetl",
        database="Telecom",
        spark=spark,
        extra={"ApplicationIntent": "ReadOnly"},
    )

where  **spark** is the current SparkSession. ``onETL`` uses ``Spark`` under the hood to work with databases.

For a description of other parameters, see the documentation for the `available DBConnections <db_connection/clickhouse.html>`_.

FileConnection
--------------

Classes inherited from ``FileConnection`` could be used to access files stored on the different file systems/file servers

A ``FileConnection`` could be instantiated as follows:

.. code:: python

    from onetl.connection import SFTP

    sftp = SFTP(
        host="sftp.test.com",
        user="onetl",
        password="onetl",
    )

For a description of other parameters, see the documentation for the `available FileConnections <file_connection/ftp.html>`_.

Checking connection availability
--------------------------------

Once you have created the required connection, you can check the database/filesystem availability using the method ``check()``:

.. code:: python

    mssql.check()
    sftp.check()

It will raise an exception if database/filesystem cannot be accessed.

Extract/Load data
=================

Basics
------

As we said above, onETL is used to extract data from and load data into remote systems.

onETL provides several classes for this:

    * ``DBReader``
    * ``DBWriter``
    * ``FileDownloader``
    * ``FileUploader``

All of these classes have a method ``run()`` that starts extracting/loading the data:

.. code:: python

    from onetl.core import DBReader, DBWriter

    reader = DBReader(
        connection=mssql,
        table="dbo.demo_table",
        columns=["column_1", "column_2"],
    )

    # Extract data to df
    df = reader.run()

    writer = DBWriter(
        connection=hive,
        table="dl_sb.demo_table",
    )

    # Load df to hive table
    writer.run(df)

Extract data
------------

To extract data you can use classes:

+------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------+-------------------------------------------+------------------------------------------------------------------+
|                                                | Use case                                                                                                                                                                             | Connection              | ``run()`` gets                            | ``run()`` returns                                                |
+================================================+======================================================================================================================================================================================+=========================+===========================================+==================================================================+
| `DBReader <core/db_reader.html>`_              | Reading data from a database and saving it as a `Spark DataFrame <https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html#pyspark.sql.DataFrame>`_  | Any ``DBConnection``    | \-                                        | Spark DataFrame                                                  |
+------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------+-------------------------------------------+------------------------------------------------------------------+
| `FileDownloader <core/file_downloader.html>`_  | Download files from remote FS to local FS                                                                                                                                            | Any ``FileConnection``  | No input, or List[File path on remote FS] | :obj:`onetl.core.file_downloader.download_result.DownloadResult` |
+------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------+-------------------------------------------+------------------------------------------------------------------+

Load data
---------

To load data you can use classes:

+----------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+-------------------------------+------------------------------------------------------------+
|                                              | Use case                                                                                                                                                                | Connection               | ``run()`` gets                | ``run()`` returns                                          |
+==============================================+=========================================================================================================================================================================+==========================+===============================+============================================================+
| `DBWriter <core/db_writer.html>`_            | Writing data from a `Spark DataFrame <https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html#pyspark.sql.DataFrame>`_ to a database   | Any ``DBConnection``     | Spark DataFrame               | None                                                       |
+----------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+-------------------------------+------------------------------------------------------------+
| `FileUploader <core/file_downloader.html>`_  | Uploading files from a local FS to remote FS                                                                                                                            | Any ``FileConnection``   | List[File path on local FS]   | :obj:`onetl.core.file_uploader.upload_result.UploadResult` |
+----------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+-------------------------------+------------------------------------------------------------+

Options
-------

Extract and load classes have a ``options`` parameter, which has a special meaning:

    * all other parameters - *WHAT* we extract from / *WHERE* we load to
    * ``options`` parameter - *HOW* we extract/load data

.. code:: python

    reader = DBReader(
        # WHAT do we extract:
        connection=mssql,
        table="dbo.demo_table",  # some table from MSSQL
        columns=["column_1", "column_2"],  # but only specific set of columns
        where="column_2 > 1000",  # only rows matching the clause
        # HOW do we extract:
        options=MSSQL.ReadOptions(
            numPartitions=10,  # run in 10 parallel jobs
            partitionColumn="id",  # each job will get only part of data based on "id" column hash
            partitioningMode="hash",
            fetchsize=1000,  # each job will fetch data in loop of 1000 rows per iter
        ),
    )

    writer = DBWriter(
        # WHERE do we load - to some table in Hive
        connection=hive,
        table="dl_sb.demo_table",
        # HOW do we load - overwrite all the data in existing table
        options=Hive.WriteOptions(mode="overwrite_all"),
    )

    downloader = FileDownloader(
        # WHAT do we extract from
        connection=sftp,
        source_path="/source",  # files some path from SFTP
        filter=FileFilter(glob="*.csv"),  # only CSV files
        limit=FileLimit(count_limit=1000),  # 1000 files max
        # WHERE do we extract to - a specific path on local FS
        local_path="/some",
        # HOW do we extract
        options=FileDownloader.Options(
            delete_source=True,  # not only download files, but remove them from source
            mode="overwrite",  # overwrite existing files in the local_path
        ),
    )

    uploader = FileUploader(
        # WHAT do we load from - files from some local path
        local_path="/source",
        # WHERE do we load to
        connection=hdfs,
        target_path="/some",  # save to a specific remote path on HDFS
        # HOW do we load
        options=FileUploader.Options(
            delete_local=True,  # not only upload files, but remove them from local FS
            mode="append",  # overwrite existing files in the target_path
        ),
    )

More information about ``options`` could be found on `DB connection <db_connection/clickhouse.html>`_. and
:ref:`file-downloader` / :ref:`file-uploader` documentation

Read Strategies
---------------

onETL have several builtin strategies for reading data:

1. `Snapshot strategy <strategy/snapshot_strategy.html>`_ (default strategy)
2. `Incremental strategy <strategy/incremental_strategy.html>`_
3. `Snapshot batch strategy <strategy/snapshot_batch_strategy.html>`_
4. `Incremental batch strategy <strategy/incremental_batch_strategy.html>`_

For example, an incremental strategy allows you to get only new data from the table:

.. code:: python

    from onetl.strategy import IncrementalStrategy

    reader = DBReader(
        connection=mssql,
        table="dbo.demo_table",
        hwm_column="id",  # detect new data based on value of "id" column
    )

    # first run
    with IncrementalStrategy():
        df = reader.run()

    sleep(3600)

    # second run
    with IncrementalStrategy():
        # only rows, that appeared in the source since previous run
        df = reader.run()

or get only files which were not downloaded before:

.. code:: python

    from onetl.strategy import IncrementalStrategy

    downloader = FileDownloader(
        connection=sftp,
        source_path="/remote",
        local_path="/local",
        hwm_type="file_list",  # save all downloaded files to a list, and exclude files already present in this list
    )

    # first run
    with IncrementalStrategy():
        files = downloader.run()

    sleep(3600)

    # second run
    with IncrementalStrategy():
        # only files, that appeared in the source since previous run
        files = downloader.run()

Most of strategies are based on :ref:`hwm`, Please check each strategy documentation for more details


Why just not use Connection class for extract/load?
----------------------------------------------------

Connections are very simple, they have only a set of some basic operations,
like ``mkdir``, ``remove_file``, ``get_table_schema``, and so on.

High-level operations, like
    * :ref:`strategy` support
    * Handling metadata push/pull
    * Handling different options (``overwrite``, ``error``, ``ignore``) in case of file download/upload

is moved to a separate class which calls the connection object methods to perform some complex logic.
