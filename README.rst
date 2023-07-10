.. _readme:

onETL
=====

|Repo Status| |PyPI| |PyPI License| |PyPI Python Version|
|Documentation| |Build Status| |Coverage|

.. |Repo Status| image:: https://www.repostatus.org/badges/latest/active.svg
    :target: https://github.com/MobileTeleSystems/onetl
.. |PyPI| image:: https://img.shields.io/pypi/v/onetl
    :target: https://pypi.org/project/onetl/
.. |PyPI License| image:: https://img.shields.io/pypi/l/onetl.svg
    :target: https://github.com/MobileTeleSystems/onetl/blob/develop/LICENSE.txt
.. |PyPI Python Version| image:: https://img.shields.io/pypi/pyversions/onetl.svg
    :target: https://badge.fury.io/py/onetl
.. |Documentation| image:: https://readthedocs.org/projects/onetl/badge/?version=stable
    :target: https://onetl.readthedocs.io/
.. |Build Status| image:: https://github.com/MobileTeleSystems/onetl/workflows/Tests/badge.svg
    :target: https://github.com/MobileTeleSystems/onetl/actions
.. |Coverage| image:: https://codecov.io/gh/MobileTeleSystems/onetl/branch/develop/graph/badge.svg?token=RIO8URKNZJ
    :target: https://codecov.io/gh/MobileTeleSystems/onetl

|Logo|

.. |Logo| image:: docs/static/logo_wide.svg
    :alt: onETL logo
    :target: https://github.com/MobileTeleSystems/onetl

What is onETL?
--------------

Python ETL/ELT library powered by `Apache Spark <https://spark.apache.org/>`_ & other open-source tools.

Goals
-----

* Provide unified classes to extract data from (**E**) & load data to (**L**) various stores.
* Provides `Spark DataFrame API <https://spark.apache.org/docs/3.2.0/api/python/reference/api/pyspark.sql.DataFrame.html>`_ for performing transformations (**T**) in terms of *ETL*.
* Provide direct assess to database, allowing to execute SQL queries, as well as DDL, DML, and call functions/procedures. This can be used for building up *ELT* pipelines.
* Support different `read strategies <https://onetl.readthedocs.io/en/stable/strategy/index.html>`_ for incremental and batch data fetching.
* Provide `hooks <https://onetl.readthedocs.io/en/stable/hooks/index.html>`_ & `plugins <https://onetl.readthedocs.io/en/stable/plugins.html>`_ mechanism for altering behavior of internal classes.

Non-goals
---------

* onETL is not a Spark replacement. It just provides additional functionality that Spark does not have, and simplifies UX for end users.
* onETL is not a framework, as it does not have requirements to project structure, naming, the way of running ETL/ELT processes, configuration, etc. All of that should be implemented in some other tool.
* onETL is deliberately developed without any integration with scheduling software like Apache Airflow. All integrations should be implemented as separated tools.

Requirements
------------
* **Python 3.7 - 3.11**
* PySpark 2.3.x - 3.4.x (depends on used connector)
* Java 8+ (required by Spark, see below)
* Kerberos libs & GCC (required by ``Hive`` and ``HDFS`` connectors)

Supported storages
------------------

+------------+------------+----------------------------------------------------------------------------------------------------------+
| Type       | Storage    | Powered by                                                                                               |
+============+============+==========================================================================================================+
| Database   | Clickhouse | Apache Spark `JDBC Data Source <https://spark.apache.org/docs/3.4.1/sql-data-sources-jdbc.html>`_        |
+            +------------+                                                                                                          +
|            | MSSQL      |                                                                                                          |
+            +------------+                                                                                                          +
|            | MySQL      |                                                                                                          |
+            +------------+                                                                                                          +
|            | Postgres   |                                                                                                          |
+            +------------+                                                                                                          +
|            | Oracle     |                                                                                                          |
+            +------------+                                                                                                          +
|            | Teradata   |                                                                                                          |
+            +------------+----------------------------------------------------------------------------------------------------------+
|            | Hive       | Apache Spark `Hive integration <https://spark.apache.org/docs/3.4.1/sql-data-sources-hive-tables.html>`_ |
+            +------------+----------------------------------------------------------------------------------------------------------+
|            | Greenplum  | Pivotal `Greenplum Spark connector <https://network.tanzu.vmware.com/products/vmware-tanzu-greenplum>`_  |
+            +------------+----------------------------------------------------------------------------------------------------------+
|            | MongoDB    | `MongoDB Spark connector <https://www.mongodb.com/docs/spark-connector/current>`_                        |
+------------+------------+----------------------------------------------------------------------------------------------------------+
| File       | HDFS       | `HDFS Python client <https://pypi.org/project/hdfs/>`_                                                   |
+            +------------+----------------------------------------------------------------------------------------------------------+
|            | S3         | `minio-py client <https://pypi.org/project/minio/>`_                                                     |
+            +------------+----------------------------------------------------------------------------------------------------------+
|            | SFTP       | `Paramiko library <https://pypi.org/project/paramiko/>`_                                                 |
+            +------------+----------------------------------------------------------------------------------------------------------+
|            | FTP        | `FTPUtil library <https://pypi.org/project/ftputil/>`_                                                   |
+            +------------+                                                                                                          +
|            | FTPS       |                                                                                                          |
+            +------------+----------------------------------------------------------------------------------------------------------+
|            | WebDAV     | `WebdavClient3 library <https://pypi.org/project/webdavclient3/>`_                                       |
+------------+------------+----------------------------------------------------------------------------------------------------------+


.. documentation

Documentation
-------------

See https://onetl.readthedocs.io/

.. install

How to install
---------------

.. _minimal-install:

Minimal installation
~~~~~~~~~~~~~~~~~~~~

Base ``onetl`` package contains:

* ``DBReader``, ``DBWriter`` and related classes
* ``FileDownloader``, ``FileUploader``, ``FileMover`` and related classes, like file filters & limits
* Read Strategies & HWM classes
* Plugins support

It can be installed via:

.. code:: bash

    pip install onetl

.. warning::

    This method does NOT include any connections.

    This method is recommended for use in third-party libraries which require for ``onetl`` to be installed,
    but do not use its connection classes.

.. _spark-install:

With DB connections
~~~~~~~~~~~~~~~~~~~

All DB connection classes (``Clickhouse``, ``Greenplum``, ``Hive`` and others) requires PySpark to be installed.

Firstly, you should install JDK. The exact installation instruction depends on your OS, here are some examples:

.. code:: bash

    yum install java-1.8.0-openjdk-devel  # CentOS 7 + Spark 2
    dnf install java-11-openjdk-devel  # CentOS 8 + Spark 3
    apt-get install openjdk-11-jdk  # Debian-based + Spark 3

.. _spark-compatibility-matrix:

Compatibility matrix
^^^^^^^^^^^^^^^^^^^^

+--------------------------------------------------------------+-------------+-------------+-------+
| Spark                                                        | Python      | Java        | Scala |
+==============================================================+=============+=============+=======+
| `2.3.x <https://spark.apache.org/docs/2.3.0/#downloading>`_  | 3.7 only    | 8 only      | 2.11  |
+--------------------------------------------------------------+-------------+-------------+-------+
| `2.4.x <https://spark.apache.org/docs/2.4.8/#downloading>`_  | 3.7 only    | 8 only      | 2.11  |
+--------------------------------------------------------------+-------------+-------------+-------+
| `3.2.x <https://spark.apache.org/docs/3.2.3/#downloading>`_  | 3.7 - 3.10  | 8u201 - 11  | 2.12  |
+--------------------------------------------------------------+-------------+-------------+-------+
| `3.3.x <https://spark.apache.org/docs/3.3.2/#downloading>`_  | 3.7 - 3.10  | 8u201 - 17  | 2.12  |
+--------------------------------------------------------------+-------------+-------------+-------+
| `3.4.x <https://spark.apache.org/docs/3.4.1/#downloading>`_  | 3.7 - 3.11  | 8u362 - 17  | 2.12  |
+--------------------------------------------------------------+-------------+-------------+-------+

Then you should install PySpark via passing ``spark`` to ``extras``:

.. code:: bash

    pip install onetl[spark]  # install latest PySpark

or install PySpark explicitly:

.. code:: bash

    pip install onetl pyspark==3.4.1  # install a specific PySpark version

or inject PySpark to ``sys.path`` in some other way BEFORE creating a class instance.
**Otherwise class import will fail.**


.. _files-install:

With file connections
~~~~~~~~~~~~~~~~~~~~~

All file connection classes (``FTP``,  ``SFTP``, ``HDFS`` and so on) requires specific Python clients to be installed.

Each client can be installed explicitly by passing connector name (in lowercase) to ``extras``:

.. code:: bash

    pip install onetl[ftp]  # specific connector
    pip install onetl[ftp,ftps,sftp,hdfs,s3,webdav]  # multiple connectors

To install all file connectors at once you can pass ``files`` to ``extras``:

.. code:: bash

    pip install onetl[files]

**Otherwise class import will fail.**


.. _kerberos-install:

With Kerberos support
~~~~~~~~~~~~~~~~~~~~~

Most of Hadoop instances set up with Kerberos support,
so some connections require additional setup to work properly.

* ``HDFS``
  Uses `requests-kerberos <https://pypi.org/project/requests-kerberos/>`_ and
  `GSSApi <https://pypi.org/project/gssapi/>`_ for authentication in WebHDFS.
  It also uses ``kinit`` executable to generate Kerberos ticket.

* ``Hive``
  Requires Kerberos ticket to exist before creating Spark session.

So you need to install OS packages with:

* ``krb5`` libs
* Headers for ``krb5``
* ``gcc`` or other compiler for C sources

The exact installation instruction depends on your OS, here are some examples:

.. code:: bash

    dnf install krb5-devel gcc  # CentOS, OracleLinux
    apt install libkrb5-dev gcc  # Debian-based

Also you should pass ``kerberos`` to ``extras`` to install required Python packages:

.. code:: bash

    pip install onetl[kerberos]


.. _full-install:

Full bundle
~~~~~~~~~~~

To install all connectors and dependencies, you can pass ``all`` into ``extras``:

.. code:: bash

    pip install onetl[all]

    # this is just the same as
    pip install onetl[spark,files,kerberos]

.. warning::

    This method consumes a lot of disk space, and requires for Java & Kerberos libraries to be installed into your OS.

.. quick-start

Quick start
------------

MSSQL → Hive
~~~~~~~~~~~~

Read data from MSSQL, transform & write to Hive.

.. code:: python

    # Import pyspark to initialize the SparkSession
    from pyspark.sql import SparkSession

    # import function to setup onETL logging
    from onetl.log import setup_logging

    # Import required connections
    from onetl.connection import MSSQL, Hive

    # Import onETL classes to read & write data
    from onetl.db import DBReader, DBWriter

    # change logging level to INFO, and set up default logging format and handler
    setup_logging()

    # Initiate new SparkSession
    spark = (
        SparkSession.builder.appName("spark_app_onetl_demo")
        .config("spark.jars.packages", MSSQL.package)
        .enableHiveSupport()
        .getOrCreate()
    )

    # Initiate MSSQL connection and check it
    mssql = MSSQL(
        host="mssqldb.demo.com",
        user="onetl",
        password="onetl",
        database="Telecom",
        spark=spark,
        extra={"ApplicationIntent": "ReadOnly"},
    ).check()

    # >>> INFO:|MSSQL| Connection is available.

    # Initiate reader
    reader = DBReader(
        connection=mssql,
        source="dbo.demo_table",
        columns=["on", "etl"],
        # Set some MSSQL read options:
        options=MSSQL.ReadOptions(fetchsize=10000),
    )

    # Read data to DataFrame
    df = reader.run()

    # Apply any PySpark transformations
    from pyspark.sql.functions import lit

    df_to_write = df.withColumn("engine", lit("onetl"))


    # Initiate Hive connection
    hive = Hive(cluster="rnd-dwh", spark=spark)

    # Initiate writer
    writer = DBWriter(
        connection=hive,
        target="dl_sb.demo_table",
        # Set some Hive write options:
        options=Hive.WriteOptions(mode="overwrite"),
    )

    # Write data from DataFrame to Hive
    writer.run(df_to_write)

    # Success!

SFTP → HDFS
~~~~~~~~~~~

Download files from SFTP & upload them to HDFS.

.. code:: python

    # import function to setup onETL logging
    from onetl.log import setup_logging

    # Import required connections
    from onetl.connection import SFTP, HDFS

    # Import onETL classes to download & upload files
    from onetl.file import FileDownloader, FileUploader

    # import filter & limit classes
    from onetl.file.filter import Glob, ExcludeDir
    from onetl.file.limit import MaxFilesCount

    # change logging level to INFO, and set up default logging format and handler
    setup_logging()

    # Initiate SFTP connection and check it
    sftp = SFTP(
        host="sftp.test.com",
        user="onetl",
        password="onetl",
    ).check()

    # >>> INFO:|SFTP| Connection is available.

    # Initiate downloader
    downloader = FileDownloader(
        connection=sftp,
        source_path="/remote/tests/Report",  # path on SFTP
        local_path="/local/onetl/Report",  # local fs path
        filters=[
            Glob("*.csv"),  # download only files matching the glob
            ExcludeDir(
                "/remote/tests/Report/exclude_dir/"
            ),  # exclude files from this directory
        ],
        limits=[
            MaxFilesCount(1000),  # download max 1000 files per run
        ],
        options=FileDownloader.Options(
            delete_source=True,  # delete files from SFTP after successful download
            mode="error",  # mark file as failed if it already exist in local_path
        ),
    )

    # Download files to local filesystem
    download_result = downloader.run()

    # Method run returns a DownloadResult object,
    # which contains collection of downloaded files, divided to 4 categories
    download_result

    #  DownloadResult(
    #      successful=[
    #          LocalPath('/local/onetl/Report/file_1.json'),
    #          LocalPath('/local/onetl/Report/file_2.json'),
    #      ],
    #      failed=[FailedRemoteFile('/remote/onetl/Report/file_3.json')],
    #      ignored=[RemoteFile('/remote/onetl/Report/file_4.json')],
    #      missing=[],
    #  )

    # Raise exception if there are failed files, or there were no files in the remote filesystem
    download_result.raise_if_failed() or download_result.raise_if_empty()

    # Do any kind of magic with files: rename files, remove header for csv files, ...
    renamed_files = my_rename_function(download_result.success)

    # function removed "_" from file names
    # [
    #    LocalPath('/home/onetl/Report/file1.json'),
    #    LocalPath('/home/onetl/Report/file2.json'),
    # ]

    # Initiate HDFS connection
    hdfs = HDFS(
        host="my.name.node",
        user="onetl",
        password="onetl",  # or keytab
    )

    # Initiate uploader
    uploader = FileUploader(
        connection=hdfs,
        target_path="/user/onetl/Report/",  # hdfs path
    )

    # Upload files from local fs to HDFS
    upload_result = uploader.run(renamed_files)

    # Method run returns a UploadResult object,
    # which contains collection of uploaded files, divided to 4 categories
    upload_result

    #  UploadResult(
    #      successful=[RemoteFile('/user/onetl/Report/file1.json')],
    #      failed=[FailedRemoteFile('/local/onetl/Report/file2.json')],
    #      ignored=[],
    #      missing=[],
    #  )

    # Raise exception if there are failed files, or there were no files in the local filesystem, or some input file is missing
    upload_result.raise_if_failed() or upload_result.raise_if_empty() or upload_result.raise_if_missing()
