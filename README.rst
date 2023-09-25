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
* Provides `Spark DataFrame API <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html>`_ for performing transformations (**T**) in terms of *ETL*.
* Provide direct assess to database, allowing to execute SQL queries, as well as DDL, DML, and call functions/procedures. This can be used for building up *ELT* pipelines.
* Support different `read strategies <https://onetl.readthedocs.io/en/stable/strategy/index.html>`_ for incremental and batch data fetching.
* Provide `hooks <https://onetl.readthedocs.io/en/stable/hooks/index.html>`_ & `plugins <https://onetl.readthedocs.io/en/stable/plugins.html>`_ mechanism for altering behavior of internal classes.

Non-goals
---------

* onETL is not a Spark replacement. It just provides additional functionality that Spark does not have, and simplifies UX for end users.
* onETL is not a framework, as it does not have requirements to project structure, naming, the way of running ETL/ELT processes, configuration, etc. All of that should be implemented in some other tool.
* onETL is deliberately developed without any integration with scheduling software like Apache Airflow. All integrations should be implemented as separated tools.
* Only batch operations, no streaming. For streaming prefer `Apache Flink <https://flink.apache.org/>`_.

Requirements
------------
* **Python 3.7 - 3.11**
* PySpark 2.3.x - 3.4.x (depends on used connector)
* Java 8+ (required by Spark, see below)
* Kerberos libs & GCC (required by ``Hive``, ``HDFS`` and ``SparkHDFS`` connectors)

Supported storages
------------------

+--------------------+--------------+----------------------------------------------------------------------------------------------------------------------+
| Type               | Storage      | Powered by                                                                                                           |
+====================+==============+======================================================================================================================+
| Database           | Clickhouse   | Apache Spark `JDBC Data Source <https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html>`_                   |
+                    +--------------+                                                                                                                      +
|                    | MSSQL        |                                                                                                                      |
+                    +--------------+                                                                                                                      +
|                    | MySQL        |                                                                                                                      |
+                    +--------------+                                                                                                                      +
|                    | Postgres     |                                                                                                                      |
+                    +--------------+                                                                                                                      +
|                    | Oracle       |                                                                                                                      |
+                    +--------------+                                                                                                                      +
|                    | Teradata     |                                                                                                                      |
+                    +--------------+----------------------------------------------------------------------------------------------------------------------+
|                    | Hive         | Apache Spark `Hive integration <https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html>`_            |
+                    +--------------+----------------------------------------------------------------------------------------------------------------------+
|                    | Kafka        | Apache Spark `Kafka integration <https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html>`_ |
+                    +--------------+----------------------------------------------------------------------------------------------------------------------+
|                    | Greenplum    | Pivotal `Greenplum Spark connector <https://network.tanzu.vmware.com/products/vmware-tanzu-greenplum>`_              |
+                    +--------------+----------------------------------------------------------------------------------------------------------------------+
|                    | MongoDB      | `MongoDB Spark connector <https://www.mongodb.com/docs/spark-connector/current>`_                                    |
+--------------------+--------------+----------------------------------------------------------------------------------------------------------------------+
| File               | HDFS         | `HDFS Python client <https://pypi.org/project/hdfs/>`_                                                               |
+                    +--------------+----------------------------------------------------------------------------------------------------------------------+
|                    | S3           | `minio-py client <https://pypi.org/project/minio/>`_                                                                 |
+                    +--------------+----------------------------------------------------------------------------------------------------------------------+
|                    | SFTP         | `Paramiko library <https://pypi.org/project/paramiko/>`_                                                             |
+                    +--------------+----------------------------------------------------------------------------------------------------------------------+
|                    | FTP          | `FTPUtil library <https://pypi.org/project/ftputil/>`_                                                               |
+                    +--------------+                                                                                                                      +
|                    | FTPS         |                                                                                                                      |
+                    +--------------+----------------------------------------------------------------------------------------------------------------------+
|                    | WebDAV       | `WebdavClient3 library <https://pypi.org/project/webdavclient3/>`_                                                   |
+                    +--------------+----------------------------------------------------------------------------------------------------------------------+
|                    | Samba        | `pysmb library <https://pypi.org/project/pysmb/>`_                                                                   |
+--------------------+--------------+----------------------------------------------------------------------------------------------------------------------+
| Files as DataFrame | SparkLocalFS | Apache Spark `File Data Source <https://spark.apache.org/docs/3.4.1/sql-data-sources-generic-options.html>`_         |
|                    +--------------+                                                                                                                      +
|                    | SparkHDFS    |                                                                                                                      |
|                    +--------------+----------------------------------------------------------------------------------------------------------------------+
|                    | SparkS3      | `Hadoop AWS <https://hadoop.apache.org/docs/current3/hadoop-aws/tools/hadoop-aws/index.html>`_ library               |
+--------------------+--------------+----------------------------------------------------------------------------------------------------------------------+


.. documentation

Documentation
-------------

See https://onetl.readthedocs.io/

How to install
---------------

.. _install:

Minimal installation
~~~~~~~~~~~~~~~~~~~~

.. _minimal-install:

Base ``onetl`` package contains:

* ``DBReader``, ``DBWriter`` and related classes
* ``FileDownloader``, ``FileUploader``, ``FileMover`` and related classes, like file filters & limits
* ``FileDFReader``, ``FileDFWriter`` and related classes, like file formats
* Read Strategies & HWM classes
* Plugins support

It can be installed via:

.. code:: bash

    pip install onetl

.. warning::

    This method does NOT include any connections.

    This method is recommended for use in third-party libraries which require for ``onetl`` to be installed,
    but do not use its connection classes.

With DB and FileDF connections
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. _spark-install:

All DB connection classes (``Clickhouse``, ``Greenplum``, ``Hive`` and others)
and all FileDF connection classes (``SparkHDFS``, ``SparkLocalFS``, ``SparkS3``)
require Spark to be installed.

.. _java-install:

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
| `3.2.x <https://spark.apache.org/docs/3.2.4/#downloading>`_  | 3.7 - 3.10  | 8u201 - 11  | 2.12  |
+--------------------------------------------------------------+-------------+-------------+-------+
| `3.3.x <https://spark.apache.org/docs/3.3.3/#downloading>`_  | 3.7 - 3.10  | 8u201 - 17  | 2.12  |
+--------------------------------------------------------------+-------------+-------------+-------+
| `3.4.x <https://spark.apache.org/docs/3.4.1/#downloading>`_  | 3.7 - 3.11  | 8u362 - 20  | 2.12  |
+--------------------------------------------------------------+-------------+-------------+-------+

.. _pyspark-install:

Then you should install PySpark via passing ``spark`` to ``extras``:

.. code:: bash

    pip install onetl[spark]  # install latest PySpark

or install PySpark explicitly:

.. code:: bash

    pip install onetl pyspark==3.4.1  # install a specific PySpark version

or inject PySpark to ``sys.path`` in some other way BEFORE creating a class instance.
**Otherwise connection object cannot be created.**

With File connections
~~~~~~~~~~~~~~~~~~~~~

.. _files-install:

All File (but not *FileDF*) connection classes (``FTP``,  ``SFTP``, ``HDFS`` and so on) requires specific Python clients to be installed.

Each client can be installed explicitly by passing connector name (in lowercase) to ``extras``:

.. code:: bash

    pip install onetl[ftp]  # specific connector
    pip install onetl[ftp,ftps,sftp,hdfs,s3,webdav,samba]  # multiple connectors

To install all file connectors at once you can pass ``files`` to ``extras``:

.. code:: bash

    pip install onetl[files]

**Otherwise class import will fail.**

With Kerberos support
~~~~~~~~~~~~~~~~~~~~~

.. _kerberos-install:

Most of Hadoop instances set up with Kerberos support,
so some connections require additional setup to work properly.

* ``HDFS``
  Uses `requests-kerberos <https://pypi.org/project/requests-kerberos/>`_ and
  `GSSApi <https://pypi.org/project/gssapi/>`_ for authentication.
  It also uses ``kinit`` executable to generate Kerberos ticket.

* ``Hive`` and ``SparkHDFS``
  require Kerberos ticket to exist before creating Spark session.

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

Full bundle
~~~~~~~~~~~

.. _full-bundle:

To install all connectors and dependencies, you can pass ``all`` into ``extras``:

.. code:: bash

    pip install onetl[all]

    # this is just the same as
    pip install onetl[spark,files,kerberos]

.. warning::

    This method consumes a lot of disk space, and requires for Java & Kerberos libraries to be installed into your OS.

.. _quick-start:

Quick start
------------

MSSQL → Hive
~~~~~~~~~~~~

Read data from MSSQL, transform & write to Hive.

.. code:: bash

    # install onETL and PySpark
    pip install onetl[spark]

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

    # Initialize new SparkSession with MSSQL driver loaded
    maven_packages = MSSQL.get_packages()
    spark = (
        SparkSession.builder.appName("spark_app_onetl_demo")
        .config("spark.jars.packages", ",".join(maven_packages))
        .enableHiveSupport()  # for Hive
        .getOrCreate()
    )

    # Initialize MSSQL connection and check if database is accessible
    mssql = MSSQL(
        host="mssqldb.demo.com",
        user="onetl",
        password="onetl",
        database="Telecom",
        spark=spark,
        # These options are passed to MSSQL JDBC Driver:
        extra={"ApplicationIntent": "ReadOnly"},
    ).check()

    # >>> INFO:|MSSQL| Connection is available

    # Initialize DB reader
    reader = DBReader(
        connection=mssql,
        source="dbo.demo_table",
        columns=["on", "etl"],
        # Set some MSSQL read options:
        options=MSSQL.ReadOptions(fetchsize=10000),
    )

    # Read data to DataFrame
    df = reader.run()
    df.printSchema()
    # root
    #  |-- id: integer (nullable = true)
    #  |-- phone_number: string (nullable = true)
    #  |-- region: string (nullable = true)
    #  |-- birth_date: date (nullable = true)
    #  |-- registered_at: timestamp (nullable = true)
    #  |-- account_balance: double (nullable = true)

    # Apply any PySpark transformations
    from pyspark.sql.functions import lit

    df_to_write = df.withColumn("engine", lit("onetl"))
    df_to_write.printSchema()
    # root
    #  |-- id: integer (nullable = true)
    #  |-- phone_number: string (nullable = true)
    #  |-- region: string (nullable = true)
    #  |-- birth_date: date (nullable = true)
    #  |-- registered_at: timestamp (nullable = true)
    #  |-- account_balance: double (nullable = true)
    #  |-- engine: string (nullable = false)

    # Initialize Hive connection
    hive = Hive(cluster="rnd-dwh", spark=spark)

    # Initialize DB writer
    db_writer = DBWriter(
        connection=hive,
        target="dl_sb.demo_table",
        # Set some Hive write options:
        options=Hive.WriteOptions(if_exists="replace_entire_table"),
    )

    # Write data from DataFrame to Hive
    db_writer.run(df_to_write)

    # Success!

SFTP → HDFS
~~~~~~~~~~~

Download files from SFTP & upload them to HDFS.

.. code:: bash

    # install onETL with SFTP and HDFS clients, and Kerberos support
    pip install onetl[hdfs,sftp,kerberos]

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

    # Initialize SFTP connection and check it
    sftp = SFTP(
        host="sftp.test.com",
        user="someuser",
        password="somepassword",
    ).check()

    # >>> INFO:|SFTP| Connection is available

    # Initialize downloader
    file_downloader = FileDownloader(
        connection=sftp,
        source_path="/remote/tests/Report",  # path on SFTP
        local_path="/local/onetl/Report",  # local fs path
        filters=[
            # download only files matching the glob
            Glob("*.csv"),
            # exclude files from this directory
            ExcludeDir("/remote/tests/Report/exclude_dir/"),
        ],
        limits=[
            # download max 1000 files per run
            MaxFilesCount(1000),
        ],
        options=FileDownloader.Options(
            # delete files from SFTP after successful download
            delete_source=True,
            # mark file as failed if it already exist in local_path
            if_exists="error",
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

    # Initialize HDFS connection
    hdfs = HDFS(
        host="my.name.node",
        user="someuser",
        password="somepassword",  # or keytab
    )

    # Initialize uploader
    file_uploader = FileUploader(
        connection=hdfs,
        target_path="/user/onetl/Report/",  # hdfs path
    )

    # Upload files from local fs to HDFS
    upload_result = file_uploader.run(renamed_files)

    # Method run returns a UploadResult object,
    # which contains collection of uploaded files, divided to 4 categories
    upload_result

    #  UploadResult(
    #      successful=[RemoteFile('/user/onetl/Report/file1.json')],
    #      failed=[FailedLocalFile('/local/onetl/Report/file2.json')],
    #      ignored=[],
    #      missing=[],
    #  )

    # Raise exception if there are failed files, or there were no files in the local filesystem, or some input file is missing
    upload_result.raise_if_failed() or upload_result.raise_if_empty() or upload_result.raise_if_missing()

    # Success!


S3 → Postgres
~~~~~~~~~~~~~~~~

Read files directly from S3 path, convert them to dataframe, transform it and then write to a database.

.. code:: bash

    # install onETL and PySpark
    pip install onetl[spark]

.. code:: python

    # Import pyspark to initialize the SparkSession
    from pyspark.sql import SparkSession

    # import function to setup onETL logging
    from onetl.log import setup_logging

    # Import required connections
    from onetl.connection import Postgres, SparkS3

    # Import onETL classes to read files
    from onetl.file import FileDFReader
    from onetl.file.format import CSV

    # Import onETL classes to write data
    from onetl.db import DBWriter

    # change logging level to INFO, and set up default logging format and handler
    setup_logging()

    # Initialize new SparkSession with Hadoop AWS libraries and Postgres driver loaded
    maven_packages = SparkS3.get_packages(spark_version="3.4.1") + Postgres.get_packages()
    spark = (
        SparkSession.builder.appName("spark_app_onetl_demo")
        .config("spark.jars.packages", ",".join(maven_packages))
        .getOrCreate()
    )

    # Initialize S3 connection and check it
    spark_s3 = SparkS3(
        host="s3.test.com",
        protocol="https",
        bucket="my-bucket",
        access_key="somekey",
        secret_key="somesecret",
        # Access bucket as s3.test.com/my-bucket
        extra={"path.style.access": True},
        spark=spark,
    ).check()

    # >>> INFO:|SparkS3| Connection is available

    # Describe file format and parsing options
    csv = CSV(
        delimiter=";",
        header=True,
        encoding="utf-8",
    )

    # Describe DataFrame schema of files
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    df_schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("phone_number", StringType()),
            StructField("region", StringType()),
            StructField("birth_date", DateType()),
            StructField("registered_at", TimestampType()),
            StructField("account_balance", DoubleType()),
        ],
    )

    # Initialize file df reader
    reader = FileDFReader(
        connection=spark_s3,
        source_path="/remote/tests/Report",  # path on S3 there *.csv files are located
        format=csv,  # file format with specific parsing options
        df_schema=df_schema,  # columns & types
    )

    # Read files directly from S3 as Spark DataFrame
    df = reader.run()

    # Check that DataFrame schema is same as expected
    df.printSchema()
    # root
    #  |-- id: integer (nullable = true)
    #  |-- phone_number: string (nullable = true)
    #  |-- region: string (nullable = true)
    #  |-- birth_date: date (nullable = true)
    #  |-- registered_at: timestamp (nullable = true)
    #  |-- account_balance: double (nullable = true)

    # Apply any PySpark transformations
    from pyspark.sql.functions import lit

    df_to_write = df.withColumn("engine", lit("onetl"))
    df_to_write.printSchema()
    # root
    #  |-- id: integer (nullable = true)
    #  |-- phone_number: string (nullable = true)
    #  |-- region: string (nullable = true)
    #  |-- birth_date: date (nullable = true)
    #  |-- registered_at: timestamp (nullable = true)
    #  |-- account_balance: double (nullable = true)
    #  |-- engine: string (nullable = false)

    # Initialize Postgres connection
    postgres = Postgres(
        host="192.169.11.23",
        user="onetl",
        password="somepassword",
        database="mydb",
        spark=spark,
    )

    # Initialize DB writer
    db_writer = DBWriter(
        connection=postgres,
        # write to specific table
        target="public.my_table",
        # with some writing options
        options=Postgres.WriteOptions(if_exists="append"),
    )

    # Write DataFrame to Postgres table
    db_writer.run(df_to_write)

    # Success!
