Quick start
===========

MSSQL → Hive
------------

`Read data from MSSQL, transform & write to Hive.`

.. code:: python

    # Import mtspark to initialize the SparkSession
    from mtspark import get_spark

    # Import required connections
    from onetl.connection import MSSQL, Hive

    # Import onETL classes to read & write data
    from onetl.core import DBReader, DBWriter

    # Initiate new SparkSession
    spark = get_spark({"appName": "spark_app_onetl_demo"})

    # Initiate MSSQL connection
    mssql = MSSQL(
        host="mssqldb.demo.com",
        user="onetl",
        password="onetl",
        database="Telecom",
        spark=spark,
        extra={"ApplicationIntent": "ReadOnly"},
    )

    # Сheck database availability
    mssql.check()

    # >>> INFO:|MSSQL| Connection is available.

    # Initiate reader
    reader = DBReader(
        connection=mssql,
        table="dbo.demo_table",
        columns=["on", "etl"],
        # Set some MSSQL read options:
        options=MSSQL.Options(fetchsize=10000),
    )

    # Read data to DataFrame
    df = reader.run()

    # Apply any PySpark transformations
    from pyspark.sql.functions import lit

    df_to_write = df.withColumn("engine", lit("onetl"))


    # Initiate Hive connection
    hive = Hive(spark=spark)

    # Initiate writer
    writer = DBWriter(
        connection=hive,
        table="dl_sb.demo_table",
        # Set some Hive write options:
        options=Hive.Options(mode="overwrite"),
    )

    # Write data from DataFrame to Hive
    writer.run(df_to_write)

    # Success!

SFTP → HDFS
-----------

`Download files from FTP & upload them to HDFS.`

.. code:: python

    # Import required connections
    from onetl.connection import SFTP, HDFS

    # Import onETL classes to download & upload files
    from onetl.core import FileDownloader, FileUploader

    # Initiate SFTP connection
    sftp = SFTP(
        host="sftp.test.com",
        user="onetl",
        password="onetl",
    )

    # Сheck server availability
    sftp.check()

    # >>> INFO:|SFTP| Connection is available.

    # Initiate downloader
    downloader = FileDownloader(
        connection=sftp,
        source_path="/home/tests/Report",  # sftp_path
        local_path="/home/onetl/Report",  # local fs path
        source_exclude_dirs=["/home/tests/Report/exclude_dir/"],
        source_file_pattern="*.json",
    )


    # Download files to local filesystem
    downloaded_files = downloader.run()

    # Method run returns a list of downloaded files, i.e. list of full path for each downloaded file:
    downloaded_files

    # >>> [PosixPath('/home/onetl/Report/file_1.json'), PosixPath('/home/onetl/Report/file_2.json')]

    # Do any kind of magic with files: rename files, remove header for csv files, ...
    renamed_downloaded_files = my_rename_function(downloaded_files)

    # Initiate HDFS connection
    hdfs = HDFS(
        host="my-nn-001.msk.ru",
        user="onetl",
        password="onetl",  # or keytab
    )

    # Initiate uploader
    uploader = FileUploader(
        connection=hdfs,
        target_path="/user/onetl/Report/",  # hdfs path
    )

    # Upload files from local fs to HDFS
    uploaded_files = uploader.run(renamed_downloaded_files)

    uploaded_files  # return list of uploaded files:
    # >>> [PosixPath('/user/onetl/Report/rename_file_1.json'), PosixPath('/user/onetl/Report/rename_file_2.json')]
