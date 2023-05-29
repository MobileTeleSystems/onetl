Quick start
===========

MSSQL → Hive
------------

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
-----------

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
