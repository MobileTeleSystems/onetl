:

# Quick start

## MSSQL → Hive

Read data from MSSQL, transform & write to Hive.

```bash
# install onETL and PySpark
pip install onetl[spark]
```

```python
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
    extra={"applicationIntent": "ReadOnly"},
).check()

# >>> INFO:|MSSQL| Connection is available

# Initialize DBReader
reader = DBReader(
    connection=mssql,
    source="dbo.demo_table",
    columns=["on", "etl"],
    # Set some MSSQL read options:
    options=MSSQL.ReadOptions(fetchsize=10000),
)

# checks that there is data in the table, otherwise raises exception
reader.raise_if_no_data()

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

# Initialize DBWriter
db_writer = DBWriter(
    connection=hive,
    target="dl_sb.demo_table",
    # Set some Hive write options:
    options=Hive.WriteOptions(if_exists="replace_entire_table"),
)

# Write data from DataFrame to Hive
db_writer.run(df_to_write)

# Success!
```

## SFTP → HDFS

Download files from SFTP & upload them to HDFS.

```bash
# install onETL with SFTP and HDFS clients, and Kerberos support
pip install onetl[hdfs,sftp,kerberos]
```

```python
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
```

## S3 → Postgres

Read files directly from S3 path, convert them to dataframe, transform it and then write to a database.

```bash
# install onETL and PySpark
pip install onetl[spark]
```

```python
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
maven_packages = SparkS3.get_packages(spark_version="3.5.5") + Postgres.get_packages()
exclude_packages = SparkS3.get_exclude_packages()
spark = (
    SparkSession.builder.appName("spark_app_onetl_demo")
    .config("spark.jars.packages", ",".join(maven_packages))
    .config("spark.jars.excludes", ",".join(exclude_packages))
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

# Initialize DBWriter
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
```
