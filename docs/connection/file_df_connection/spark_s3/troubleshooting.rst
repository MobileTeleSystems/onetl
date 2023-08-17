.. _spark-s3-troubleshooting:

Spark S3 Troubleshooting
========================

More details:

* `Hadoop AWS Troubleshooting Guide <https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/troubleshooting_s3a.html>`_
* `Hadoop AWS Performance Guide <https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/performance.html>`_
* `Spark integration with Cloud Infrastructures <https://spark.apache.org/docs/latest/cloud-integration.html>`_

``SparkS3.check()`` and other methods hang
------------------------------------------

Details
~~~~~~~

S3 may not respond for connection attempts for a long time if it's under heavy load.
To handle this, Hadoop AWS library has retry mechanism. By default it retries 7 times with 500ms interval.

Hadoop AWS is based on AWS SDK library, which also has retry mechanism. This mechanism is not disabled because it handles different
errors than Hadoop AWS, so they complement each other. Default number of attempts in AWS SDK is 20 with minimal 5s interval,
which is exponentially increasing with each failed attempt.

It is not a problem if S3 source is not accessible at all, like hostname cannot be resolved, or port is not opened.
These errors are not recoverable, and retry mechanism is not activated.

But errors like SSL issues, are considered recoverable, and this causing retry of retry over increasing interval.
So user is waiting for `almost 15 minutes <https://issues.apache.org/jira/browse/HADOOP-18839>`_ just to get exception message.

How to determine reason
~~~~~~~~~~~~~~~~~~~~~~~

Make logging more verbose
^^^^^^^^^^^^^^^^^^^^^^^^^

Change Spark session log level to ``DEBUG`` to print result of each attempt:

.. code:: python

    spark.sparkContext.setLogLevel("debug")

Resulting logs will look like this

.. dropdown:: See log

    .. code:: txt

        23/08/03 11:25:10 DEBUG S3AFileSystem: Using S3ABlockOutputStream with buffer = disk; block=67108864; queue limit=4
        23/08/03 11:25:10 DEBUG S3Guard: Metastore option source [core-default.xml]
        23/08/03 11:25:10 DEBUG S3Guard: Using NullMetadataStore metadata store for s3a filesystem
        23/08/03 11:25:10 DEBUG S3AFileSystem: S3Guard is disabled on this bucket: test-bucket
        23/08/03 11:25:10 DEBUG DirectoryPolicyImpl: Directory markers will be deleted
        23/08/03 11:25:10 DEBUG S3AFileSystem: Directory marker retention policy is DirectoryMarkerRetention{policy='delete'}
        23/08/03 11:25:10 DEBUG S3AUtils: Value of fs.s3a.multipart.purge.age is 86400
        23/08/03 11:25:10 DEBUG S3AUtils: Value of fs.s3a.bulk.delete.page.size is 250
        23/08/03 11:25:10 DEBUG FileSystem: Creating FS s3a://test-bucket/fake: duration 0:01.029s
        23/08/03 11:25:10 DEBUG IOStatisticsStoreImpl: Incrementing counter op_is_directory by 1 with final value 1
        23/08/03 11:25:10 DEBUG S3AFileSystem: Getting path status for s3a://test-bucket/fake  (fake); needEmptyDirectory=false
        23/08/03 11:25:10 DEBUG S3AFileSystem: S3GetFileStatus s3a://test-bucket/fake
        23/08/03 11:25:10 DEBUG S3AFileSystem: LIST List test-bucket:/fake/ delimiter=/ keys=2 requester pays=false
        23/08/03 11:25:10 DEBUG S3AFileSystem: Starting: LIST
        23/08/03 11:25:10 DEBUG IOStatisticsStoreImpl: Incrementing counter object_list_request by 1 with final value 1
        23/08/03 11:25:10 DEBUG AWSCredentialProviderList: Using credentials from SimpleAWSCredentialsProvider
        23/08/03 11:25:10 DEBUG request: Sending Request: GET https://test-bucket.localhost:9000 / Parameters: ({"list-type":["2"],"delimiter":["/"],"max-keys":["2"],"prefix":["fake/"],"fetch-owner":["false"]}Headers: (amz-sdk-invocation-id: e6d62603-96e4-a80f-10a1-816e0822bc71, Content-Type: application/octet-stream, User-Agent: Hadoop 3.3.4, aws-sdk-java/1.12.262 Linux/6.4.7-1-MANJARO OpenJDK_64-Bit_Server_VM/25.292-b10 java/1.8.0_292 scala/2.12.17 vendor/AdoptOpenJDK cfg/retry-mode/legacy, )
        23/08/03 11:25:10 DEBUG AWS4Signer: AWS4 Canonical Request: '"GET
        /
        delimiter=%2F&fetch-owner=false&list-type=2&max-keys=2&prefix=fake%2F
        amz-sdk-invocation-id:e6d62603-96e4-a80f-10a1-816e0822bc71
        amz-sdk-request:attempt=1;max=21
        amz-sdk-retry:0/0/500
        content-type:application/octet-stream
        host:test-bucket.localhost:9000
        user-agent:Hadoop 3.3.4, aws-sdk-java/1.12.262 Linux/6.4.7-1-MANJARO OpenJDK_64-Bit_Server_VM/25.292-b10 java/1.8.0_292 scala/2.12.17 vendor/AdoptOpenJDK cfg/retry-mode/legacy
        x-amz-content-sha256:UNSIGNED-PAYLOAD
        x-amz-date:20230803T112510Z

        amz-sdk-invocation-id;amz-sdk-request;amz-sdk-retry;content-type;host;user-agent;x-amz-content-sha256;x-amz-date
        UNSIGNED-PAYLOAD"
        23/08/03 11:25:10 DEBUG AWS4Signer: AWS4 String to Sign: '"AWS4-HMAC-SHA256
        20230803T112510Z
        20230803/us-east-1/s3/aws4_request
        31a317bb7f6d97248dd0cf03429d701cbb3e29ce889cfbb98ba7a34c57a3bfba"
        23/08/03 11:25:10 DEBUG AWS4Signer: Generating a new signing key as the signing key not available in the cache for the date 1691020800000
        23/08/03 11:25:10 DEBUG RequestAddCookies: CookieSpec selected: default
        23/08/03 11:25:10 DEBUG RequestAuthCache: Auth cache not set in the context
        23/08/03 11:25:10 DEBUG PoolingHttpClientConnectionManager: Connection request: [route: {s}->https://test-bucket.localhost:9000][total available: 0; route allocated: 0 of 96; total allocated: 0 of 96]
        23/08/03 11:25:10 DEBUG PoolingHttpClientConnectionManager: Connection leased: [id: 0][route: {s}->https://test-bucket.localhost:9000][total available: 0; route allocated: 1 of 96; total allocated: 1 of 96]
        23/08/03 11:25:10 DEBUG MainClientExec: Opening connection {s}->https://test-bucket.localhost:9000
        23/08/03 11:25:10 DEBUG DefaultHttpClientConnectionOperator: Connecting to test-bucket.localhost/127.0.0.1:9000
        23/08/03 11:25:10 DEBUG SSLConnectionSocketFactory: Connecting socket to test-bucket.localhost/127.0.0.1:9000 with timeout 5000
        23/08/03 11:25:10 DEBUG SSLConnectionSocketFactory: Enabled protocols: [TLSv1.2]
        23/08/03 11:25:10 DEBUG SSLConnectionSocketFactory: Enabled cipher suites:[TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384, TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384, TLS_RSA_WITH_AES_256_CBC_SHA256, TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA384, TLS_ECDH_RSA_WITH_AES_256_CBC_SHA384, TLS_DHE_RSA_WITH_AES_256_CBC_SHA256, TLS_DHE_DSS_WITH_AES_256_CBC_SHA256, TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA, TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA, TLS_RSA_WITH_AES_256_CBC_SHA, TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA, TLS_ECDH_RSA_WITH_AES_256_CBC_SHA, TLS_DHE_RSA_WITH_AES_256_CBC_SHA, TLS_DHE_DSS_WITH_AES_256_CBC_SHA, TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256, TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256, TLS_RSA_WITH_AES_128_CBC_SHA256, TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA256, TLS_ECDH_RSA_WITH_AES_128_CBC_SHA256, TLS_DHE_RSA_WITH_AES_128_CBC_SHA256, TLS_DHE_DSS_WITH_AES_128_CBC_SHA256, TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA, TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_128_CBC_SHA, TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA, TLS_ECDH_RSA_WITH_AES_128_CBC_SHA, TLS_DHE_RSA_WITH_AES_128_CBC_SHA, TLS_DHE_DSS_WITH_AES_128_CBC_SHA, TLS_EMPTY_RENEGOTIATION_INFO_SCSV]
        23/08/03 11:25:10 DEBUG SSLConnectionSocketFactory: Starting handshake
        23/08/03 11:25:10 DEBUG ClientConnectionManagerFactory:
        java.lang.reflect.InvocationTargetException
                at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
                at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
                at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
                at java.lang.reflect.Method.invoke(Method.java:498)
                at com.amazonaws.http.conn.ClientConnectionManagerFactory$Handler.invoke(ClientConnectionManagerFactory.java:76)
                at com.amazonaws.http.conn.$Proxy32.connect(Unknown Source)
                at com.amazonaws.thirdparty.apache.http.impl.execchain.MainClientExec.establishRoute(MainClientExec.java:393)
                at com.amazonaws.thirdparty.apache.http.impl.execchain.MainClientExec.execute(MainClientExec.java:236)
                at com.amazonaws.thirdparty.apache.http.impl.execchain.ProtocolExec.execute(ProtocolExec.java:186)
                at com.amazonaws.thirdparty.apache.http.impl.client.InternalHttpClient.doExecute(InternalHttpClient.java:185)
                at com.amazonaws.thirdparty.apache.http.impl.client.CloseableHttpClient.execute(CloseableHttpClient.java:83)
                at com.amazonaws.thirdparty.apache.http.impl.client.CloseableHttpClient.execute(CloseableHttpClient.java:56)
                at com.amazonaws.http.apache.client.impl.SdkHttpClient.execute(SdkHttpClient.java:72)
                at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeOneRequest(AmazonHttpClient.java:1346)
                at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeHelper(AmazonHttpClient.java:1157)
                at com.amazonaws.http.AmazonHttpClient$RequestExecutor.doExecute(AmazonHttpClient.java:814)
                at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeWithTimer(AmazonHttpClient.java:781)
                at com.amazonaws.http.AmazonHttpClient$RequestExecutor.execute(AmazonHttpClient.java:755)
                at com.amazonaws.http.AmazonHttpClient$RequestExecutor.access$500(AmazonHttpClient.java:715)
                at com.amazonaws.http.AmazonHttpClient$RequestExecutionBuilderImpl.execute(AmazonHttpClient.java:697)
                at com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:561)
                at com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:541)
                at com.amazonaws.services.s3.AmazonS3Client.invoke(AmazonS3Client.java:5456)
                at com.amazonaws.services.s3.AmazonS3Client.invoke(AmazonS3Client.java:5403)
                at com.amazonaws.services.s3.AmazonS3Client.invoke(AmazonS3Client.java:5397)
                at com.amazonaws.services.s3.AmazonS3Client.listObjectsV2(AmazonS3Client.java:971)
                at org.apache.hadoop.fs.s3a.S3AFileSystem.lambda$listObjects$11(S3AFileSystem.java:2595)
                at org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.lambda$trackDurationOfOperation$5(IOStatisticsBinding.java:499)
                at org.apache.hadoop.fs.s3a.Invoker.retryUntranslated(Invoker.java:414)
                at org.apache.hadoop.fs.s3a.Invoker.retryUntranslated(Invoker.java:377)
                at org.apache.hadoop.fs.s3a.S3AFileSystem.listObjects(S3AFileSystem.java:2586)
                at org.apache.hadoop.fs.s3a.S3AFileSystem.s3GetFileStatus(S3AFileSystem.java:3832)
                at org.apache.hadoop.fs.s3a.S3AFileSystem.innerGetFileStatus(S3AFileSystem.java:3688)
                at org.apache.hadoop.fs.s3a.S3AFileSystem.lambda$isDirectory$35(S3AFileSystem.java:4724)
                at org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.lambda$trackDurationOfOperation$5(IOStatisticsBinding.java:499)
                at org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDuration(IOStatisticsBinding.java:444)
                at org.apache.hadoop.fs.s3a.S3AFileSystem.trackDurationAndSpan(S3AFileSystem.java:2337)
                at org.apache.hadoop.fs.s3a.S3AFileSystem.trackDurationAndSpan(S3AFileSystem.java:2356)
                at org.apache.hadoop.fs.s3a.S3AFileSystem.isDirectory(S3AFileSystem.java:4722)
                at org.apache.spark.sql.execution.streaming.FileStreamSink$.hasMetadata(FileStreamSink.scala:54)
                at org.apache.spark.sql.execution.datasources.DataSource.resolveRelation(DataSource.scala:366)
                at org.apache.spark.sql.DataFrameReader.loadV1Source(DataFrameReader.scala:229)
                at org.apache.spark.sql.DataFrameReader.$anonfun$load$2(DataFrameReader.scala:211)
                at scala.Option.getOrElse(Option.scala:189)
                at org.apache.spark.sql.DataFrameReader.load(DataFrameReader.scala:211)
                at org.apache.spark.sql.DataFrameReader.load(DataFrameReader.scala:186)
                at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
                at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
                at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
                at java.lang.reflect.Method.invoke(Method.java:498)
                at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
                at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374)
                at py4j.Gateway.invoke(Gateway.java:282)
                at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
                at py4j.commands.CallCommand.execute(CallCommand.java:79)
                at py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:182)
                at py4j.ClientServerConnection.run(ClientServerConnection.java:106)
                at java.lang.Thread.run(Thread.java:748)
        Caused by: javax.net.ssl.SSLException: Unsupported or unrecognized SSL message
                at sun.security.ssl.SSLSocketInputRecord.handleUnknownRecord(SSLSocketInputRecord.java:448)
                at sun.security.ssl.SSLSocketInputRecord.decode(SSLSocketInputRecord.java:184)
                at sun.security.ssl.SSLTransport.decode(SSLTransport.java:109)
                at sun.security.ssl.SSLSocketImpl.decode(SSLSocketImpl.java:1383)
                at sun.security.ssl.SSLSocketImpl.readHandshakeRecord(SSLSocketImpl.java:1291)
                at sun.security.ssl.SSLSocketImpl.startHandshake(SSLSocketImpl.java:435)
                at com.amazonaws.thirdparty.apache.http.conn.ssl.SSLConnectionSocketFactory.createLayeredSocket(SSLConnectionSocketFactory.java:436)
                at com.amazonaws.thirdparty.apache.http.conn.ssl.SSLConnectionSocketFactory.connectSocket(SSLConnectionSocketFactory.java:384)
                at com.amazonaws.thirdparty.apache.http.impl.conn.DefaultHttpClientConnectionOperator.connect(DefaultHttpClientConnectionOperator.java:142)
                at com.amazonaws.thirdparty.apache.http.impl.conn.PoolingHttpClientConnectionManager.connect(PoolingHttpClientConnectionManager.java:376)
                ... 58 more
        23/08/03 11:25:10 DEBUG DefaultManagedHttpClientConnection: http-outgoing-0: Shutdown connection
        23/08/03 11:25:10 DEBUG MainClientExec: Connection discarded
        23/08/03 11:25:10 DEBUG PoolingHttpClientConnectionManager: Connection released: [id: 0][route: {s}->https://test-bucket.localhost:9000][total available: 0; route allocated: 0 of 96; total allocated: 0 of 96]
        23/08/03 11:25:10 DEBUG AmazonHttpClient: Unable to execute HTTP request: Unsupported or unrecognized SSL message Request will be retried.
        23/08/03 11:25:10 DEBUG request: Retrying Request: GET https://test-bucket.localhost:9000 / Parameters: ({"list-type":["2"],"delimiter":["/"],"max-keys":["2"],"prefix":["fake/"],"fetch-owner":["false"]}Headers: (amz-sdk-invocation-id: e6d62603-96e4-a80f-10a1-816e0822bc71, Content-Type: application/octet-stream, User-Agent: Hadoop 3.3.4, aws-sdk-java/1.12.262 Linux/6.4.7-1-MANJARO OpenJDK_64-Bit_Server_VM/25.292-b10 java/1.8.0_292 scala/2.12.17 vendor/AdoptOpenJDK cfg/retry-mode/legacy, )
        23/08/03 11:25:10 DEBUG AmazonHttpClient: Retriable error detected, will retry in 49ms, attempt number: 0

After getting all information you need, make logs less verbose:

.. code:: python

    spark.sparkContext.setLogLevel("info")

    # or
    spark.sparkContext.setLogLevel("warn")

Change number of retries
^^^^^^^^^^^^^^^^^^^^^^^^

You can also change number of retries performed by both libraries using ``extra`` parameter:

.. code:: python

    spark_s3 = SparkS3(
        ...,
        extra={
            "attempts.maximum": 1,
            "retry.limit": 1,
        },
    )

So accessing S3 will fail almost immediately if there is any error.

Most common mistakes
~~~~~~~~~~~~~~~~~~~~

No network access
^^^^^^^^^^^^^^^^^

.. code:: txt

    Caused by: java.net.ConnectException: Connection refused

Mostly caused by:

* Trying to access port number which S3 server does not listen
* You're trying to access host which is unreachable from your network (e.g. running behind some proxy or VPN)
* There are some firewall restrictions for accessing specific host or port

Using HTTPS protocol for HTTP port
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: txt

    Caused by: javax.net.ssl.SSLException: Unsupported or unrecognized SSL message

By default, SparkS3 uses HTTPS protocol for connection.
If you change port number, this does not lead to changing protocol:

.. code:: python

    spark_s3 = SparkS3(host="s3.domain.com", port=8080, ...)

You should pass protocol explicitly:

.. code:: python

    spark_s3 = SparkS3(host="s3.domain.com", port=8080, protocol="http", ...)

SSL certificate is self-signed
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: txt

    sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target

To connect to HTTPS port with self-signed certificate, you should
`add certificate chain to Java TrustedStore <https://stackoverflow.com/questions/373295/digital-certificate-how-to-import-cer-file-in-to-truststore-file-using>`_.

Another option is to disable SSL check:

.. code:: python

    spark_s3 = SparkS3(
        ...,
        extra={
            "connection.ssl.enabled": False,
        },
    )

But is is **NOT** recommended.

Accessing S3 without domain-style access style support
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: txt

    Caused by: java.net.UnknownHostException: my-bucket.s3.domain.com

By default, Hadoop AWS uses domain-style access ``my-bucket.domain.com`` instead of path-style access ``domain.com/my-bucket``,
because this is default option for AWS S3.

But some S3 implementations does not support domain-style access, e.g. MinIO by default allows only path-style access
(see `MINIO_DOMAIN <https://min.io/docs/minio/linux/reference/minio-server/minio-server.html#envvar.MINIO_DOMAIN>`_).

To use path-style access, use option below:

.. code:: python

    spark_s3 = SparkS3(
        host="s3.domain.com",
        bucket="my-bucket",
        ...,
        extra={
            "path.style.access": True,
        },
    )

Slow or unstable writing to S3
------------------------------

Hadoop AWS allows to use different writing strategies for different S3 implementations, depending
on list of supported features by server.

These strategies are called `committers <https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/committers.html>`_.
There are `different types of committers <https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/committers.html#Switching_to_an_S3A_Committer>`_:

* ``file`` (default)
* ``directory``
* ``partitioned``
* ``magic``

``file`` committer
~~~~~~~~~~~~~~~~~~

This committer is quite slow and unstable, so it is not recommended to use:

.. code:: txt

    WARN AbstractS3ACommitterFactory: Using standard FileOutputCommitter to commit work. This is slow and potentially unsafe.

This is caused by the fact it creates files in the temp directory on remote filesystem, and after all of them are written successfully,
they are moved to target directory on same remote filesystem.

This is not an issue for HDFS which does support file move operations and also support renaming directory
as atomic operation with ``O(1)`` time complexity.

But S3 does support only file copying, so moving is performed via copy + delete.
Also it does not support atomic directory rename operation. Instead, renaming files with the same prefix has time complexity ``O(n)``.

``directory`` and ``partitioned`` committers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These are `staging committers <https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/committer_architecture.html>`_,
meaning that they create temp directories on local filesystem, and after all files are written successfully,
they will be uploaded to S3. Local filesystems do support file moving and directory renaming,
so these committers does not have issues that ``file`` committer has.

But they both require free space on local filesystem, and this may be an issue if user need to write large amount of data.
Also this can be an issue for container environment, like Kubernetes, there resources should be allocated before starting a container.

``magic`` committer
~~~~~~~~~~~~~~~~~~~

This committer uses multipart upload feature of S3 API, allowing to create multiple files
and after all of them were written successfully finish the transaction. Before transaction is finished,
files will not be accessible by other clients.

Because it does not require neither file moving operations, nor directory atomic rename,
upload process is done in most efficient way S3 support.
This `drastically increases writing performance <https://spot.io/blog/improve-apache-spark-performance-with-the-s3-magic-committer/>`_.

To use this committer, set `following properties <https://github.com/apache/spark/pull/32518>`_ while creating Spark session.

.. tabs::

    .. code-tab:: py S3 your main distributed filesystem (Spark on Kubernetes)

        # https://issues.apache.org/jira/browse/SPARK-23977
        # https://spark.apache.org/docs/latest/cloud-integration.html#committing-work-into-cloud-storage-safely-and-fast
        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
            .config("spark.hadoop.fs.s3a.committer.name", "magic")
            .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
            .config("spark.sql.parquet.output.committer.class", "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")
            .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
            .getOrCreate()
        )

    .. code-tab:: py HDFS is your main distributed filesystem (Spark on Hadoop)

        # https://community.cloudera.com/t5/Support-Questions/spark-sql-sources-partitionOverwriteMode-dynamic-quot-not/m-p/343483/highlight/true
        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
            .config("spark.hadoop.fs.s3a.committer.name", "magic")
            .getOrCreate()
        )

.. warning::

    ``magic`` committer requires S3 implementation to have strong consistency - file upload API return response only
    if it was written on enough number of cluster nodes, and any cluster node error does not lead to missing or corrupting files.

    Some S3 implementations does have strong consistency
    (like `AWS S3 <https://aws.amazon.com/ru/blogs/aws/amazon-s3-update-strong-read-after-write-consistency/>`_ and
    `MinIO <https://blog.min.io/migrating-hdfs-to-object-storage/>`_), some not. Please contact your S3 provider
    to get information about S3 implementation consistency.

.. warning::

    ``magic`` committer does not support ``if_exists="replace_overlapping_partitions"``.
    Either use another ``if_exists`` value, or use ``partitioned`` committer.

See also
~~~~~~~~

* `directory.marker.retention="keep" <https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/directory_markers.html>`_

Slow reading from S3
--------------------

Please read following documentation:

* `prefetch.enabled <https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/prefetching.html>`_
* `experimental.input.fadvise <https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/performance.html#Improving_data_input_performance_through_fadvise>`_
* `Parquet and ORC I/O settings <https://spark.apache.org/docs/latest/cloud-integration.html#parquet-io-settings>`_

If you're reading data from row-based formats, like :ref:`csv-file-format`, prefer
`experimental.input.fadvise="sequential" with increased readahead.range <https://issues.apache.org/jira/browse/HADOOP-17789?focusedCommentId=17383559#comment-17383559>`_.

But for other file formats, especially using compression, prefer
`experimental.input.fadvise="normal" <https://issues.apache.org/jira/browse/HADOOP-17789?focusedCommentId=17383743#comment-17383743>`_
