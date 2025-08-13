# Устранение неполадок Spark S3 { #spark-s3-troubleshooting }

!!! note

    Общее руководство: [Устранение неполадок][troubleshooting].

Подробнее:

- [Руководство по устранению неполадок Hadoop AWS](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/troubleshooting_s3a.html)
- [Руководство по производительности Hadoop AWS](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/performance.html)
- [Интеграция Spark с облачными инфраструктурами](https://spark.apache.org/docs/latest/cloud-integration.html)

## `SparkS3.check()` и другие методы зависают

### Подробности

S3 может долго не отвечать на попытки подключения, если она находится под большой нагрузкой.
Чтобы справиться с этим, библиотека Hadoop AWS имеет механизм повторных попыток. По умолчанию он повторяет 7 раз с интервалом 500 мс.

Hadoop AWS основан на библиотеке AWS SDK, которая также имеет механизм повторных попыток. Этот механизм не отключен, потому что он обрабатывает другие ошибки, чем Hadoop AWS, поэтому они дополняют друг друга. Количество попыток по умолчанию в AWS SDK составляет 20 с минимальным интервалом 5 с, который экспоненциально увеличивается с каждой неудачной попыткой.

Это не проблема, если источник S3 вообще недоступен, например, не удается разрешить имя хоста или порт не открыт.
Эти ошибки не восстанавливаются, и механизм повторных попыток не активируется.

Но такие ошибки, как проблемы с SSL, считаются восстанавливаемыми, и это вызывает повторную попытку с увеличением интервала.
Таким образом, пользователь ждет [почти 15 минут](https://issues.apache.org/jira/browse/HADOOP-18839) только для того, чтобы получить сообщение об исключении.

### Как определить причину

#### Сделайте ведение журнала более подробным

Измените уровень журнала сеанса Spark на [DEBUG][troubleshooting-spark], чтобы распечатать результат каждой попытки.
Полученные журналы будут выглядеть так

??? note "Посмотреть лог"

    ```text

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
    ```

#### Измените количество повторных попыток

Вы также можете изменить количество повторных попыток, выполняемых обеими библиотеками, используя параметр `extra`:

    ```python
    spark_s3 = SparkS3(
        ...,
        extra={
            "attempts.maximum": 1,
            "retry.limit": 1,
        },
    )
    ```

Таким образом, доступ к S3 завершится почти сразу, если возникнет какая-либо ошибка.

### Наиболее распространенные ошибки

#### Нет доступа к сети

    ```text
    Caused by: java.net.ConnectException: Connection refused
    ```

В основном вызвано:

- Попытка доступа к номеру порта, который не прослушивается сервером S3
- Вы пытаетесь получить доступ к хосту, который недоступен из вашей сети (например, работает за каким-то прокси или VPN)
- Существуют некоторые ограничения брандмауэра для доступа к определенному хосту или порту

#### Использование протокола HTTPS для порта HTTP

    ```text
    Caused by: javax.net.ssl.SSLException: Unsupported or unrecognized SSL message
    ```

По умолчанию SparkS3 использует протокол HTTPS для подключения.
Если вы измените номер порта, это не приведет к изменению протокола:

    ```python
    spark_s3 = SparkS3(host="s3provider.com", port=8080, ...)
    ```

Вы должны явно передать протокол:

    ```python
    spark_s3 = SparkS3(host="s3provider.com", port=8080, protocol="http", ...)
    ```

#### SSL-сертификат является самоподписанным

    ```text
    sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target
    ```

Чтобы подключиться к порту HTTPS с самоподписанным сертификатом, вы должны
[добавить цепочку сертификатов в Java TrustedStore](https://stackoverflow.com/questions/373295/digital-certificate-how-to-import-cer-file-in-to-truststore-file-using).

Другой вариант - отключить проверку SSL:

    ```python
    spark_s3 = SparkS3(
        ...,
        extra={
            "connection.ssl.enabled": False,
        },
    )
    ```

Но это **НЕ** рекомендуется.

#### Доступ к S3 без поддержки стиля доступа на основе домена { #s3-0 }

    ```text
    Caused by: java.net.UnknownHostException: my-bucket.s3provider.com
    ```

Чтобы использовать стиль доступа на основе пути, используйте опцию ниже:

    ```python
    spark_s3 = SparkS3(
        host="s3provider.com",
        bucket="my-bucket",
        ...,
        extra={
            "path.style.access": True,
        },
    )
    ```

## Медленная или нестабильная запись в S3 { #s3-1 }

Hadoop AWS позволяет использовать различные стратегии записи для различных реализаций S3, в зависимости
от списка поддерживаемых сервером функций.

Эти стратегии называются [коммиттерами](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/committers.html).
Существуют [различные типы коммиттеров](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/committers.html#Switching_to_an_S3A_Committer):

- `file` (по умолчанию)
- `directory`
- `partitioned`
- `magic`

### `file` коммиттер

Этот коммиттер довольно медленный и нестабильный, поэтому его не рекомендуется использовать:

    ```text
    WARN AbstractS3ACommitterFactory: Using standard FileOutputCommitter to commit work. This is slow and potentially unsafe.
    ```

Это вызвано тем, что он создает файлы во временном каталоге в удаленной файловой системе, и после того, как все они будут успешно записаны,
они перемещаются в целевой каталог в той же удаленной файловой системе.

Это не проблема для HDFS, который поддерживает операции перемещения файлов, а также поддерживает переименование каталога
как атомарную операцию со сложностью по времени `O(1)`.

Но S3 поддерживает только копирование файлов, поэтому перемещение выполняется путем копирования + удаления.
Также он не поддерживает атомарную операцию переименования каталога. Вместо этого переименование файлов с одинаковым префиксом имеет временную сложность `O(n)`.

### `directory` и `partitioned` коммиттеры

Это [промежуточные коммиттеры](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/committer_architecture.html),
что означает, что они создают временные каталоги в локальной файловой системе, и после того, как все файлы будут успешно записаны,
они будут загружены в S3. Локальные файловые системы поддерживают перемещение файлов и переименование каталогов,
поэтому у этих коммиттеров нет проблем, которые есть у коммиттера `file`.

Но им обоим требуется свободное место в локальной файловой системе, и это может быть проблемой, если пользователю необходимо записать большой объем данных.
Также это может быть проблемой для контейнерной среды, такой как Kubernetes, где ресурсы должны быть выделены до запуска контейнера.

### `magic` коммиттер

Этот коммиттер использует функцию многокомпонентной загрузки API S3, позволяя создавать несколько файлов
и после того, как все они будут успешно записаны, завершить транзакцию. До завершения транзакции
файлы не будут доступны другим клиентам.

Поскольку он не требует ни операций перемещения файлов, ни атомарного переименования каталога,
процесс загрузки выполняется наиболее эффективным способом, который поддерживает S3.
Это [резко увеличивает производительность записи](https://spot.io/blog/improve-apache-spark-performance-with-the-s3-magic-committer/).

Чтобы использовать этот коммиттер, установите [следующие свойства](https://github.com/apache/spark/pull/32518) при создании сеанса Spark.

=== "S3 - ваша основная распределенная файловая система (Spark on Kubernetes)"

    ```python 
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
    ```

=== "HDFS - ваша основная распределенная файловая система (Spark on Hadoop)"

    ```python 
        # https://community.cloudera.com/t5/Support-Questions/spark-sql-sources-partitionOverwriteMode-dynamic-quot-not/m-p/343483/highlight/true
        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
            .config("spark.hadoop.fs.s3a.committer.name", "magic")
            .getOrCreate()
        )
    ```

!!! warning

    `magic` коммиттер требует, чтобы реализация S3 имела строгую согласованность - API загрузки файлов возвращает ответ только в том случае, если он был записан на достаточном количестве узлов кластера, и любая ошибка узла кластера не приводит к отсутствию или повреждению файлов.

    Некоторые реализации S3 имеют строгую согласованность (например, [AWS S3](https://aws.amazon.com/ru/blogs/aws/amazon-s3-update-strong-read-after-write-consistency/) и [MinIO](https://blog.min.io/migrating-hdfs-to-object-storage/)), некоторые - нет. Пожалуйста, свяжитесь с вашим поставщиком S3, чтобы получить информацию о согласованности реализации S3.

!!! warning

    `magic` коммиттер не поддерживает `if_exists="replace_overlapping_partitions"`.
    Либо используйте другое значение `if_exists`, либо используйте `partitioned` коммиттер.

### Смотрите также

- [directory.marker.retention="keep"](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/directory_markers.html)

## Медленное чтение из S3  { #s3-2 }

Пожалуйста, прочитайте следующую документацию:

- [prefetch.enabled](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/prefetching.html)
- [experimental.input.fadvise](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/performance.html#Improving_data_input_performance_through_fadvise)
- [Настройки Parquet и ORC I/O](https://spark.apache.org/docs/latest/cloud-integration.html#parquet-io-settings)

Если вы читаете данные из строковых форматов, таких как [CSV][csv-file-format], предпочтительнее
[experimental.input.fadvise="sequential" с увеличенным readahead.range](https://issues.apache.org/jira/browse/HADOOP-17789?focusedCommentId=17383559#comment-17383559).

Но для других форматов файлов, особенно с использованием сжатия, предпочтительнее
[experimental.input.fadvise="normal"](https://issues.apache.org/jira/browse/HADOOP-17789?focusedCommentId=17383743#comment-17383743)
