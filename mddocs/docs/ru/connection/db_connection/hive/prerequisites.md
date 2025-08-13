# Предварительные требования { #hive-prerequisites }

!!! note

    Подключение к Hive в onETL на самом деле является SparkSession с доступом к [Hive Thrift Metastore](https://docs.cloudera.com/cdw-runtime/1.5.0/hive-hms-overview/topics/hive-hms-introduction.html) и HDFS/S3.
    Все перемещения данных осуществляются с использованием Spark. Hive Metastore используется только для хранения метаданных таблиц и разделов.

    Этот коннектор **НЕ** требует наличия Hive сервера. Он также **НЕ** использует Hive JDBC коннектор.

## Совместимость версий

- Версии Hive Metastore:
  - Официально заявленные: 0.12 - 3.1.3 (может потребоваться явное добавление соответствующего .jar файла)
  - Фактически протестированные: 1.2.100, 2.3.10, 3.1.3
- Версии Spark: 2.3.x - 3.5.x
- Версии Java: 8 - 20

См. [официальную документацию](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html).

## Установка PySpark

Для использования коннектора Hive у вас должен быть установлен PySpark (или добавлен в `sys.path`) **ДО** создания экземпляра коннектора.

Подробнее см. [инструкцию по установке][install-spark].

## Подключение к Hive Metastore

!!! note

    Если вы используете управляемый кластер Hadoop, пропустите этот шаг, так как все конфигурации Spark уже должны присутствовать на хосте.

Создайте файл `$SPARK_CONF_DIR/hive-site.xml` с URL-адресом Hive Metastore:

    ```xml
        <?xml version="1.0"?>
        <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
        <configuration>
            <property>
                <name>hive.metastore.uris</name>
                <value>thrift://metastore.host.name:9083</value>
            </property>
        </configuration>
    ```

Создайте файл `$SPARK_CONF_DIR/core-site.xml` с расположением хранилища, например, порт HDFS IPC неймноды Hadoop или адрес и учетные данные бакета S3:

=== "HDFS"

    ```xml
        <?xml version="1.0" encoding="UTF-8"?>
        <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
        <configuration>
            <property>
                <name>fs.defaultFS</name>
                <value>hdfs://myhadoopcluster:9820</value>
            </property>
        </configuration>
    ```

=== "S3"

    ```xml
        <?xml version="1.0" encoding="UTF-8"?>
        <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
        <configuration>
            !-- См. https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#General_S3A_Client_configuration
            <property>
                <name>fs.defaultFS</name>
                <value>s3a://mys3bucket/</value>
            </property>
            <property>
                <name>fs.s3a.bucket.mybucket.endpoint</name>
                <value>http://s3.somain</value>
            </property>
            <property>
                <name>fs.s3a.bucket.mybucket.connection.ssl.enabled</name>
                <value>false</value>
            </property>
            <property>
                <name>fs.s3a.bucket.mybucket.path.style.access</name>
                <value>true</value>
            </property>
            <property>
                <name>fs.s3a.bucket.mybucket.aws.credentials.provider</name>
                <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
            </property>
            <property>
                <name>fs.s3a.bucket.mybucket.access.key</name>
                <value>some-user</value>
            </property>
            <property>
                <name>fs.s3a.bucket.mybucket.secret.key</name>
                <value>mysecrettoken</value>
            </property>
        </configuration>
    ```

## Использование Kerberos

Некоторые управляемые кластеры Hadoop используют аутентификацию Kerberos. В этом случае вы должны вызвать команду [kinit](https://web.mit.edu/kerberos/krb5-1.12/doc/user/user_commands/kinit.html) **ДО** запуска сессии Spark для генерации тикета Kerberos. См. [Установка Kerberos][install-kerberos].

Иногда также требуется передать файл keytab в конфигурацию Spark, позволяя экзекуторам Spark генерировать собственные тикеты Kerberos:

=== "Spark 3"

    ```python
        SparkSession.builder
            .option("spark.kerberos.access.hadoopFileSystems", "hdfs://namenode1.domain.com:9820,hdfs://namenode2.domain.com:9820")
            .option("spark.kerberos.principal", "user")
            .option("spark.kerberos.keytab", "/path/to/keytab")
            .gerOrCreate()
    ```

=== "Spark 2"

    ```python
        SparkSession.builder
            .option("spark.yarn.access.hadoopFileSystems", "hdfs://namenode1.domain.com:9820,hdfs://namenode2.domain.com:9820")
            .option("spark.yarn.principal", "user")
            .option("spark.yarn.keytab", "/path/to/keytab")
            .gerOrCreate()
    ```

Дополнительную информацию см. в [документации по безопасности Spark](https://spark.apache.org/docs/latest/security.html#kerberos).
