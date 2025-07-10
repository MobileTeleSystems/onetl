# Предварительные требования { #spark-hdfs-prerequisites }

## Совместимость версий

- Версии Hadoop: 2.x, 3.x (только с библиотеками Hadoop 3.x)
- Версии Spark: 2.3.x - 3.5.x
- Версии Java: 8 - 20

## Установка PySpark

Чтобы использовать коннектор SparkHDFS, у вас должен быть установлен PySpark (или добавлен в `sys.path`) **ДО** создания экземпляра коннектора.

См. [инструкцию по установке][install-spark] для получения более подробной информации.

## Использование Kerberos

Некоторые управляемые кластеры Hadoop используют аутентификацию Kerberos. В этом случае вам следует вызвать команду [kinit](https://web.mit.edu/kerberos/krb5-1.12/doc/user/user_commands/kinit.html) **ДО** запуска сессии Spark для генерации тикета Kerberos. См. [инструкции по установке Kerberos][install-kerberos].

Иногда также требуется передать файл keytab в конфигурацию Spark, позволяя исполнителям Spark генерировать собственные тикеты Kerberos:

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

См. [документацию по безопасности Spark](https://spark.apache.org/docs/latest/security.html#kerberos) для получения более подробной информации.
