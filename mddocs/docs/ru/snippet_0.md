## Что такое onETL?

Python ETL/ELT библиотека, основанная на [Apache Spark](https://spark.apache.org/) и других инструментах с открытым исходным кодом.

## Цели

- Предложить унифицированные классы для извлечения (**E**) и загрузки данных (**L**) при работе с различными хранилищами.
- Обеспечить возможность использования [Spark DataFrame API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html) для выполнения преобразований (**T**) (в терминах *ETL*).
- Предоставить прямой доступ к базе данных, позволяющий выполнять SQL-запросы, а также DDL, DML и вызывать функции/процедуры. Эта функциональность предназначена для построения *ELT* конвейеров.
- Обеспечить поддержку различных [стратегий чтения][strategy] для инкрементной и пакетной выборки данных.
- Предоставить [хуки][hooks] и [плагины][plugins] для изменения поведения внутренних классов.

<!-- (../ru/plugins) -->

## Не цели

- onETL не является заменой Spark. Она просто предоставляет дополнительные функциональные возможности, которых нет в Spark, и улучшает UX для конечных пользователей.
- onETL не является фреймворком, поскольку не имеет требований к структуре проекта, именованию, способу запуска ETL/ELT процессов, конфигурации и т.д. Все это должно быть реализовано в каком-то другом инструменте.
- onETL намеренно разрабатывается без какой-либо интеграции с программным обеспечением для планирования, таким как, например, Apache Airflow. Все интеграции должны быть реализованы как отдельные инструменты.
- Только пакетные процессы обработки данных, без их потоковой передачи. Для потоковой передачи используйте [Apache Flink](https://flink.apache.org/).

## Требования

- **Python** 3.7 - 3.13
- PySpark 2.3.x - 3.5.x (зависит от используемого коннектора)
- Java 8+ (требуется Spark, см. ниже)
- Kerberos libs & GCC (требуется коннекторами `Hive`, `HDFS` и `SparkHDFS`)

## Поддерживаемые хранилища


| Тип               | Хранилище      | На базе                                                                                                              |
|--------------------|--------------|-------------------------------------------------------------------------------------------------------------------------|
| База данных {: rowspan=5} | Clickhouse<br/>MSSQL<br/>MySQL<br/>Postgres<br/>Oracle<br/>Teradata   | <br/><br/>Apache Spark [JDBC Data Source](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)                      |
| Hive         | Apache Spark [Hive integration](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html)                |
| Kafka        | Apache Spark [Kafka integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)     |
| Greenplum    | VMware [Greenplum Spark connector](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/index.html) |
| MongoDB      | [MongoDB Spark connector](https://www.mongodb.com/docs/spark-connector/current)                                       |
| Файл {: rowspan=6}  | HDFS         | [HDFS Python client](https://pypi.org/project/hdfs/)                                                                  |
| S3           | [minio-py client](https://pypi.org/project/minio/)                                                                    |
| SFTP         | [Paramiko library](https://pypi.org/project/paramiko/)                                                                |
| FTP<br/>FTPS       | [FTPUtil library](https://pypi.org/project/ftputil/)                                                                  |
| WebDAV       | [WebdavClient3 library](https://pypi.org/project/webdavclient3/)                                                     |
| Samba        | [pysmb library](https://pypi.org/project/pysmb/)                                                                      |
| Файлы как DataFrame {: rowspan=2} | SparkLocalFS<br/>SparkHDFS | Apache Spark [File Data Source](https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html)           |
| SparkS3      | Библиотека [Hadoop AWS](https://hadoop.apache.org/docs/current3/hadoop-aws/tools/hadoop-aws/index.html)                  |
