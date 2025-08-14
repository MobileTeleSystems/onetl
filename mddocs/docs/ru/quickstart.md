# onETL

{{ repo_status_badge }}
{{ pypi_release_bage }}
{{ pypi_license_bage }}
{{ pypi_pyversion_bage }}
{{ pypi_downloads_bage }}

{{ docs_status_badge }}
{{ ci_status_badge  }}
{{ precommit_badge  }}
<!-- {-{ test_cov_badge }-} -->

{{ onetl_logo_wide }}

----8<----
../mddocs/docs/ru/snippet_0.md
----8<----

 <!-- (documentation)= -->

## Документация

Смотрите на [ReadTheDocs](https://onetl.readthedocs.io/ru/latest/)

## Как установить

<!-- (install)= -->

### Минимальная установка

<!-- (minimal-install)= -->

Базовый пакет `onetl` содержит:

- `DBReader`, `DBWriter` и связанные с ними классы
- `FileDownloader`, `FileUploader`, `FileMover` и вспомогательные классы, такие как файловые фильтры и лимиты
- `FileDFReader`, `FileDFWriter` и дополняющие их классы, такие как, например, форматы файлов
- Стратегии чтения и классы HWM
- Поддержку плагинов

Базовый пакет можно установить выполнив:

```bash
pip install onetl
```

!!! warning

    Этот способ установки пакета НЕ включает в него какие-либо подключения.
    Этот метод рекомендуется использовать в сторонних библиотеках, которые требуют установки ``onetl``, но не используют ее классы подключений.

### С подключениями к БД и возможностью чтения файловых данных в Spark DataFrame

<!-- (spark-install)= -->

Все классы подключений к БД (`Clickhouse`, `Greenplum`, `Hive` и другие) также как и все классы представления файловых данных как DataFrame (`SparkHDFS`, `SparkLocalFS`, `SparkS3`) требуют установки Spark.

<!-- (java-install)= -->

Во-первых, необходимо установить JDK. Точная инструкция по установке зависит от вашей ОС, вот несколько примеров:

```shell
yum install java-1.8.0-openjdk-devel  # CentOS 7 | Spark 2
dnf install java-11-openjdk-devel  # CentOS 8 | Spark 3
apt-get install openjdk-11-jdk  # Debian-based | Spark 3
```

<!-- (spark-compatibility-matrix)= -->

#### Матрица совместимости

| Spark                                                     | Python     | Java       | Scala |
| --------------------------------------------------------- | ---------- | ---------- | ----- |
| [2.3.x](https://spark.apache.org/docs/2.3.1/#downloading) | 3.7 only   | 8 only     | 2.11  |
| [2.4.x](https://spark.apache.org/docs/2.4.8/#downloading) | 3.7 only   | 8 only     | 2.11  |
| [3.2.x](https://spark.apache.org/docs/3.2.4/#downloading) | 3.7 - 3.10 | 8u201 - 11 | 2.12  |
| [3.3.x](https://spark.apache.org/docs/3.3.4/#downloading) | 3.7 - 3.12 | 8u201 - 17 | 2.12  |
| [3.4.x](https://spark.apache.org/docs/3.4.4/#downloading) | 3.7 - 3.12 | 8u362 - 20 | 2.12  |
| [3.5.x](https://spark.apache.org/docs/3.5.5/#downloading) | 3.8 - 3.13 | 8u371 - 20 | 2.12  |

<!-- (pyspark-install)= -->

Затем необходимо установить PySpark, передав `spark` в `extras`:

```bash
pip install onetl[spark]  # установить последнюю версию PySpark
```

или установить PySpark явно:

```bash
pip install onetl pyspark==3.5.5  # установить определенную версию PySpark
```

или внедрите PySpark в `sys.path` каким-либо другим способом ДО создания экземпляра класса. **В противном случае объект подключения не может быть создан.**

### С файловыми подключениями

<!-- (files-install)= -->

Все классы файловых (но не *FileDF*) подключений (`FTP`, `SFTP`, `HDFS` и т.д.) требуют установки определенных Python клиентов.

Каждый клиент можно установить явно, передав имя коннектора (в нижнем регистре) в `extras`:

```bash
pip install onetl[ftp]  # конкретный коннектор
pip install onetl[ftp,ftps,sftp,hdfs,s3,webdav,samba]  # несколько коннекторов
```

Чтобы установить все файловые коннекторы сразу, вы можете передать `files` в `extras`:

```bash
pip install onetl[files]
```

**В противном случае импорт класса завершится неудачей.**

### С поддержкой Kerberos

<!-- (kerberos-install)= -->

Многие экземпляры Hadoop развернуты с поддержкой Kerberos, поэтому для правильной работы некоторых соединений требуется дополнительная настройка.

- `HDFS`<br/>
  Использует [requests-kerberos](https://pypi.org/project/requests-kerberos/) и [GSSApi](https://pypi.org/project/gssapi/) для аутентификации. Он также выполняет `kinit` для создания тикета Kerberos.
- `Hive` и `SparkHDFS`<br/>
  требуют наличия тикета Kerberos перед созданием сеанса Spark.

Таким образом, необходимо установить пакеты ОС содержащие:

- Библиотеки `krb5`
- Заголовочные файлы для `krb5`
- `gcc` или другой компилятор для C

Точная инструкция по установке зависит от вашей ОС, вот несколько примеров:

```bash
apt install libkrb5-dev krb5-user gcc  # Debian-based
dnf install krb5-devel krb5-libs krb5-workstation gcc  # CentOS, OracleLinux
```

Также вы должны передать `kerberos` в `extras` для установки необходимых пакетов Python:

```bash
pip install onetl[kerberos]
```

### Полный пакет

<!-- (full-bundle-1)= -->

Чтобы установить все коннекторы и зависимости, вы можете передать `all` в `extras`:

```bash
pip install onetl[all]

# это то же самое, что и
pip install onetl[spark,files,kerberos]
```

!!! warning

    Этот метод потребляет много дискового пространства и требует установки библиотек Java и Kerberos в вашей ОС.

<!-- (quick-start)= -->

## Быстрый старт

### MSSQL → Hive

Чтение данных из MSSQL, преобразование и запись в Hive.

```bash
# установить onETL и PySpark
pip install onetl[spark]
```

```python
# Импорт pyspark для инициализации SparkSession
from pyspark.sql import SparkSession

# Импорт функции для настройки логирования onETL
from onetl.log import setup_logging

# Импорт необходимых подключений
from onetl.connection import MSSQL, Hive

# Импорт классов onETL для чтения и записи данных
from onetl.db import DBReader, DBWriter

# Изменение уровня логирования на INFO, а также настройка формата логирования и обработчика
setup_logging()

# Инициализация SparkSession с загрузкой драйвера MSSQL
maven_packages = MSSQL.get_packages()
spark = (
    SparkSession.builder.appName("spark_app_onetl_demo")
    .config("spark.jars.packages", ",".join(maven_packages))
    .enableHiveSupport()  # for Hive
    .getOrCreate()
)

# Инициализация подключения к MSSQL и проверка его доступности
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

# Инициализация DBReader
reader = DBReader(
    connection=mssql,
    source="dbo.demo_table",
    columns=["on", "etl"],
    # Установка опций чтения MSSQL:
    options=MSSQL.ReadOptions(fetchsize=10000),
)

# Проверка, что таблица содержит данные
reader.raise_if_no_data()

# Чтение данных в DataFrame
df = reader.run()
df.printSchema()
# root
#  |-- id: integer (nullable = true)
#  |-- phone_number: string (nullable = true)
#  |-- region: string (nullable = true)
#  |-- birth_date: date (nullable = true)
#  |-- registered_at: timestamp (nullable = true)
#  |-- account_balance: double (nullable = true)

# Применение любых трансформаций PySpark
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

# Инициализация подключения Hive (не требует загрузки драйвера Hive в сессию Spark)
hive = Hive(cluster="rnd-dwh", spark=spark)

# Инициализация DBWriter
db_writer = DBWriter(
    connection=hive,
    target="dl_sb.demo_table",
    # Установка опций записи Hive:
    options=Hive.WriteOptions(if_exists="replace_entire_table"),
)

# Запись данных из DataFrame в Hive
db_writer.run(df_to_write)

# Успех!
```

### SFTP → HDFS

Получение файлов из SFTP и загрузка их в HDFS.

```bash
# установить onETL с клиентами SFTP и HDFS, и поддержкой Kerberos
pip install onetl[hdfs,sftp,kerberos]
```

```python
# Импорт функции для настройки логирования onETL
from onetl.log import setup_logging

# Импорт необходимых подключений
from onetl.connection import SFTP, HDFS

# Импорт классов onETL для скачивания и загрузки файлов
from onetl.file import FileDownloader, FileUploader

# Импорт вспомогательных классов filter & limit
from onetl.file.filter import Glob, ExcludeDir
from onetl.file.limit import MaxFilesCount

# Изменение уровня логирования на INFO, а также настройка формата логирования и обработчика
setup_logging()

# Инициализация подключения к SFTP и его проверка
sftp = SFTP(
    host="sftp.test.com",
    user="someuser",
    password="somepassword",
).check()

# >>> INFO:|SFTP| Connection is available

# Инициализация объекта скачивания файлов
file_downloader = FileDownloader(
    connection=sftp,
    source_path="/remote/tests/Report",  # path on SFTP
    local_path="/local/onetl/Report",  # local fs path
    filters=[
        # скачивать только если имена файлов совпадают с glob
        Glob("*.csv"),
        # исключить файлы из директории
        ExcludeDir("/remote/tests/Report/exclude_dir/"),
    ],
    limits=[
        # скачивать не более 1000 за запуск
        MaxFilesCount(1000),
    ],
    options=FileDownloader.Options(
        # Удалить файлы из SFTP после успешной загрузки
        delete_source=True,
        # пометить файлы ошибкой, если в локальном пути существует файл с таким же именем
        if_exists="error",
    ),
)

# Скачать файлы на локальную файловую систему
download_result = downloader.run()

# Метод run возвращает объект DownloadResult,
# который содержит коллекцию полученных файлов разделенную на 4 категории:
# successful - успешно скачанные файлы
# failed - файлы, которые не удалось скачать
# ignored - файлы, которые были игнорированы по фильтрам
# missing - файлы, которые отсутствовали на SFTP
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

# Выбросить исключение если есть файлы с ошибкой или на удаленном сервере не было файлов
download_result.raise_if_failed() or download_result.raise_if_empty()

# Выполняйте любые необходимые вам трансформации с файлами: переименуйте, удалите строку заголовков в csv...
renamed_files = my_rename_function(download_result.success)

# function removed "_" from file names
# [
#    LocalPath('/home/onetl/Report/file1.json'),
#    LocalPath('/home/onetl/Report/file2.json'),
# ]

# Инициализировать подключение к HDFS
hdfs = HDFS(
    host="my.name.node",
    user="someuser",
    password="somepassword",  # or keytab
)

# Инициализировать объект загрузки файлов
file_uploader = FileUploader(
    connection=hdfs,
    target_path="/user/onetl/Report/",  # hdfs path
)

# Загрузить файлы из локальной ФС в HDFS
upload_result = file_uploader.run(renamed_files)

# Метод run вернет объект UploadResult,
# который содержит коллекцию полученных файлов разделенную на 4 категории:
# successful - успешно скачанные файлы
# failed - файлы, которые не удалось скачать
# ignored - файлы, которые были игнорированы по фильтрам
# missing - файлы, которые отсутствовали на SFTP
upload_result

#  UploadResult(
#      successful=[RemoteFile('/user/onetl/Report/file1.json')],
#      failed=[FailedLocalFile('/local/onetl/Report/file2.json')],
#      ignored=[],
#      missing=[],
#  )

# Выбросить исключение если есть файлы с ошибкой или на локальной ФС не было файлов
upload_result.raise_if_failed() or upload_result.raise_if_empty() or upload_result.raise_if_missing()

# Успех!
```

### S3 → Postgres

Чтение файлов непосредственно из пути S3, преобразование их в DataFrame, преобразование его и затем запись в базу данных.

```bash
# установить onETL и PySpark
pip install onetl[spark]
```

```python
# Импорт pyspark для инициализации SparkSession
from pyspark.sql import SparkSession

# Импорт функции для настройки логирования onETL
from onetl.log import setup_logging

# Импорт необходимых подключений
from onetl.connection import Postgres, SparkS3

# Импорт классов onETL необходимых для чтения файлов
from onetl.file import FileDFReader
from onetl.file.format import CSV

# импорт классов onETL для записи данных
from onetl.db import DBWriter

# Изменение уровня логирования на INFO, а также настройка формата логирования и обработчика
setup_logging()

# Инициализация SparkSession с включение библиотеки Hadoop AWS и JDBC драйвера Postgres
maven_packages = SparkS3.get_packages(spark_version="3.5.5") | Postgres.get_packages()
exclude_packages = SparkS3.get_exclude_packages()
spark = (
    SparkSession.builder.appName("spark_app_onetl_demo")
    .config("spark.jars.packages", ",".join(maven_packages))
    .config("spark.jars.excludes", ",".join(exclude_packages))
    .getOrCreate()
)

# Инициализация подключения к S3 и его проверка
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

# Опишите формат файла и опции его парсинга
csv = CSV(
    delimiter=";",
    header=True,
    encoding="utf-8",
)

# Опишите структуру данных в файле для загрузки их в DataFrame
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

# Инициализация объекта чтения из файлов в DataFrame
reader = FileDFReader(
    connection=spark_s3,
    source_path="/remote/tests/Report",  # путь на S3, по которому расположены файлы *.csv
    format=csv,  # формат файлов со специфическими опциями разбора
    df_schema=df_schema,  # колонки и их типы
)

# Прочитать данные из файла на S3 в Spark DataFrame
df = reader.run()

# Проверить что схема DataFrame такая как ожидается
df.printSchema()
# root
#  |-- id: integer (nullable = true)
#  |-- phone_number: string (nullable = true)
#  |-- region: string (nullable = true)
#  |-- birth_date: date (nullable = true)
#  |-- registered_at: timestamp (nullable = true)
#  |-- account_balance: double (nullable = true)

# Применить любые трансформации PySpark
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

# Инициализировать подключение к Postgres
postgres = Postgres(
    host="192.169.11.23",
    user="onetl",
    password="somepassword",
    database="mydb",
    spark=spark,
)

# Инициализировать объект DBWriter
db_writer = DBWriter(
    connection=postgres,
    # write to specific table
    target="public.my_table",
    # with some writing options
    options=Postgres.WriteOptions(if_exists="append"),
)

# Записать DataFrame в таблицу Postgres
db_writer.run(df_to_write)

# Успех!
```
