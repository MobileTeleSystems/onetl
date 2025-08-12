# Spark { #install-spark }

<!-- 
```{eval-rst}
.. include:: ../../README.rst
    :start-after: .. _spark-install:
    :end-before: .. _java-install:
```
 -->

Все классы соединений БД (`Clickhouse`, `Greenplum`, `Hive` и другие) и все классы соединений FileDF (`SparkHDFS`, `SparkLocalFS`, `SparkS3`) требуют установки Spark.

## Установка Java

<!-- 
```{eval-rst}
.. include:: ../../README.rst
    :start-after: .. _java-install:
    :end-before: .. _pyspark-install:
```
 -->

В первую очередь следует установить JDK. Точная инструкция по установке зависит от вашей ОС, вот некоторые примеры:

```bash
yum install java-1.8.0-openjdk-devel  # CentOS 7 + Spark 2
dnf install java-11-openjdk-devel  # CentOS 8 + Spark 3
apt-get install openjdk-11-jdk  # Debian-based + Spark 3
```

### Матрица совместимости { #spark-compatibility-matrix }

| Spark                                                     | Python     | Java       |   Scala |
|-----------------------------------------------------------|------------|------------|---------|
| [2.3.x](https://spark.apache.org/docs/2.3.1/#downloading) | только 3.7 | только 8   |    2.11 |
| [2.4.x](https://spark.apache.org/docs/2.4.8/#downloading) | только 3.7 | только 8   |    2.11 |
| [3.2.x](https://spark.apache.org/docs/3.2.4/#downloading) | 3.7 - 3.10 | 8u201 - 11 |    2.12 |
| [3.3.x](https://spark.apache.org/docs/3.3.4/#downloading) | 3.7 - 3.12 | 8u201 - 17 |    2.12 |
| [3.4.x](https://spark.apache.org/docs/3.4.4/#downloading) | 3.7 - 3.12 | 8u362 - 20 |    2.12 |
| [3.5.x](https://spark.apache.org/docs/3.5.5/#downloading) | 3.8 - 3.13 | 8u371 - 20 |    2.12 |

## Установка PySpark

<!-- 
```{eval-rst}
.. include:: ../../README.rst
    :start-after: .. _pyspark-install:
    :end-before: With File connections
```
 -->

Затем нужно установить PySpark, передав `spark` в `extras`:

```bash
pip install onetl[spark]  # установить последнюю версию PySpark
```

или установить PySpark явно:

```bash
pip install onetl pyspark==3.5.5  # установить конкретную версию PySpark
```

или внедрить PySpark в `sys.path` каким-либо другим способом ДО создания экземпляра класса. **В противном случае объект соединения не может быть создан.**

## Внедрение Java-пакетов { #java-packages }

Некоторые классы соединений БД и FileDF требуют внедрения определенных пакетов в `CLASSPATH` сессии Spark,
например, JDBC драйверов.

Обычно это делается путем настройки опции `spark.jars.packages` при создании сессии Spark:

```python
# вот список пакетов для загрузки:
maven_packages = (
    Greenplum.get_packages(spark_version="3.2")
    + MySQL.get_packages()
    + Teradata.get_packages()
)

spark = (
    SparkSession.builder.config("spark.app.name", "onetl")
    .config("spark.jars.packages", ",".join(maven_packages))
    .getOrCreate()
)
```

Spark автоматически разрешает пакет и все его зависимости, загружает их и внедряет в сессию Spark (как драйвер, так и все исполнители).

Это требует доступа к интернету, поскольку метаданные пакетов и файлы `.jar` загружаются из [Maven Repository](https://mvnrepository.com/).

Но иногда требуется:

- Установить пакет без прямого доступа к интернету (изолированная сеть)
- Установить пакет, который недоступен в Maven

Существует несколько способов сделать это.

### Использование `spark.jars`

Самое простое решение, но это требует хранения файлов `.jar` где-то в файловой системе или на веб-сервере.

- Загрузите файлы `package.jar` (обычно это что-то вроде `some-package_1.0.0.jar`). Локальное имя файла не имеет значения, но оно должно быть уникальным.
- (Для `spark.submit.deployMode=cluster`) поместите загруженные файлы в HDFS или разверните их на любом HTTP-веб-сервере, обслуживающем статические файлы. Смотрите [официальную документацию](https://spark.apache.org/docs/latest/submitting-applications.html#advanced-dependency-management) для получения более подробной информации.
- Создайте сессию Spark, передав абсолютный путь к файлу `.jar` в опцию конфигурации Spark `spark.jars`:

=== spark.submit.deployMode=client (по умолчанию)

    ```python 

        jar_files = ["/path/to/package.jar"]

        # не передавайте spark.jars.packages
        spark = (
            SparkSession.builder.config("spark.app.name", "onetl")
            .config("spark.jars", ",".join(jar_files))
            .getOrCreate()
        )
    ```

=== spark.submit.deployMode=cluster

    ```python 

        # вы также можете передавать URL-адреса, например http://domain.com/path/to/downloadable/package.jar
        jar_files = ["hdfs:///path/to/package.jar"]

        # не передавайте spark.jars.packages
        spark = (
            SparkSession.builder.config("spark.app.name", "onetl")
            .config("spark.jars", ",".join(jar_files))
            .getOrCreate()
        )
    ```

### Использование `spark.jars.repositories`

!!! note "Примечание"

    В этом случае Spark все равно будет пытаться получить пакеты из интернета, поэтому если у вас нет доступа к интернету, сессия Spark будет создана со значительной задержкой из-за всех попыток получить пакеты.

Может использоваться, если у вас есть доступ как к публичным репозиториям (таким как Maven), так и к приватному репозиторию Artifactory/Nexus.

- Настройте приватный репозиторий Maven в [JFrog Artifactory](https://jfrog.com/artifactory/) или [Sonatype Nexus](https://www.sonatype.com/products/sonatype-nexus-repository).
- Загрузите файл `package.jar` (обычно это что-то вроде `some-package_1.0.0.jar`). Локальное имя файла не имеет значения.
- Загрузите файл `package.jar` в приватный репозиторий (с теми же `groupId` и `artifactoryId`, что и в исходном пакете в Maven).
- Передайте URL репозитория в опцию конфигурации Spark `spark.jars.repositories`.
- Создайте сессию Spark, передав имя пакета в опцию конфигурации Spark `spark.jars.packages`:

```python
maven_packages = (
    Greenplum.get_packages(spark_version="3.2")
    + MySQL.get_packages()
    + Teradata.get_packages()
)

spark = (
    SparkSession.builder.config("spark.app.name", "onetl")
    .config("spark.jars.repositories", "http://nexus.mydomain.com/private-repo/")
    .config("spark.jars.packages", ",".join(maven_packages))
    .getOrCreate()
)
```

### Использование `spark.jars.ivySettings`

То же самое, что и выше, но может использоваться даже при отсутствии сетевого доступа к публичным репозиториям, таким как Maven.

- Настройте приватный репозиторий Maven в [JFrog Artifactory](https://jfrog.com/artifactory/) или [Sonatype Nexus](https://www.sonatype.com/products/sonatype-nexus-repository).
- Загрузите файл `package.jar` (обычно это что-то вроде `some-package_1.0.0.jar`). Локальное имя файла не имеет значения.
- Загрузите файл `package.jar` в [приватный репозиторий](https://help.sonatype.com/repomanager3/nexus-repository-administration/repository-management#RepositoryManagement-HostedRepository) (с теми же `groupId` и `artifactoryId`, что и в исходном пакете в Maven).
- Создайте файл `ivysettings.xml` (см. ниже).
- Добавьте сюда resolver с URL репозитория (и учетными данными, если требуется).
- Передайте абсолютный путь к `ivysettings.xml` в опцию конфигурации Spark `spark.jars.ivySettings`.
- Создайте сессию Spark, передав имя пакета в опцию конфигурации Spark `spark.jars.packages`:

=== ivysettings-all-packages-uploaded-to-nexus.xml

    ```xml 
        <ivysettings>
            <settings defaultResolver="main"/>
            <resolvers>
                <chain name="main" returnFirst="true">
                    <!-- Использовать кэш Maven -->
                    <ibiblio name="local-maven-cache" m2compatible="true" root="file://${user.home}/.m2/repository"/>
                    <!-- Использовать файлы -/.ivy2/jars/*.jar -->
                    <ibiblio name="local-ivy2-cache" m2compatible="false" root="file://${user.home}/.ivy2/jars"/>
                    <!-- Загрузить все пакеты из собственного экземпляра Nexus -->
                    <ibiblio name="nexus-private" m2compatible="true" root="http://nexus.mydomain.com/private-repo/" />
                </chain>
            </resolvers>
        </ivysettings>
    ```

=== ivysettings-private-packages-in-nexus-public-in-maven.xml

    ```xml 
        <ivysettings>
            <settings defaultResolver="main"/>
            <resolvers>
                <chain name="main" returnFirst="true">
                    <!-- Использовать кэш Maven -->
                    <ibiblio name="local-maven-cache" m2compatible="true" root="file://${user.home}/.m2/repository"/>
                    <!-- Использовать файлы -/.ivy2/jars/*.jar -->
                    <ibiblio name="local-ivy2-cache" m2compatible="false" root="file://${user.home}/.ivy2/jars"/>
                    <!-- Загрузить приватные пакеты из собственного экземпляра Nexus -->
                    <ibiblio name="nexus-private" m2compatible="true" root="http://nexus.mydomain.com/private-repo/" />
                    <!-- Загрузить другие пакеты из Maven -->
                    <ibiblio name="central" m2compatible="true" />
                    <!-- Загрузить другие пакеты из SparkPackages -->
                    <ibiblio name="spark-packages" m2compatible="true" root="https://repos.spark-packages.org/" />
                </chain>
            </resolvers>
        </ivysettings>
    ```

=== ivysettings-private-packages-in-nexus-public-fetched-using-proxy-repo.xml

    ```xml 
        <ivysettings>
            <settings defaultResolver="main"/>
            <resolvers>
                <chain name="main" returnFirst="true">
                    <!-- Использовать кэш Maven -->
                    <ibiblio name="local-maven-cache" m2compatible="true" root="file://${user.home}/.m2/repository"/>
                    <!-- Использовать файлы -/.ivy2/jars/*.jar -->
                    <ibiblio name="local-ivy2-cache" m2compatible="false" root="file://${user.home}/.ivy2/jars"/>
                    <!-- Загрузить приватные пакеты из собственного экземпляра Nexus -->
                    <ibiblio name="nexus-private" m2compatible="true" root="http://nexus.mydomain.com/private-repo/" />
                    <!-- Загрузить публичные пакеты из того же экземпляра Nexus, используя Proxy Repo
                    См. https://help.sonatype.com/repomanager3/nexus-repository-administration/repository-management#RepositoryManagement-ProxyRepository
                    -->
                    <ibiblio name="nexus-proxy" m2compatible="true" root="http://nexus.mydomain.com/proxy-repo/" />
                </chain>
            </resolvers>
        </ivysettings>
    ```

=== ivysettings-nexus-with-auth-required.xml

    ```xml 
        <ivysettings>
            <settings defaultResolver="main"/>
            <properties environment="env"/>
            <!-- использовать переменные окружения NEXUS_USER и NEXUS_PASSWORD как учетные данные для аутентификации в Nexus -->
            <property name="repo.username" value="${env.NEXUS_USER}"/>
            <property name="repo.pass" value="${env.NEXUS_PASSWORD}"/>
            <!-- значение realm описано
            - здесь https://stackoverflow.com/a/38019000
            - здесь https://github.com/sonatype/nexus-book-examples/blob/master/ant-ivy/simple-project/ivysettings.xml
            - здесь https://support.sonatype.com/hc/en-us/articles/213465388-How-do-I-configure-my-Ivy-build-to-deploy-artifacts-to-Nexus-Repository-2-
            -->
            <credentials host="nexus.mydomain.com" username="${repo.username}" passwd="${repo.pass}" realm="Sonatype Nexus Repository Manager" />
            <resolvers>
                <chain name="main" returnFirst="true">
                    <!-- Использовать кэш Maven -->
                    <ibiblio name="local-maven-cache" m2compatible="true" root="file://${user.home}/.m2/repository"/>
                    <!-- Использовать файлы -/.ivy2/jars/*.jar -->
                    <ibiblio name="local-ivy2-cache" m2compatible="false" root="file://${user.home}/.ivy2/jars"/>
                    <!-- Загрузить все пакеты из собственного экземпляра Nexus, используя учетные данные для домена выше -->
                    <ibiblio name="nexus-private" m2compatible="true" root="http://nexus.mydomain.com/private-repo/" />
                </chain>
            </resolvers>
        </ivysettings>

    ```

```python "script.py"

maven_packages = (
    Greenplum.get_packages(spark_version="3.2")
    + MySQL.get_packages()
    + Teradata.get_packages()
)

spark = (
    SparkSession.builder.config("spark.app.name", "onetl")
    .config("spark.jars.ivySettings", "/path/to/ivysettings.xml")
    .config("spark.jars.packages", ",".join(maven_packages))
    .getOrCreate()
)
```

### Поместить файл `.jar` в `-/.ivy2/jars/`

Может использоваться для передачи уже загруженного файла в Ivy и пропуска разрешения пакета из Maven.

- Загрузите файл `package.jar` (обычно это что-то вроде `some-package_1.0.0.jar`). Локальное имя файла не имеет значения, но оно должно быть уникальным.
- Переместите его в папку `-/.ivy2/jars/`.
- Создайте сессию Spark, передав имя пакета в опцию конфигурации Spark `spark.jars.packages`:

```python
maven_packages = (
    Greenplum.get_packages(spark_version="3.2")
    + MySQL.get_packages()
    + Teradata.get_packages()
)

spark = (
    SparkSession.builder.config("spark.app.name", "onetl")
    .config("spark.jars.packages", ",".join(maven_packages))
    .getOrCreate()
)
```

### Поместить файл `.jar` в папку jars Spark

!!! note "Примечание"

    Файл пакета должен быть размещен на всех хостах/контейнерах, где работает Spark, как на драйвере, так и на всех экзекуторах.

    Обычно это используется только с:
        * `spark.master=local` (драйвер и исполнители работают на одном хосте),
        * `spark.master=k8s://...` (файлы `.jar` добавляются в образ или в том, подключенный ко всем подам).

Может использоваться для встраивания файлов `.jar` в путь классов Spark по умолчанию.

- Загрузите файл `package.jar` (обычно это что-то вроде `some-package_1.0.0.jar`). Локальное имя файла не имеет значения, но оно должно быть уникальным.
- Переместите его в папку `$SPARK_HOME/jars/`, например, `^/.local/lib/python3.7/site-packages/pyspark/jars/` или `/opt/spark/3.2.3/jars/`.
- Создайте сессию Spark **БЕЗ** передачи имени пакета в `spark.jars.packages`

```python
# нет необходимости устанавливать spark.jars.packages или любую другую опцию spark.jars.*
# все jars уже присутствуют в CLASSPATH и загружаются автоматически

spark = SparkSession.builder.config("spark.app.name", "onetl").getOrCreate()
```

### Ручное добавление файлов `.jar` в `CLASSPATH`

!!! note "Примечание"

    Файл пакета должен быть размещен на всех хостах/контейнерах, где работает Spark, как на драйвере, так и на всех экзекуторах.

    Обычно это используется только с:
        * `spark.master=local` (драйвер и исполнители работают на одном хосте),
        * `spark.master=k8s://...` (файлы `.jar` добавляются в образ или в том, подключенный ко всем подам).

Может использоваться для встраивания файлов `.jar` в путь классов Java по умолчанию.

- Загрузите файл `package.jar` (обычно это что-то вроде `some-package_1.0.0.jar`). Локальное имя файла не имеет значения.
- Установите переменную окружения `CLASSPATH` на `/path/to/package.jar`. Вы можете установить несколько путей к файлам
- Создайте сессию Spark **БЕЗ** передачи имени пакета в `spark.jars.packages`

```python
# нет необходимости устанавливать spark.jars.packages или любую другую опцию spark.jars.*
# все jars уже присутствуют в CLASSPATH и загружаются автоматически

import os

jar_files = ["/path/to/package.jar"]
# разные разделители для Windows и Linux
delimiter = ";" if os.name == "nt" else ":"
spark = (
    SparkSession.builder.config("spark.app.name", "onetl")
    .config("spark.driver.extraClassPath", delimiter.join(jar_files))
    .config("spark.executor.extraClassPath", delimiter.join(jar_files))
    .getOrCreate()
)
```
