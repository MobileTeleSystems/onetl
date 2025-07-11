# Предварительные требования { #greenplum-prerequisites }

## Совместимость версий

- Версии сервера Greenplum:
    - Официально заявленные: 5.x, 6.x и 7.x (для которой требуется `Greenplum.get_packages(package_version="2.3.0")` или выше)
    - Фактически протестированные: 6.23, 7.0
- Версии Spark: 2.3.x - 3.2.x (Spark 3.3+ пока не поддерживается)
- Версии Java: 8 - 11

См. [официальную документацию](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.2/greenplum-connector-spark/release_notes.html).

## Установка PySpark

Для использования коннектора Greenplum у вас должен быть установлен PySpark (или добавлен в `sys.path`) **ДО** создания экземпляра коннектора.

См. [инструкцию по установке][install-spark] для более подробной информации.

## Загрузка пакета VMware

Для использования коннектора Greenplum вам необходимо загрузить файл `.jar` коннектора с  [веб-сайта VMware](https://network.tanzu.vmware.com/products/vmware-greenplum#/releases/1413479/file_groups/16966), а затем передать его в сессию Spark.

!!! warning "Предупреждение"

    Обратите внимание на [матрицу совместимости Spark и Scala][spark-compatibility-matrix].

!!! warning "Предупреждение"

    Существуют проблемы с использованием пакета версии 2.3.0/2.3.1 с Greenplum 6.x - коннектор может открыть транзакцию с запросом `SELECT * FROM table LIMIT 0`, но не закрывает её, что приводит к взаимным блокировкам во время записи.

Есть несколько способов сделать это. Более подробно описано в разделе [установка Java-пакетов][java-packages].

!!! note "Примечание"

    Если вы загружаете пакет в приватный репозиторий пакетов, используйте `groupId=io.pivotal` и `artifactoryId=greenplum-spark_2.12`
    (`2.12` - это версия Scala), чтобы дать загруженному пакету правильное имя.

## Подключение к Greenplum

### Схема взаимодействия

Исполнители Spark открывают порты для прослушивания входящих запросов.
Сегменты Greenplum инициируют подключения к исполнителям Spark, используя функциональность [EXTERNAL TABLE](https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/ref_guide-sql_commands-CREATE_EXTERNAL_TABLE.html), и отправляют/читают данные по протоколу [gpfdist](https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/admin_guide-external-g-using-the-greenplum-parallel-file-server--gpfdist-.html#about-gpfdist-setup-and-performance-1).

Данные **не** отправляются через мастер-узел Greenplum.
Мастер Greenplum только получает команды для начала процесса чтения/записи и управляет всеми метаданными (расположение внешней таблицы, схема и т.д.).

Более подробную информацию можно найти в [официальной документации](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/overview.html).

### Установка количества соединений

!!! warning "Предупреждение"

    Это очень важно!!!

    Если вы не ограничите количество соединений, их число может превысить лимит [max_connections](https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/admin_guide-client_auth.html#limiting-concurrent-connections#limiting-concurrent-connections-2), установленный на стороне Greenplum. Обычно он не так высок, например, максимум 500-1000 соединений, в зависимости от настроек вашего экземпляра Greenplum и использования балансировщиков соединений, таких как `pgbouncer`.

    Потребление всех доступных соединений означает, что **никто** (даже администраторы) не сможет подключиться к Greenplum.

Каждая задача на экзекуторе Spark создает собственное соединение с мастер-узлом Greenplum, поэтому вам нужно ограничить количество соединений, чтобы избежать слишком большого открытых соединений.

- Чтение около `5-10Гб` данных требует около `3-5` параллельных соединений.
- Чтение около `20-30Гб` данных требует около `5-10` параллельных соединений.
- Чтение около `50Гб` данных требует ~ `10-20` параллельных соединений.
- Чтение около `100+Гб` данных требует `20-30` параллельных соединений.
- Открытие более `30-50` соединений не рекомендуется.

Количество соединений можно ограничить 2 способами:

- Ограничение количества экзекуторов Spark и количества ядер на исполнитель. Максимальное количество параллельных задач равно `исполнители * ядра`.

=== "Spark с master=local"

    ```python 
        spark = (
            SparkSession.builder
            # Spark будет работать с 5 потоками в локальном режиме, позволяя до 5 параллельных задач
            .config("spark.master", "local[5]")
            .config("spark.executor.cores", 1)
        ).getOrCreate() 
    ```

=== "Spark с master=yarn или master=k8s, dynamic allocation"

    ```python 
        spark = (
            SparkSession.builder
            .config("spark.master", "yarn")
            # Spark запустит МАКСИМУМ 10 исполнителей с 1 ядром каждый (динамически), так что максимальное число параллельных задач - 10
            .config("spark.dynamicAllocation.maxExecutors", 10)
            .config("spark.executor.cores", 1)
        ).getOrCreate() 
    ```

=== "Spark с master=yarn или master=k8s, static allocation"

    ```python
        spark = (
            SparkSession.builder
            .config("spark.master", "yarn")
            # Spark запустит РОВНО 10 исполнителей с 1 ядром каждый, так что максимальное число параллельных задач - 10
            .config("spark.executor.instances", 10)
            .config("spark.executor.cores", 1)
        ).getOrCreate() 
    ```

- Ограничение размера пула соединений, используемого Spark (**только** для Spark с `master=local`):

        ```python
            spark = SparkSession.builder.config("spark.master", "local[*]").getOrCreate()

            # Независимо от того, сколько исполнителей запущено и сколько ядер они имеют,
            # количество соединений не может превышать размер пула:
            Greenplum(
                ...,
                extra={
                    "pool.maxSize": 10,
                },
            ) 
        ```

См. документацию по [пулу соединений](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/using_the_connector.html#jdbcconnpool).

- Установка [num_partitions][onetl.connection.db_connection.greenplum.options.GreenplumReadOptions.num_partitions] и [partition_column][onetl.connection.db_connection.greenplum.options.GreenplumReadOptions.partition_column] (не рекомендуется).

### Разрешение подключения к мастеру Greenplum

Попросите администратора вашего кластера Greenplum разрешить вашему пользователю подключаться к мастер-узлу Greenplum, например, обновив файл `pg_hba.conf`.

Более подробную информацию можно найти в [официальной документации](https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/admin_guide-client_auth.html#limiting-concurrent-connections#allowing-connections-to-greenplum-database-0).

### Настройка порта подключения

#### Spark с `master=k8s`

Пожалуйста, следуйте [официальной документации](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/configure.html#k8scfg)

#### Spark с `master=yarn` или `master=local`

Для чтения данных из Greenplum с использованием Spark, в межсетевом экране между Spark и Greenplum должны быть открыты следующие порты:

- Драйвер Spark и все экзекуторы Spark -> порт `5432` на мастер-узле Greenplum.

  Этот номер порта должен быть установлен при подключении к Greenplum:

        ```python
            greenplum = Greenplum(host="master.host", port=5432, ...)
        ```

- Сегменты Greenplum -> некоторый диапазон портов (например, `41000-42000`) **прослушиваемый исполнителями Spark**.

  Этот диапазон должен быть установлен в опции `extra`:

        ```python
            greenplum = Greenplum(
                ...,
                extra={
                    "server.port": "41000-42000",
                },
            )
        ```

  Количество портов в этом диапазоне равно `количество параллельно работающих сессий Spark` * `количество параллельных соединений на сессию`.

  Количество соединений на сессию (см. ниже) обычно меньше `30` (см. выше).

  Количество сессий зависит от вашей среды:

- Для `master=local` на одном хосте может быть запущено только несколько единиц-десятков сессий, в зависимости от доступной оперативной памяти и ЦП.
- Для `master=yarn` сотни или тысячи сессий могут быть запущены одновременно, но они выполняются на разных узлах кластера, так что один порт может быть открыт на разных узлах одновременно.

Более подробную информацию можно найти в официальной документации:

- [требования к портам](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/sys_reqshtml#network-port-requirements)
- [формат значения server.port](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-sparkoptions.html#server.port)
- [устранение неполадок с портами](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-sparktroubleshooting.html#port-errors)

### Настройка хоста подключения

#### Spark с `master=k8s`

Пожалуйста, следуйте [официальной документации](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/configure.html#k8scfg)

#### Spark с `master=local`

По умолчанию, коннектор Greenplum пытается определить IP текущего хоста, а затем передает его как URL `gpfdist` сегменту Greenplum.
Это может не сработать в некоторых случаях.

Например, IP может быть определен с использованием содержимого `/etc/hosts` такого вида:

    ```text
        127.0.0.1 localhost real-host-name
    ```

    ```bash
        $ hostname -f
        localhost

        $ hostname -i
        127.0.0.1
    ```

Чтение/запись данных в Greenplum завершится с ошибкой:

    ```text
        org.postgresql.util.PSQLException: ERROR: connection with gpfdist failed for
        "gpfdist://127.0.0.1:49152/local-1709739764667/exec/driver",
        effective url: "http://127.0.0.1:49152/local-1709739764667/exec/driver":
        error code = 111 (Connection refused);  (seg3 slice1 12.34.56.78:10003 pid=123456)
    ```

Есть 2 способа исправить это:

- Явно передать IP-адрес вашего хоста коннектору:

        ```python
            import os

            # укажите здесь реальный IP-адрес хоста (доступный из сегментов GP)
            os.environ["HOST_IP"] = "192.168.1.1"

            greenplum = Greenplum(
                ...,
                extra={
                    # коннектор будет читать IP из этой переменной окружения
                    "server.hostEnv": "env.HOST_IP",
                },
                spark=spark,
            )
        ```

  Более подробную информацию можно найти в [официальной документации](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/options.html#server.hostenv).

- Обновить файл `/etc/hosts`, чтобы включить реальный IP-адрес хоста:

        ```text
        127.0.0.1 localhost
        # этот IP должен быть доступен из сегментов GP
        192.168.1.1 driver-host-name
        ```

  Таким образом, коннектор Greenplum правильно определит IP-адрес хоста.

#### Spark с `master=yarn`

Та же проблема с определением IP-адреса может возникнуть на узле кластера Hadoop, но её сложнее исправить, поскольку каждый узел имеет свой IP.

Есть 3 способа исправить это:

- Передать имя узла в URL `gpfdist`. Таким образом, IP будет определен на стороне сегмента:

        ```python
        greenplum = Greenplum(
            ...,
            extra={
                "server.useHostname": "true",
            },
        )
        ```

  Но это может не сработать, если имя узла кластера Hadoop не может быть разрешено со стороны сегмента Greenplum.

  Более подробную информацию можно найти в [официальной документации](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/options.html#server.usehostname).

- Установить конкретный сетевой интерфейс для получения IP-адреса:

        ```python
        greenplum = Greenplum(
            ...,
            extra={
                "server.nic": "eth0",
            },
        ) 
        ```

  Вы можете получить список сетевых интерфейсов с помощью этой команды.

!!! note "Примечание"

    Эта команда должна быть выполнена на узле кластера Hadoop, **а не** на хосте драйвера Spark!

    ```bash
        $ ip address
        1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 qdisc noqueue state UNKNOWN group default qlen 1000
            inet 127.0.0.1/8 scope host lo
            valid_lft forever preferred_lft forever
        2: eth0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc fq_codel state UP group default qlen 1000
            inet 192.168.1.1/24 brd 192.168.1.255 scope global dynamic noprefixroute eth0
            valid_lft 83457sec preferred_lft 83457sec
    ```

  Обратите внимание, что в этом случае **каждый** узел кластера Hadoop должен иметь сетевой интерфейс с именем `eth0`.

  Более подробную информацию можно найти в [официальной документации](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/options.html#server.nic).

- Обновить `/etc/hosts` на каждом узле кластера Hadoop, чтобы включить реальный IP узла:

        ```text
            127.0.0.1 localhost
            # этот IP должен быть доступен из сегментов GP
            192.168.1.1 cluster-node-name
        ```

  Таким образом, коннектор Greenplum правильно определит IP-адрес узла.

### Установка необходимых прав

Попросите администратора вашего кластера Greenplum установить следующие права для пользователя,
используемого для создания соединения:

=== "Чтение + Запись"

    ```sql 
        -- получить доступ к метаданным таблиц и информации о кластере
        GRANT SELECT ON information_schema.tables TO username;
        GRANT SELECT ON pg_attribute TO username;
        GRANT SELECT ON pg_class TO username;
        GRANT SELECT ON pg_namespace TO username;
        GRANT SELECT ON pg_settings TO username;
        GRANT SELECT ON pg_stats TO username;
        GRANT SELECT ON gp_distributed_xacts TO username;
        GRANT SELECT ON gp_segment_configuration TO username;
        -- Только для Greenplum 5.x
        GRANT SELECT ON gp_distribution_policy TO username;

        -- разрешить создание внешних таблиц в той же схеме, что и исходная/целевая таблица
        GRANT USAGE ON SCHEMA myschema TO username;
        GRANT CREATE ON SCHEMA myschema TO username;
        ALTER USER username CREATEEXTTABLE(type = 'readable', protocol = 'gpfdist') CREATEEXTTABLE(type = 'writable', protocol = 'gpfdist');

        -- разрешить доступ на чтение к конкретной таблице (для получения типов столбцов)
        -- разрешить доступ на запись к конкретной таблице
        GRANT SELECT, INSERT ON myschema.mytable TO username;
    ```

=== "Только чтение"

    ```sql
        -- получить доступ к метаданным таблиц и информации о кластере
        GRANT SELECT ON information_schema.tables TO username;
        GRANT SELECT ON pg_attribute TO username;
        GRANT SELECT ON pg_class TO username;
        GRANT SELECT ON pg_namespace TO username;
        GRANT SELECT ON pg_settings TO username;
        GRANT SELECT ON pg_stats TO username;
        GRANT SELECT ON gp_distributed_xacts TO username;
        GRANT SELECT ON gp_segment_configuration TO username;
        -- Только для Greenplum 5.x
        GRANT SELECT ON gp_distribution_policy TO username;

        -- разрешить создание внешних таблиц в той же схеме, что и исходная таблица
        GRANT USAGE ON SCHEMA schema_to_read TO username;
        GRANT CREATE ON SCHEMA schema_to_read TO username;
        -- да, `writable` для чтения из GP, потому что данные записываются из Greenplum в исполнитель Spark.
        ALTER USER username CREATEEXTTABLE(type = 'writable', protocol = 'gpfdist');

        -- разрешить доступ на чтение к конкретной таблице
        GRANT SELECT ON schema_to_read.table_to_read TO username;
    ```

Более подробную информацию можно найти в [официальной документации](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/install_cfg.html#role-privileges).
