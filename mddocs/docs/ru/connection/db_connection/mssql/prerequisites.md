# Предварительные требования { #mssql-prerequisites }

## Совместимость версий

- Версии SQL Server:
    - Официально заявленные: 2016 - 2022
    - Фактически протестированные: 2017, 2022
- Версии Spark: 2.3.x - 3.5.x
- Версии Java: 8 - 20

См. [официальную документацию](https://learn.microsoft.com/en-us/sql/connect/jdbc/system-requirements-for-the-jdbc-driver)
и [официальную матрицу совместимости](https://learn.microsoft.com/en-us/sql/connect/jdbc/microsoft-jdbc-driver-for-sql-server-support-matrix).

## Установка PySpark

Для использования коннектора MSSQL у вас должен быть установлен PySpark (или добавлен в `sys.path`) **ДО** создания экземпляра коннектора.

См. [инструкцию по установке][install-spark] для получения дополнительной информации.

## Подключение к MSSQL

### Порт подключения

Подключение обычно выполняется через порт 1433. Порт может отличаться для разных экземпляров MSSQL.
Пожалуйста, обратитесь к администратору MSSQL для получения необходимой информации.

Для именованных экземпляров MSSQL (опция `instanceName`), [номер порта необязателен](https://learn.microsoft.com/en-us/sql/connect/jdbc/building-the-connection-url?view=sql-server-ver16#named-and-multiple-sql-server-instances) и может быть опущен.

### Хост подключения

Можно подключиться к MSSQL, используя либо DNS-имя хоста, либо его IP-адрес.

Если вы используете кластер MSSQL, в настоящее время возможно подключение только к **одному конкретному узлу**.
Подключение к нескольким узлам для балансировки нагрузки, а также автоматическое переключение на новый master/replica не поддерживаются.

### Необходимые разрешения

Попросите администратора кластера MSSQL установить следующие разрешения для пользователя,
используемого для создания соединения:

=== "Чтение + Запись (схема принадлежит пользователю)"

    ```sql 

        -- разрешить создание таблиц для пользователя
        GRANT CREATE TABLE TO username;

        -- разрешить доступ на чтение и запись к определенной таблице
        GRANT SELECT, INSERT ON username.mytable TO username;

        -- только если используется if_exists="replace_entire_table":
        -- разрешить удаление/очистку таблиц в любой схеме
        GRANT ALTER ON username.mytable TO username;
    ```

=== "Чтение + Запись (схема не принадлежит пользователю)"

    ```sql

        -- разрешить создание таблиц для пользователя
        GRANT CREATE TABLE TO username;

        -- разрешить управление таблицами в определенной схеме и вставку данных в таблицы
        GRANT ALTER, SELECT, INSERT ON SCHEMA::someschema TO username;
    ```

=== "Только чтение"

    ```sql

        -- разрешить доступ на чтение к определенной таблице
        GRANT SELECT ON someschema.mytable TO username;
    ```

Более подробную информацию можно найти в официальной документации:

- [GRANT ON DATABASE](https://learn.microsoft.com/en-us/sql/t-sql/statements/grant-database-permissions-transact-sql)
- [GRANT ON OBJECT](https://learn.microsoft.com/en-us/sql/t-sql/statements/grant-object-permissions-transact-sql)
- [GRANT ON SCHEMA](https://learn.microsoft.com/en-us/sql/t-sql/statements/grant-schema-permissions-transact-sql)