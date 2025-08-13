# Предварительные требования { #oracle-prerequisites }

## Совместимость версий

- Версии Oracle Server:
  - Официально заявленные: 19c, 21c, 23ai
  - Фактически протестированные: 11.2, 23.5
- Версии Spark: 2.3.x - 3.5.x
- Версии Java: 8 - 20

См. [официальную документацию](https://www.oracle.com/cis/database/technologies/appdev/jdbc-downloads.html).

## Установка PySpark

Для использования коннектора Oracle необходимо установить PySpark (или добавить его в `sys.path`) **ДО** создания экземпляра коннектора.

См. [инструкцию по установке][install-spark] для получения более подробной информации.

## Подключение к Oracle { #oracle-connection-0 }

### Порт подключения

Подключение обычно выполняется к порту 1521. Порт может отличаться для разных экземпляров Oracle.
Пожалуйста, обратитесь к вашему администратору Oracle для получения необходимой информации.

### Хост подключения

Подключение к Oracle возможно как по DNS-имени хоста, так и по его IP-адресу.

Если вы используете кластер Oracle, в настоящее время возможно подключение только к **одному конкретному узлу**.
Подключение к нескольким узлам для балансировки нагрузки, а также автоматическое переключение на новый мастер/реплику не поддерживаются.

### Подключение через прокси-пользователя

Возможно подключение к базе данных от имени другого пользователя без знания его пароля.

Это можно включить, предоставив пользователю специальное разрешение `CONNECT THROUGH`:

    ```sql
        ALTER USER schema_owner GRANT CONNECT THROUGH proxy_user;
    ```

Затем вы можете подключиться к Oracle, используя учетные данные `proxy_user`, но указать, что вам нужны разрешения `schema_owner`:

    ```python
        oracle = Oracle(
            ...,
            user="proxy_user[schema_owner]",
            password="пароль proxy_user",
        )
    ```

См. [официальную документацию](https://oracle-base.com/articles/misc/proxy-users-and-connect-through).

### Необходимые разрешения

Попросите администратора вашего кластера Oracle установить следующие разрешения для пользователя,
используемого для создания соединения:

=== "Чтение + Запись (схема принадлежит пользователю)"

    ```sql 

        -- разрешить пользователю входить в систему
        GRANT CREATE SESSION TO username;

        -- разрешить создание таблиц в схеме пользователя
        GRANT CREATE TABLE TO username;

        -- разрешить доступ на чтение и запись к конкретной таблице
        GRANT SELECT, INSERT ON username.mytable TO username;
    ```

=== "Чтение + Запись (схема не принадлежит пользователю)"

    ```sql 

        -- разрешить пользователю входить в систему
        GRANT CREATE SESSION TO username;

        -- разрешить создание таблиц в любой схеме,
        -- так как Oracle не поддерживает указание точного имени схемы
        GRANT CREATE ANY TABLE TO username;

        -- разрешить доступ на чтение и запись к конкретной таблице
        GRANT SELECT, INSERT ON someschema.mytable TO username;

        -- только если используется if_exists="replace_entire_table":
        -- разрешить удаление/очистку таблиц в любой схеме,
        -- так как Oracle не поддерживает указание точного имени схемы
        GRANT DROP ANY TABLE TO username;
    ```

=== "Только чтение"

    ```sql 

        -- разрешить пользователю входить в систему
        GRANT CREATE SESSION TO username;

        -- разрешить доступ на чтение к конкретной таблице
        GRANT SELECT ON someschema.mytable TO username;
    ```

Более подробную информацию можно найти в официальной документации:

- [GRANT](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/GRANT.html)
- [SELECT](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/SELECT.html)
- [CREATE TABLE](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/SELECT.html)
- [INSERT](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/INSERT.html)
- [TRUNCATE TABLE](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/TRUNCATE-TABLE.html)
