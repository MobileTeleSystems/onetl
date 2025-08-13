# Предварительные требования { #clickhouse-prerequisites }

## Совместимость версий

- Версии сервера Clickhouse:
  - Официально заявлено: 22.8 или выше
  - Фактически протестировано: 21.1, 25.1
- Версии Spark: 2.3.x - 3.5.x
- Версии Java: 8 - 20

См. [официальную документацию](https://clickhouse.com/docs/en/integrations/java#jdbc-driver).

## Установка PySpark

Для использования коннектора Clickhouse необходимо иметь установленный PySpark (или добавленный в `sys.path`) **ДО** создания экземпляра коннектора.

См. [инструкцию по установке][install-spark] для получения более подробной информации.

## Подключение к Clickhouse { #clickhouse-connection-0 }

### Порт подключения

Коннектор может использовать только протоколы **HTTP** (обычно порт `8123`) или **HTTPS** (обычно порт `8443`).

Протоколы TCP и GRPC **НЕ** поддерживаются.

### Подключение к кластеру

Возможно подключение к кластеру Clickhouse и использование его возможностей балансировки нагрузки для параллельного чтения или записи данных.
Каждый executor Spark может подключаться к случайным узлам Clickhouse, вместо отправки всех данных на узел, указанный в параметрах подключения.

Для этого требуется, чтобы все серверы Clickhouse работали на разных хостах и **прослушивали один и тот же HTTP-порт**.
Установите `auto_discovery=True`, чтобы включить эту функцию (по умолчанию отключена):

    ```python
    Clickhouse(
        host="node1.of.cluster",
        port=8123,
        extra={
            "auto_discovery": True,
            "load_balancing_policy": "roundRobin",
        },
    )
    ```

См. [официальную документацию](https://clickhouse.com/docs/en/integrations/java#configuring-node-discovery-load-balancing-and-failover).

### Необходимые права

Попросите администратора кластера Clickhouse установить следующие права для пользователя,
используемого для создания подключения:

=== "Чтение + Запись"

    ```sql 

        -- разрешить создание таблиц в целевой схеме
        GRANT CREATE TABLE ON myschema.* TO username;

        -- разрешить чтение и запись в определенную таблицу
        GRANT SELECT, INSERT ON myschema.mytable TO username;
    ```

=== "Только чтение"

    ```sql

        -- разрешить чтение определенной таблицы
        GRANT SELECT ON myschema.mytable TO username;
    ```

Более подробную информацию можно найти в [официальной документации](https://clickhouse.com/docs/en/sql-reference/statements/grant).
