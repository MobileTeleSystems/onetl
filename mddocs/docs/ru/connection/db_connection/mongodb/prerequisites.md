# Предварительные требования { #mongodb-prerequisites }

## Совместимость версий

- Версии сервера MongoDB:
  - Официально заявленные: 4.0 или выше
  - Фактически протестированные: 4.0.0, 8.0.4
- Версии Spark: 3.2.x - 3.5.x
- Версии Java: 8 - 20

См. [официальную документацию](https://www.mongodb.com/docs/spark-connector/).

## Установка PySpark

Для использования коннектора MongoDB у вас должен быть установлен PySpark (или добавлен в `sys.path`) **ДО** создания экземпляра коннектора.

Подробности см. в [инструкции по установке][install-spark].

## Подключение к MongoDB { #mongodb-connection-0 }

### Хост подключения

Возможно подключение к хосту MongoDB как по DNS-имени хоста, так и по его IP-адресу.

Также возможно подключение к общему кластеру MongoDB:

    ```python
        mongo = MongoDB(
            host="master.host.or.ip",
            user="user",
            password="*****",
            database="target_database",
            spark=spark,
            extra={
                # чтение данных с вторичного узла кластера, переключение на первичный, если вторичный недоступен
                "readPreference": "secondaryPreferred",
            },
        )
    ```

Поддерживаемые значения `readPreference` описаны в [официальной документации](https://www.mongodb.com/docs/manual/core/read-preference/).

### Порт подключения

Подключение обычно выполняется к порту `27017`. Порт может отличаться для разных экземпляров MongoDB.
Пожалуйста, уточните необходимую информацию у администратора MongoDB.

### Необходимые разрешения

Попросите администратора кластера MongoDB установить следующие разрешения для пользователя, используемого для создания подключения:

=== "Чтение + Запись"

    ```js
    // разрешить запись данных в определенную базу данных
    db.grantRolesToUser("username", [{db: "somedb", role: "readWrite"}])
    ```

=== "Только чтение"

    ```js
    // разрешить чтение данных из определенной базы данных
    db.grantRolesToUser("username", [{db: "somedb", role: "read"}])
    ```

См. также:

- [Документация по db.grantRolesToUser](https://www.mongodb.com/docs/manual/reference/method/db.grantRolesToUser)
- [Встроенные роли MongoDB](https://www.mongodb.com/docs/manual/reference/built-in-roles)
