# Предварительные требования { #spark-s3-prerequisites }

## Совместимость версий

- Версии Spark: 3.2.x - 3.5.x (только с библиотеками Hadoop 3.x)
- Версии Java: 8 - 20

## Установка PySpark

Для использования коннектора SparkS3 у вас должен быть установлен PySpark (или добавлен в `sys.path`) **ДО** создания экземпляра коннектора.

Подробнее см. в [инструкции по установке][install-spark].

## Подключение к S3

### Стиль доступа к корзине

AWS и некоторые другие облачные провайдеры S3 разрешают доступ к корзине только с использованием доменного стиля, например, `https://mybucket.s3provider.com`.

Другие реализации, такие как Minio, по умолчанию разрешают доступ только в стиле пути, например, `https://s3provider.com/mybucket` (см. [MINIO_DOMAIN](https://min.io/docs/minio/linux/reference/minio-server/minio-server.html#envvar.MINIO_DOMAIN)).

Вам следует установить для `path.style.access` значение `True` или `False`, чтобы выбрать предпочтительный стиль.

### Аутентификация

Разные экземпляры S3 могут использовать разные методы аутентификации, такие как:
  - `access_key + secret_key` (или имя пользователя + пароль)
  - `access_key + secret_key + session_token`

Обычно они просто передаются в конструктор SparkS3:

```python
SparkS3(
    access_key=...,
    secret_key=...,
    session_token=...,
)
```

Но некоторые облачные провайдеры S3, такие как AWS, могут требовать пользовательские провайдеры учетных данных. Вы можете передать их так:

```python
SparkS3(
    extra={
        # класс провайдера
        "aws.credentials.provider": "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider",
        # другие параметры, если необходимо
        "assumed.role.arn": "arn:aws:iam::90066806600238:role/s3-restricted",
    },
)
```

См. документацию [Hadoop-AWS](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Changing_Authentication_Providers).

## Устранение неполадок

См. [руководство по устранению неполадок][spark-s3-troubleshooting].
