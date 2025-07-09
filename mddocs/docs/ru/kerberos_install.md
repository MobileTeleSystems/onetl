Многие экземпляры Hadoop развернуты с поддержкой Kerberos, поэтому для правильной работы некоторых подключений требуется дополнительная настройка.

* `HDFS`
  Использует [requests-kerberos](https://pypi.org/project/requests-kerberos/) и
  [GSSApi](https://pypi.org/project/gssapi/) для аутентификации.
  Также использует исполняемый файл `kinit` для создания Kerberos-билета.
* `Hive` и `SparkHDFS`
  требуют наличия Kerberos-ticket перед созданием Spark-сессии.

Поэтому вам необходимо установить пакеты ОС с:

* `krb5` libs
* Заголовки для `krb5`
* `gcc` или другой компилятор для C-исходников

Точная инструкция по установке зависит от вашей ОС, вот несколько примеров:

```bash
apt install libkrb5-dev krb5-user gcc  # Debian-based
dnf install krb5-devel krb5-libs krb5-workstation gcc  # CentOS, OracleLinux
```

Также вам следует передать `kerberos` в `extras` для установки необходимых пакетов Python:

```bash
pip install onetl[kerberos]
```
