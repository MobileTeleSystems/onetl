# Плагины { #plugins }

:octicons-versions-16: **добавлено в версии 0.6.0**


## Что такое плагины?

### Термины

- `Plugin` - Python пакет, который реализует дополнительную функциональность для onETL, например [hooks][hooks]
- `Plugin autoimport` - поведение onETL, которое позволяет автоматически импортировать этот пакет, если он содержит правильные метаданные (`entry_points`)

### Функциональность

Механизм плагинов позволяет:

- Автоматически регистрировать [hooks][hooks], которые могут изменять поведение onETL
- Автоматически регистрировать новые классы, такие как тип HWM, хранилища HWM и т.д.

### Ограничения

В отличие от других проектов (таких как *Airflow 1.x*), плагины не внедряют импортированные классы или функции в пространство имен `onetl.*`.
Пользователи должны импортировать классы из пакета плагина **явно**, чтобы избежать конфликтов имен.

## Как реализовать плагин?

Создайте Python пакет `some-plugin` с файлом `some_plugin/setup.py`:

```python
# some_plugin/setup.py
from setuptools import setup

setup(
    # если вы хотите импортировать что-то из onETL, добавьте это в список requirements
    install_requires=["onetl"],
    entry_points={
        # этот ключ включает функциональность автоимпорта плагинов
        "onetl.plugins": [
            "some-plugin-name=some_plugin.module",  # автоматически импортировать все содержимое модуля
            "some-plugin-class=some_plugin.module.internals:MyClass",  # импортировать определенный класс
            "some-plugin-function=some_plugin.module.internals:my_function",  # импортировать определенную функцию
        ],
    },
)
```

См. [документацию setuptools для entry_points](https://setuptools.pypa.io/en/latest/userguide/entry_point.html)

## Как импортируются плагины?

- Пользователь должен установить пакет, реализующий плагин:

```bash
pip install some-package
```

- Затем пользователь должен импортировать что-то из модуля `onetl` или его подмодулей:

```python
import onetl
from onetl.connection import SomeConnection

# и так далее
```

- Этот импорт автоматически выполняет что-то вроде:

```python
import some_plugin.module
from some_plugin.module.internals import MyClass
from some_plugin.module.internals import my_function
```

Если конкретный модуль/класс/функция использует некоторые возможности регистрации onETL, например [декоратор `@hook`][hook-decorator], он будет выполнен во время этого импорта.

## Как включить/отключить плагины?

:octicons-versions-16: **добавлено в версии  0.7.0**

### Отключить/включить все плагины

По умолчанию плагины включены.

Чтобы отключить их, вы можете установить переменную окружения `ONETL_PLUGINS_ENABLED` в `false` ДО импорта onETL. Это отключит весь автоимпорт плагинов.

Но пользователь по-прежнему сможет явно импортировать `some_plugin.module`, выполняя все декораторы и возможности регистрации onETL.

### Отключить конкретный плагин (черный список)

Если какой-то плагин не работает во время импорта, вы можете отключить его, установив переменную окружения `ONETL_PLUGINS_BLACKLIST=some-failing-plugin`. Несколько имен плагинов можно передать используя `,` в качестве разделителя.

Опять же, эту переменную окружения следует установить ДО импорта onETL.

### Отключить все плагины, кроме конкретного (белый список)

Вы также можете отключить все плагины, кроме конкретного, установив переменную окружения
`ONETL_PLUGINS_WHITELIST=some-not-failing-plugin`. Несколько имен плагинов можно передать через `,` в качестве разделителя.

Опять же, эту переменную окружения следует установить ДО импорта onETL.

Если установлены обе переменные окружения, белый и черный списки, черный список имеет более высокий приоритет.

## Как увидеть логи механизма плагинов?

Регистрация плагинов выдает логи с уровнем `INFO`:

```python
import logging

logging.basicConfig(level=logging.INFO)
```

```text
INFO   |onETL| Found 2 plugins
INFO   |onETL| Loading plugin 'my-plugin'
INFO   |onETL| Skipping plugin 'failing' because it is in a blacklist
```

Более подробные логи выдаются с уровнем `DEBUG`, чтобы сделать вывод менее многословным:

```python
import logging

logging.basicConfig(level=logging.DEBUG)
```

```text
DEBUG  |onETL| Searching for plugins with group 'onetl.plugins'
DEBUG  |Plugins| Plugins whitelist: []
DEBUG  |Plugins| Plugins blacklist: ['failing-plugin']
INFO   |Plugins| Found 2 plugins
INFO   |onETL| Loading plugin (1/2):
DEBUG            name: 'my-plugin'
DEBUG            package: 'my-package'
DEBUG            version: '0.1.0'
DEBUG            importing: 'my_package.my_module:MyClass'
DEBUG  |onETL| Successfully loaded plugin 'my-plugin'
DEBUG            source: '/usr/lib/python3.11/site-packages/my_package/my_module/my_class.py'
INFO   |onETL| Skipping plugin 'failing' because it is in a blacklist
```
