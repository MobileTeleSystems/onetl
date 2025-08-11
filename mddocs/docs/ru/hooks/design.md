# Высокоуровневый дизайн { #hooks-design }

## Что такое хуки?

Механизм хуков — это часть onETL, которая позволяет внедрять дополнительное поведение в существующие методы (почти) любого класса.

### Возможности

Механизм хуков позволяет:

- Проверять и валидировать входные аргументы и результаты вызова метода
- Получать доступ, изменять или заменять результат вызова метода (но НЕ входные аргументы)
- Оборачивать вызовы методов контекстным менеджером и перехватывать возникающие исключения

Хуки можно размещать в [плагинах][plugins], что позволяет изменять поведение onETL путем установки дополнительных пакетов.

### Ограничения

- Хуки могут быть привязаны только к методам класса (не к функциям).
- Только методы, декорированные с помощью [`slot-decorator`][slot-decorator], реализуют механизм хуков. Такие классы и методы помечены как `support_hooks`.
- Хуки могут быть привязаны только к публичным методам.

## Термины

- [`slot-decorator`][slot-decorator] - метод класса со специальным декоратором
- `Callback` - функция, которая реализует дополнительную логику, изменяющую поведение слота
- [`hook-decorator`][hook-decorator] - обертка вокруг функции обратного вызова, которая хранит состояние хука, приоритет и некоторые полезные методы
- `Механизм хуков` - вызов `Slot()` вызовет все включенные хуки, привязанные к слоту. Реализовано через [`support-hooks-decorator`][support-hooks-decorator].

## Как реализовать хуки?

### Краткое руководство

```python
from onetl.hooks import support_hooks, slot, hook


@support_hooks  # включение механизма хуков для класса
class MyClass:
    def __init__(self, data):
        self.data = data

    # это слот
    @slot
    def method(self, arg):
        pass


@MyClass.method.bind  # привязка хука к слоту
@hook  # это хук
def callback(obj, arg):  # это функция обратного вызова
    print(obj.data, arg)


obj = MyClass(1)
obj.method(2)  # вызовет callback(obj, 1)

# выведет "1 2"
```

#### Определение слота

- Создайте класс с методом:

```python
class MyClass:
    def __init__(self, data):
        self.data = data

    def method(self, arg):
        return self.data, arg
```

- Добавьте декоратор [`slot-decorator`][slot-decorator] к методу:

```python
from onetl.hooks import support_hooks, slot, hook


class MyClass:
    @slot
    def method(self, arg):
        return self.data, arg
```

Если метод имеет другие декораторы, такие как `@classmethod` или `@staticmethod`, `@slot` должен быть размещен сверху:

```python
from onetl.hooks import support_hooks, slot, hook


class MyClass:
    @slot
    @classmethod
    def class_method(cls, arg):
        return cls, arg

    @slot
    @staticmethod
    def static_method(arg):
        return arg
```

- Добавьте декоратор [`support-hooks-decorator`][support-hooks-decorator] к классу:

```python
from onetl.hooks import support_hooks, slot, hook


@support_hooks
class MyClass:
    @slot
    def method(self, arg):
        return self.data, arg
```

Слот создан.

#### Определение функции обратного вызова

Определите некоторую функцию (т.е. callback):

```python
def callback(self, arg):
    print(self.data, arg)
```

Она должна иметь сигнатуру, *совместимую* с `MyClass.method`. *Совместимая* не означает *в точности такую же* - например, вы можете переименовать позиционные аргументы:

```python
def callback(obj, arg):
    print(obj.data, arg)
```

Используйте `*args` и `**kwargs` для пропуска аргументов, которые вам не важны:

```python
def callback(obj, *args, **kwargs):
    print(obj.data, args, kwargs)
```

Также есть аргумент `method_name`, который имеет специальное значение - имя метода, к которому привязана функция обратного вызова, передается в этот аргумент:

```python
def callback(obj, *args, method_name: str, **kwargs):
    print(obj.data, args, method_name, kwargs)
```

!!! note

    `method_name` всегда должен быть именованным аргументом, а **НЕ** позиционным.

!!! warning

    Если сигнатура callback несовместима с сигнатурой слота, будет вызвано исключение, но **ТОЛЬКО** при вызове слота.

#### Определение хука

Добавьте декоратор [`hook-decorator`][hook-decorator] для создания хука из вашей функции обратного вызова:

```python
@hook
def callback(obj, arg):
    print(obj.data, arg)
```

Вы можете передать больше опций декоратору `@hook`, такие как состояние или приоритет.
Подробнее смотрите в документации декоратора.

#### Привязка хука к слоту

Используйте метод `Slot.bind` для привязки хука к слоту:

```python
@MyClass.method.bind
@hook
def callback(obj, arg):
    print(obj, arg)
```

Вы можете привязать более одного хука к одному слоту, и привязать один и тот же хук к нескольким слотам:

```python
@MyClass.method1.bind
@MyClass.method2.bind
@hook
def callback1(obj, arg):
    "Будет вызван как MyClass.method1, так и MyClass.method2"


@MyClass.method1.bind
@hook
def callback2(obj, arg):
    "Также будет вызван MyClass.method1"
```

## Как вызываются хуки?

### Общая информация

Просто вызовите метод, декорированный `@slot`, чтобы активировать хук:

```python
obj = MyClass(1)
obj.method(2)  # вызовет callback(obj, 2)

# выведет "1 2"
```

Есть некоторые специальные типы обратных вызовов, которые имеют несколько иное поведение.

### Контекстные менеджеры

Декоратор `@hook` может быть размещен на классе контекстного менеджера:

```python
@hook
class ContextManager:
    def __init__(self, obj, arg):
        self.obj = obj
        self.arg = arg

    def __enter__(self):
        # делаем что-то при входе
        print(obj.data, arg)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # делаем что-то при выходе
        return False
```

Контекстный менеджер входит при вызове `Slot()` и выходит, когда вызов завершается.

Если присутствует, метод `process_result` имеет особое значение - он может получать результат вызова `MyClass.method`, а также изменять/заменять его:

```python
@hook
class ContextManager:
    def __init__(self, obj, arg):
        self.obj = obj
        self.arg = arg

    def __enter__(self):
        # делаем что-то при входе
        print(obj.data, arg)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # делаем что-то при выходе
        return False

    def process_result(self, result):
        # делаем что-то с результатом вызова метода
        return modified(result)
```

Смотрите примеры ниже для получения дополнительной информации.

### Функция-генератор

Декоратор `@hook` может быть размещен на функции-генераторе:

```python
@hook
def callback(obj, arg):
    print(obj.data, arg)
    # вызывается до тела оригинального метода

    yield  # метод вызывается здесь

    # вызывается после тела оригинального метода
```

Он преобразуется в контекстный менеджер, аналогично
[contextlib.contextmanager](https://docs.python.org/3/library/contextlib.html#contextlib.contextmanager).

Тело генератора может быть обернуто в `try..except..finally` для перехвата исключений:

```python
@hook
def callback(obj, arg):
    print(obj.data, arg)

    try:
        # вызывается до тела оригинального метода

        yield  # метод вызывается здесь
    except Exception as e:
        process_exception(a)
    finally:
        # вызывается после тела оригинального метода
        finalizer()
```

Существует также специальный синтаксис, который позволяет генератору получать доступ и изменять/заменять результат вызова метода:

```python
@hook
def callback(obj, arg):
    original_result = yield  # метод вызывается здесь

    new_result = do_something(original_result)

    yield new_result  # изменить/заменить результат
```

### Вызов хуков подробно

- Функция обратного вызова будет вызвана с теми же аргументами, что и оригинальный метод.

  - Если слот является обычным методом:

    ```python
    callback_result = callback(self, *args, **kwargs)
    ```

    Здесь `self` — это экземпляр класса (`obj`).

  - Если слот является методом класса:

    ```python
    callback_result = callback(cls, *args, **kwargs)
    ```

    Здесь `cls` — это сам класс (`MyClass`).

  - Если слот является статическим методом:

    ```python
    callback_result = callback(*args, **kwargs)
    ```

    В этом случае в функцию обратного вызова не передается ни объект, ни класс.

- Если `callback_result` является контекстным менеджером, войдите в контекст. Контекстный менеджер может перехватывать все вызванные исключения.

  > Если к слоту привязано несколько хуков, каждый контекстный менеджер будет введен.

- Затем вызовите оригинальный метод, обернутый `@slot`:

  ```python
  original_result = method(*args, **kwargs)
  ```

- Обработайте `original_result`:

  - Если объект `callback_result` имеет метод `process_result` или является генератором, обернутым `@hook`, вызовите его:

    ```python
    new_result = callback_result.process_result(original_result)
    ```

  - В противном случае установите `new_result = callback_result`.

  - Если к методу привязано несколько хуков, пропустите `new_result` через цепочку:

    ```python
    new_result = callback1_result.process_result(original_result)
    new_result = callback2_result.process_result(new_result or original_result)
    new_result = callback3_result.process_result(new_result or original_result)
    ```

- Наконец, верните:

  ```python
  return new_result or original_result
  ```

  Все значения `None` игнорируются на каждом этапе выше.

- Выход из всех контекстных менеджеров, введенных во время вызова слота.

### Приоритет хуков

Хуки выполняются в следующем порядке:

1. Слот родительского класса + [`FIRST`][onetl.hooks.hook.HookPriority.FIRST]
2. Слот унаследованного класса + [`FIRST`][onetl.hooks.hook.HookPriority.FIRST]
3. Слот родительского класса + [`NORMAL`][onetl.hooks.hook.HookPriority.NORMAL]
4. Слот унаследованного класса + [`NORMAL`][onetl.hooks.hook.HookPriority.NORMAL]
5. Слот родительского класса + [`LAST`][onetl.hooks.hook.HookPriority.LAST]
6. Слот унаследованного класса + [`LAST`][onetl.hooks.hook.HookPriority.LAST]

Хуки с одинаковым приоритетом и наследованием будут выполняться в том же порядке, в котором они были зарегистрированы (вызов `Slot.bind`).

!!! note

    Вызовы `super()` внутри методов унаследованного класса не вызывают вызов хуков. Хуки вызываются только при явном вызове метода.

    Это позволяет обернуть хуком весь вызов слота, не влияя на его внутреннюю логику.

### Типы хуков

Вот несколько примеров использования хуков. Эти типы не являются исключительными, их можно смешивать — например, хук может как изменять результат метода, так и перехватывать исключения.

#### Хук до (Before hook)

Может использоваться для проверки или валидации входных аргументов оригинальной функции:

```python
@hook
def before1(obj, arg):
    print(obj, arg)
    # оригинальный метод вызывается после выхода из этой функции


@hook
def before2(obj, arg):
    if arg == 1:
        raise ValueError("arg=1 не разрешен")
    return None  # возврат None то же самое, что и отсутствие оператора return
```

Выполняется перед вызовом оригинального метода, обернутого `@slot`. Если хук вызывает исключение, метод вообще не будет вызван.

#### Хук после (After hook)

Может использоваться для выполнения некоторых действий после успешного выполнения оригинального метода:

```python
@hook
def after1(obj, arg):
    yield  # оригинальный метод вызывается здесь
    print(obj, arg)


@hook
def after2(obj, arg):
    yield None  # yield None то же самое, что и пустой yield
    if arg == 1:
        raise ValueError("arg=1 не разрешен")
```

Если оригинальный метод вызывает исключение, блок кода после `yield` не будет вызван.

#### Контекстный хук (Context hook)

Может использоваться для перехвата и обработки некоторых исключений или для определения того, что во время вызова слота не было исключения:

=== Синтаксис генератора

    ```python
      # Это то же самое, что и использование @contextlib.contextmanager

      @hook
      def context_generator(obj, arg):
          try:
              yield  # оригинальный метод вызывается здесь
              print(obj, arg)  # <-- эта строка не будет вызвана, если метод вызвал исключение
          except SomeException as e:
              magic(e)
          finally:
              finalizer()
    ```

=== Синтаксис контекстного менеджера

    ```python
      @hook
      class ContextManager:
          def __init__(self, obj, args):
              self.obj = obj
              self.args = args

          def __enter__(self):
              return self

          # оригинальный метод вызывается между __enter__ и __exit__

          def __exit__(self, exc_type, exc_value, traceback):
              result = False
              if exc_type is not None and isinstance(exc_value, SomeException):
                  magic(exc_value)
                  result = True  # подавить исключение
              else:
                  print(self.obj, self.arg)
              finalizer()
              return result
    ```

!!! note

    Контексты выходят в обратном порядке вызовов хуков. Таким образом, если какой-то хук вызвал исключение, оно будет передано в предыдущий хук, а не в следующий.

    Рекомендуется указывать правильный приоритет для хука, например [`FIRST`][onetl.hooks.hook.HookPriority.FIRST]

#### Хук замены результата (Replacing result hook)

Заменяет выходной результат оригинального метода.

Может использоваться для делегирования некоторых деталей реализации сторонним расширениям.
См. [`hive`][hive] и [`hdfs`][hdfs] в качестве примера.

```python
@hook
def replace1(obj, arg):
    result = arg + 10  # любой не None результат

    # результат вызова оригинального метода игнорируется, выход всегда будет arg + 10
    return result


@hook
def replace2(obj, arg):
    yield arg + 10  # то же, что и выше
```

!!! note

    Если к одному слоту привязано несколько хуков, будет использоваться результат последнего хука.
    Рекомендуется указывать правильный приоритет для хука, например [`LAST`][onetl.hooks.hook.HookPriority.LAST]

#### Хук доступа к результату (Accessing result hook)

Может получать доступ к выходному результату оригинального метода и проверять или валидировать его:

=== Синтаксис генератора

    ```python

        @hook
        def access_result(obj, arg):
            result = yield  # оригинальный метод вызывается здесь, и результат может использоваться в хуке
            print(result)
            yield  # не изменяет результат
    ```

=== Синтаксис контекстного менеджера

    ```python

        @hook
        class ModifiesResult:
            def __init__(self, obj, args):
                self.obj = obj
                self.args = args

            def __enter__(self):
                return self

            # оригинальный метод вызывается между __enter__ и __exit__
            # результат передается в метод process_result контекстного менеджера, если он есть

            def process_result(self, result):
                print(result)  # результат может использоваться в хуке
                return None  # не изменяет результат. то же самое, что и отсутствие оператора return в методе

            def __exit__(self, exc_type, exc_value, traceback):
                return False

    ```

#### Хук изменения результата (Modifying result hook)

Может получать доступ к выходному результату оригинального метода и возвращать измененный:

=== Синтаксис генератора

    ```python 

        @hook
        def modifies_result(obj, arg):
            result = yield  # оригинальный метод вызывается здесь, и результат может использоваться в хуке
            yield result + 10  # изменить выходной результат. Значения None игнорируются
    ```

=== Синтаксис контекстного менеджера

    ```python 

        @hook
        class ModifiesResult:
            def __init__(self, obj, args):
                self.obj = obj
                self.args = args

            def __enter__(self):
                return self

            # оригинальный метод вызывается между __enter__ и __exit__
            # результат передается в метод process_result контекстного менеджера, если он есть

            def process_result(self, result):
                print(result)  # результат может использоваться в хуке
                return result + 10  # изменить выходной результат. Значения None игнорируются

            def __exit__(self, exc_type, exc_value, traceback):
                return False
    ```

!!! note

    Если к одному слоту привязано несколько хуков, будет использоваться результат последнего хука.
    Рекомендуется указывать правильный приоритет для хука, например [`LAST`][onetl.hooks.hook.HookPriority.LAST]

## Как включить/отключить хуки?

Вы можете включить/отключить/временно отключить хуки на 4 разных уровнях:

- Управление глобальным состоянием хуков (уровень 1):

  - [`onetl.hooks.hooks_state.stop_all_hooks`][onetl.hooks.hooks_state.stop_all_hooks]
  - [`onetl.hooks.hooks_state.resume_all_hooks`][onetl.hooks.hooks_state.resume_all_hooks]
  - [`onetl.hooks.hooks_state.skip_all_hooks`][onetl.hooks.hooks_state.skip_all_hooks]

- Управление всеми хуками, привязанными к конкретному классу (уровень 2):

  - [`onetl.hooks.support_hooks.suspend_hooks`][onetl.hooks.support_hooks.suspend_hooks]
  - [`onetl.hooks.support_hooks.resume_hooks`][onetl.hooks.support_hooks.resume_hooks]
  - [`onetl.hooks.support_hooks.skip_hooks`][onetl.hooks.support_hooks.skip_hooks]

- Управление всеми хуками, привязанными к конкретному слоту (уровень 3):

  - [`onetl.hooks.slot.Slot.suspend_hooks`][onetl.hooks.slot.Slot.suspend_hooks]
  - [`onetl.hooks.slot.Slot.resume_hooks`][onetl.hooks.slot.Slot.resume_hooks]
  - [`onetl.hooks.slot.Slot.skip_hooks`][onetl.hooks.slot.Slot.skip_hooks]

- Управление состоянием конкретного хука (уровень 4):

  - [`onetl.hooks.hook.Hook.enable`][onetl.hooks.hook.Hook.enable]
  - [`onetl.hooks.hook.Hook.disable`][onetl.hooks.hook.Hook.disable]

Подробнее см. в документации выше.

!!! note

    Все эти уровни независимы.

    Вызов `stop` на уровне 1 имеет более высокий приоритет, чем уровень 2, и так далее.
    Но вызов `resume` на уровне 1 не возобновляет автоматически хуки, остановленные на уровне 2,
    их нужно возобновлять явно.

## Как увидеть логи механизма хуков?

Регистрация хуков выводит логи с уровнем `DEBUG`:

```python
from onetl.logs import setup_logging

setup_logging()
```

```text
DEBUG  |onETL| Registered hook 'mymodule.callback1' for 'MyClass.method' (enabled=True, priority=HookPriority.NORMAL)
DEBUG  |onETL| Registered hook 'mymodule.callback2' for 'MyClass.method' (enabled=True, priority=HookPriority.NORMAL)
DEBUG  |onETL| Registered hook 'mymodule.callback3' for 'MyClass.method' (enabled=False, priority=HookPriority.NORMAL)
```

Но большая часть логов выводится с еще более низким уровнем `NOTICE`, чтобы сделать вывод менее подробным:

```python
from onetl.logs import NOTICE, setup_logging

setup_logging(level=NOTICE)
```

```text
NOTICE  |Hooks| 2 hooks registered for 'MyClass.method'
NOTICE  |Hooks| Calling hook 'mymodule.callback1' (1/2)
NOTICE  |Hooks| Hook is finished with returning non-None result
NOTICE  |Hooks| Calling hook 'mymodule.callback2' (2/2)
NOTICE  |Hooks| This is a context manager, entering ...
NOTICE  |Hooks|   Calling original method 'MyClass.method'
NOTICE  |Hooks|   Method call is finished
NOTICE  |Hooks| Method call result (*NOT* None) will be replaced with result of hook 'mymodule.callback1'
NOTICE  |Hooks|   Passing result to 'process_result' method of context manager 'mymodule.callback2'
NOTICE  |Hooks|   Method call result (*NOT* None) is modified by hook!
```
