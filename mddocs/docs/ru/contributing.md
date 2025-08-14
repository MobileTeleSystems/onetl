# Руководство по участию

Добро пожаловать! Существует множество способов внести свой вклад, включая отправку отчетов об ошибках, улучшение документации, отправку запросов на добавление функций, рецензирование новых материалов или предоставление кода, который можно включить в проект.

## Ограничения

При разработке следует придерживаться следующих пунктов:

* Некоторые компании до сих пор используют старые версии Spark, например 2.3.1. Поэтому, по возможности, необходимо сохранять совместимость, например, добавлять ветки для разных версий Spark.
* Разные пользователи используют onETL по-разному - некоторые используют только DB коннекторы, некоторые только файлы. Зависимости, специфичные для коннекторов, должны быть необязательными.
* Вместо создания классов с большим количеством различных опций, лучше разделить их на более мелкие классы, например, класс опций, контекстный менеджер и т.д., и использовать композицию.

## Начальная настройка для локальной разработки

### Установите Git

Пожалуйста, следуйте [инструкции](https://docs.github.com/en/get-started/quickstart/set-up-git).

### Создайте форк

Если вы не являетесь членом команды разработчиков, создающей onETL, вам следует создать форк, прежде чем вносить какие-либо изменения.

Пожалуйста, следуйте [инструкции](https://docs.github.com/en/get-started/quickstart/fork-a-repo).

### Клонируйте репозиторий

Откройте терминал и выполните следующие команды:

```bash
git clone git@github.com:myuser/onetl.git -b develop

cd onetl
```

### Настройте окружение

Создайте virtualenv и установите зависимости:

```bash
python -m venv venv
source venv/bin/activate
pip install -U wheel
pip install -U pip setuptools
pip install -U \
    -r requirements/core.txt \
    -r requirements/ftp.txt \
    -r requirements/hdfs.txt \
    -r requirements/kerberos.txt \
    -r requirements/s3.txt \
    -r requirements/sftp.txt \
    -r requirements/webdav.txt \
    -r requirements/dev.txt \
    -r requirements/docs.txt \
    -r requirements/tests/base.txt \
    -r requirements/tests/clickhouse.txt \
    -r requirements/tests/kafka.txt \
    -r requirements/tests/mongodb.txt \
    -r requirements/tests/mssql.txt \
    -r requirements/tests/mysql.txt \
    -r requirements/tests/postgres.txt \
    -r requirements/tests/oracle.txt \
    -r requirements/tests/pydantic-2.txt \
    -r requirements/tests/spark-3.5.5.txt

# TODO: remove after https://github.com/zqmillet/sphinx-plantuml/pull/4
pip install sphinx-plantuml --no-deps
```

### Включите pre-commit hooks

Установите pre-commit hooks:

```bash
pre-commit install --install-hooks
```

Проверьте запуск pre-commit hooks:

```bash
pre-commit run
```

## Как тестировать

### Запустите тесты локально

#### Используя docker-compose

Соберите образ для запуска тестов:

```bash
docker-compose build
```

Запустите все контейнеры с зависимостями:

```bash
docker-compose --profile all up -d
```

Вы можете запустить ограниченный набор зависимостей:

```bash
docker-compose --profile mongodb up -d
```

Запустите тесты:

```bash
docker-compose run --rm onetl ./run_tests.sh
```

Вы можете передать дополнительные аргументы, они будут переданы в pytest:

```bash
docker-compose run --rm onetl ./run_tests.sh -m mongodb -lsx -vvvv --log-cli-level=INFO
```

Вы можете запустить интерактивную bash сессию и использовать ее:

```bash
docker-compose run --rm onetl bash

./run_tests.sh -m mongodb -lsx -vvvv --log-cli-level=INFO
```

Посмотрите логи тестового контейнера:

```bash
docker-compose logs -f onetl
```

Остановите все контейнеры и удалите созданные тома:

```bash
docker-compose --profile all down -v
```

#### Без docker-compose

!!! warning

    Для локального запуска тестов HDFS необходимо добавить следующую строку в файл `/etc/hosts` (путь к файлу зависит от ОС):

    ```default
    # HDFS server returns container hostname as connection address, causing error in DNS resolution
    127.0.0.1 hdfs
    ```

!!! note

    Для запуска тестов Oracle необходимо установить [Oracle instantclient](https://www.oracle.com/database/technologies/instant-client.html)
    и передать его путь в переменные окружения `ONETL_ORA_CLIENT_PATH` и `LD_LIBRARY_PATH`,
    например, `ONETL_ORA_CLIENT_PATH=/path/to/client64/lib`.

    Также может потребоваться добавить тот же путь в переменную окружения `LD_LIBRARY_PATH`.

!!! note

    Для запуска тестов Greenplum необходимо:

    * Скачать [VMware Greenplum connector for Spark][greenplum-prerequisites]
    * Либо переместить его в `~/.ivy2/jars/`, либо передать путь к файлу в `CLASSPATH`
    * Установить переменную окружения `ONETL_GP_PACKAGE_VERSION=local`.

Запустите все контейнеры с зависимостями:

```bash
docker-compose --profile all up -d
```

Вы можете запустить ограниченный набор зависимостей:

```bash
docker-compose --profile mongodb up -d
```

Загрузите переменные окружения со свойствами подключения:

```bash
source .env.local
```

Запустите тесты:

```bash
./run_tests.sh
```

Вы можете передать дополнительные аргументы, они будут переданы в pytest:

```bash
./run_tests.sh -m mongodb -lsx -vvvv --log-cli-level=INFO
```

Остановите все контейнеры и удалите созданные тома:

```bash
docker-compose --profile all down -v
```

### Соберите документацию

Соберите документацию с помощью Sphinx:

```bash
cd docs
make html
```

Затем откройте в браузере `docs/_build/index.html`.

## Процесс рецензирования

Пожалуйста, создайте новую задачу GitHub для любых значительных изменений и улучшений, которые вы хотите внести. Укажите функцию, которую вы хотели бы видеть, зачем она вам нужна и как она будет работать. Обсуждайте свои идеи открыто и получайте отзывы сообщества, прежде чем продолжить.

Значительные изменения, которые вы хотите внести в проект, следует сначала обсудить в задаче GitHub, в которой четко изложены изменения и преимущества этой функции.

Небольшие изменения можно сразу же создать и отправить в репозиторий GitHub в виде запроса на включение (Pull Request).

### Создайте pull request

Зафиксируйте свои изменения:

```bash
git commit -m "Commit message"
git push
```

Затем откройте интерфейс Github и [создайте pull request](https://docs.github.com/en/get-started/quickstart/contributing-to-projects#making-a-pull-request).
Пожалуйста, следуйте руководству из шаблона тела PR.

После создания pull request он получает соответствующий номер, например, 123 (`pr_number`).

### Напишите release notes

`onETL` использует [towncrier](https://pypi.org/project/towncrier/)
для управления журналом изменений.

Чтобы отправить заметку об изменении для вашего PR, добавьте текстовый файл в папку [docs/changelog/next_release](./next_release). Он должен содержать объяснение того, как применение этого PR изменит способ взаимодействия конечных пользователей с проектом. Одного предложения обычно достаточно, но не стесняйтесь добавлять столько деталей, сколько считаете необходимым для того, чтобы пользователи поняли, что это значит.

**Используйте прошедшее время** для текста в вашем фрагменте, потому что, в сочетании с другими, он будет частью "сводки новостей", рассказывающей читателям **что изменилось** в конкретной версии библиотеки *с момента предыдущей версии*.

Вам также следует использовать синтаксис reStructuredText для выделения кода (встроенного или блочного), связывания частей документации или внешних сайтов. Если вы хотите подписать свое изменение, не стесняйтесь добавить `-- by :user:`github-username`` в конце (замените `github-username` на свой собственный!).

Наконец, назовите свой файл в соответствии с соглашением, которое понимает Towncrier:
он должен начинаться с номера задачи или PR, за которым следует точка,
затем добавьте тип патча, например, `feature`,
`doc`, `misc` и т.д., и добавьте `.rst` в качестве суффикса. Если вам
нужно добавить более одного фрагмента, вы можете добавить необязательный
порядковый номер (разделенный другой точкой) между типом
и суффиксом.

В общем, имя будет соответствовать шаблону `<pr_number>.<category>.rst`, где категории:

* `feature`: Любая новая функция
* `bugfix`: Исправление ошибки
* `improvement`: Улучшение
* `doc`: Изменение в документации
* `dependency`: Изменения, связанные с зависимостями
* `misc`: Внутренние изменения в репозитории, такие как CI, тесты и изменения сборки

Pull request может иметь более одного из этих компонентов, например, изменение кода может ввести новую функцию, которая устаревает старую функцию, в этом случае следует добавить два фрагмента. Нет необходимо делать отдельный фрагмент документации для изменений документации, сопровождающих соответствующие изменения кода.

#### Примеры добавления записей в журнал изменений в ваши Pull Requests

```rst
Added a ``:github:user:`` role to Sphinx config -- by :github:user:`someuser`
```

```rst
Fixed behavior of ``WebDAV`` connector -- by :github:user:`someuser`
```

```rst
Added support of ``timeout`` in ``S3`` connector
-- by :github:user:`someuser`, :github:user:`anotheruser` and :github:user:`otheruser`
```

#### Как пропустить проверку заметок об изменениях?

Просто добавьте метку `ci:skip-changelog` к pull request.

#### Процесс релиза

Перед тем, как сделать релиз из ветки `develop`, выполните следующие шаги:

1. Перейдите в ветку `develop` и обновите ее до актуального состояния

```bash
git checkout develop
git pull -p
```

1. Сделайте резервную копию `NEXT_RELEASE.rst`

```bash
cp "docs/changelog/NEXT_RELEASE.rst" "docs/changelog/temp_NEXT_RELEASE.rst"
```

1. Соберите Release notes с помощью Towncrier

```bash
VERSION=$(cat onetl/VERSION)
towncrier build "--version=${VERSION}" --yes
```

1. Измените файл с журналом изменений на номер версии релиза

```bash
mv docs/changelog/NEXT_RELEASE.rst "docs/changelog/${VERSION}.rst"
```

1. Удалите содержимое над заголовком номера версии в файле `${VERSION}.rst`

```bash
awk '!/^.*towncrier release notes start/' "docs/changelog/${VERSION}.rst" > temp && mv temp "docs/changelog/${VERSION}.rst"
```

1. Обновите индекс журнала изменений

```bash
awk -v version=${VERSION} '/DRAFT/{print;print "    " version;next}1' docs/changelog/index.rst > temp && mv temp docs/changelog/index.rst
```

1. Восстановите файл `NEXT_RELEASE.rst` из резервной копии

```bash
mv "docs/changelog/temp_NEXT_RELEASE.rst" "docs/changelog/NEXT_RELEASE.rst"
```

1. Зафиксируйте и отправьте изменения в ветку `develop`

```bash
git add .
git commit -m "Prepare for release ${VERSION}"
git push
```

1. Слейте ветку `develop` в `master`, **БЕЗ** squashing

```bash
git checkout master
git pull
git merge develop
git push
```

1. Добавьте git tag к последнему коммиту в ветке `master`

```bash
git tag "$VERSION"
git push origin "$VERSION"
```

1. Обновите версию в ветке `develop` **после релиза**:

```bash
git checkout develop

NEXT_VERSION=$(echo "$VERSION" | awk -F. '/[0-9]+\./{$NF++;print}' OFS=.)
echo "$NEXT_VERSION" > onetl/VERSION

git add .
git commit -m "Bump version"
git push
```
