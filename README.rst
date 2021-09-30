Building
~~~~~~~~
Docker должен быть настроен в соответствии с "Работа с Python внутри Docker"
(https://wiki.bd.msk.mts.ru/pages/viewpage.action?pageId=42960827).

При билдинге на винде нужно **убедиться, что в .sh файлы в ./docker не пролезли CRLF**\ , иначе
entrypoint.sh могут не взлететь. Для гарантии можно прописать в **.gitconfig**\ :

.. code-block::

   [core]
       filemode = false
       autocrlf = input

Билд:

.. code-block:: bash

    docker build -t onetl -f ./docker/Dockerfile .

    docker system prune --volumes

С этого момента станет доступен образ **onetl**.

IDE (PyCharm)
~~~~~~~~~~~~~

Settings:

1. Project Interpreter -> Add -> Docker -> Image name onetl:latest

2. Project Structure:
   - Пометить onetl, как **Sources**.

Run -> Edit Configurations -> New -> pytest:
0. Name **Test All**.

1. Script path **tests**.

2. Additional Arguments **--verbose -s -c pytest.ini**.

3. Python interpreter **Project Default (onetl:latest). Нужно выбрать Python interpreter path: python3**.

4. Working directory /opt/project

5. Add content roots and source roots -- **галки необязательны**.

6. Docker container settings:

   1. Network mode **onetl** (то есть нетворк из docker-compose.yml) or  Add --net onetl in Run options

   2. Volume bindings (container -> local):
      - **/opt/project -> (absolute path to)/onetl** (PyCharm должен сделать это сам).

Run -> Edit Configurations -> Copy Configuration **Test All**:

Дополнительно можно создать конфигурации под конкретные тесты.


Testing
~~~~~~~~

.. code-block:: bash

    docker-compose down

    docker system prune --volumes

    docker-compose up

Ждем, пока инициализируется окружение и сервисы.


С этого момента можно запустить всё что есть конфигурацией **Test All**.


PS, **есть проблема с .pyc файлами, которую пока что неясно, как решить**. На
текущий момент при локальной разработке проще всего нажать правой кнопкой на
директорию с проектом и сделать **Clean Python Compiled Files**.
