.. _readme:

onETL
=====

|Repo Status| |PyPI| |PyPI License| |PyPI Python Version|
|Documentation| |Build Status| |Coverage|

.. |Repo Status| image:: https://www.repostatus.org/badges/latest/active.svg
    :target: https://github.com/MobileTeleSystems/onetl
.. |PyPI| image:: https://img.shields.io/pypi/v/onetl
    :target: https://pypi.org/project/onetl/
.. |PyPI License| image:: https://img.shields.io/pypi/l/onetl.svg
    :target: https://github.com/MobileTeleSystems/onetl/blob/develop/LICENSE.txt
.. |PyPI Python Version| image:: https://img.shields.io/pypi/pyversions/onetl.svg
    :target: https://badge.fury.io/py/onetl
.. |Documentation| image:: https://readthedocs.org/projects/onetl/badge/?version=stable
    :target: https://onetl.readthedocs.io/
.. |Build Status| image:: https://github.com/MobileTeleSystems/onetl/workflows/Tests/badge.svg
    :target: https://github.com/MobileTeleSystems/onetl/actions
.. |Coverage| image:: https://codecov.io/gh/MobileTeleSystems/onetl/branch/develop/graph/badge.svg?token=RIO8URKNZJ
    :target: https://codecov.io/gh/MobileTeleSystems/onetl

|Logo|

.. |Logo| image:: docs/static/logo_wide.svg
    :alt: onETL logo
    :target: https://github.com/MobileTeleSystems/onetl

What is onETL?
--------------

Python ETL/ELT framework powered by `Apache Spark <https://spark.apache.org/>`_ & other open-source tools.

* Provides unified classes to extract data from (**E**) & load data to (**L**) various stores.
* Relies on `Spark DataFrame API <https://spark.apache.org/docs/3.2.0/api/python/reference/api/pyspark.sql.DataFrame.html>`_ for performing transformations (**T**) in terms of *ETL*.
* Provides direct assess to database, allowing to execute SQL queries, as well as DDL, DML, and call functions/procedures. This can be used for building up *ELT* pipelines.
* Supports different `read strategies <https://onetl.readthedocs.io/en/stable/strategy/index.html>`_ for incremental and batch data fetching.
* Provides hooks & plugins mechanism for altering behavior of internal classes.

Requirements
------------
* **Python 3.7 - 3.11**
* PySpark 2.3.x - 3.4.x (depends on used connector)
* Java 8+ (required by Spark, see below)
* Kerberos libs & GCC (required by ``Hive`` and ``HDFS`` connectors)

Supported storages
------------------

+------------+------------+----------------------------------------------------------------------------------------------------------+
| Type       | Storage    | Powered by                                                                                               |
+============+============+==========================================================================================================+
| Database   | Clickhouse | Apache Spark `JDBC Data Source <https://spark.apache.org/docs/3.4.0/sql-data-sources-jdbc.html>`_        |
+            +------------+                                                                                                          +
|            | MSSQL      |                                                                                                          |
+            +------------+                                                                                                          +
|            | MySQL      |                                                                                                          |
+            +------------+                                                                                                          +
|            | Postgres   |                                                                                                          |
+            +------------+                                                                                                          +
|            | Oracle     |                                                                                                          |
+            +------------+                                                                                                          +
|            | Teradata   |                                                                                                          |
+            +------------+----------------------------------------------------------------------------------------------------------+
|            | Hive       | Apache Spark `Hive integration <https://spark.apache.org/docs/3.4.0/sql-data-sources-hive-tables.html>`_ |
+            +------------+----------------------------------------------------------------------------------------------------------+
|            | Greenplum  | Pivotal `Greenplum Spark connector <https://network.tanzu.vmware.com/products/vmware-tanzu-greenplum>`_  |
+            +------------+----------------------------------------------------------------------------------------------------------+
|            | MongoDB    | `MongoDB Spark connector <https://www.mongodb.com/docs/spark-connector/current>`_                        |
+------------+------------+----------------------------------------------------------------------------------------------------------+
| File       | HDFS       | `HDFS Python client <https://pypi.org/project/hdfs/>`_                                                   |
+            +------------+----------------------------------------------------------------------------------------------------------+
|            | S3         | `minio-py client <https://pypi.org/project/minio/>`_                                                     |
+            +------------+----------------------------------------------------------------------------------------------------------+
|            | SFTP       | `Paramiko library <https://pypi.org/project/paramiko/>`_                                                 |
+            +------------+----------------------------------------------------------------------------------------------------------+
|            | FTP        | `FTPUtil library <https://pypi.org/project/ftputil/>`_                                                   |
+            +------------+                                                                                                          +
|            | FTPS       |                                                                                                          |
+            +------------+----------------------------------------------------------------------------------------------------------+
|            | WebDAV     | `WebdavClient3 library <https://pypi.org/project/webdavclient3/>`_                                       |
+------------+------------+----------------------------------------------------------------------------------------------------------+


.. documentation

Documentation
-------------

See https://onetl.readthedocs.io/

.. contribution

Contribution guide
-------------------

See `<CONTRIBUTING.rst>`__

.. security

Security
-------------------

See `<SECURITY.rst>`__


.. install

How to install
---------------

.. _minimal-install:

Minimal installation
~~~~~~~~~~~~~~~~~~~~

Base ``onetl`` package contains:

* ``DBReader``, ``DBWriter`` and related classes
* ``FileDownloader``, ``FileUploader``, ``FileFilter``, ``FileLimit`` and related classes
* Read Strategies & HWM classes
* Plugins support

It can be installed via:

.. code:: bash

    pip install onetl

.. warning::

    This method does NOT include any connections.

    This method is recommended for use in third-party libraries which require for ``onetl`` to be installed,
    but do not use its connection classes.

.. _spark-install:

With DB connections
~~~~~~~~~~~~~~~~~~~

All DB connection classes (``Clickhouse``, ``Greenplum``, ``Hive`` and others) requires PySpark to be installed.

Firstly, you should install JDK. The exact installation instruction depends on your OS, here are some examples:

.. code:: bash

    yum install java-1.8.0-openjdk-devel  # CentOS 7 + Spark 2
    dnf install java-11-openjdk-devel  # CentOS 8 + Spark 3
    apt-get install openjdk-11-jdk  # Debian-based + Spark 3

.. _spark-compatibility-matrix:

Compatibility matrix
^^^^^^^^^^^^^^^^^^^^

+--------------------------------------------------------------+-------------+-------------+-------+
| Spark                                                        | Python      | Java        | Scala |
+==============================================================+=============+=============+=======+
| `2.3.x <https://spark.apache.org/docs/2.3.0/#downloading>`_  | 3.7 only    | 8 only      | 2.11  |
+--------------------------------------------------------------+-------------+-------------+-------+
| `2.4.x <https://spark.apache.org/docs/2.4.8/#downloading>`_  | 3.7 only    | 8 only      | 2.11  |
+--------------------------------------------------------------+-------------+-------------+-------+
| `3.2.x <https://spark.apache.org/docs/3.2.3/#downloading>`_  | 3.7 - 3.10  | 8u201 - 11  | 2.12  |
+--------------------------------------------------------------+-------------+-------------+-------+
| `3.3.x <https://spark.apache.org/docs/3.3.2/#downloading>`_  | 3.7 - 3.10  | 8u201 - 17  | 2.12  |
+--------------------------------------------------------------+-------------+-------------+-------+
| `3.4.x <https://spark.apache.org/docs/3.4.0/#downloading>`_  | 3.7 - 3.11  | 8u362 - 17  | 2.12  |
+--------------------------------------------------------------+-------------+-------------+-------+

Then you should install PySpark via passing ``spark`` to ``extras``:

.. code:: bash

    pip install onetl[spark]  # install latest PySpark

or install PySpark explicitly:

.. code:: bash

    pip install onetl pyspark==3.4.0  # install a specific PySpark version

or inject PySpark to ``sys.path`` in some other way BEFORE creating a class instance.
**Otherwise class import will fail.**


.. _files-install:

With file connections
~~~~~~~~~~~~~~~~~~~~~

All file connection classes (``FTP``,  ``SFTP``, ``HDFS`` and so on) requires specific Python clients to be installed.

Each client can be installed explicitly by passing connector name (in lowercase) to ``extras``:

.. code:: bash

    pip install onetl[ftp]  # specific connector
    pip install onetl[ftp,ftps,sftp,hdfs,s3,webdav]  # multiple connectors

To install all file connectors at once you can pass ``files`` to ``extras``:

.. code:: bash

    pip install onetl[files]

**Otherwise class import will fail.**


.. _kerberos-install:

With Kerberos support
~~~~~~~~~~~~~~~~~~~~~

Most of Hadoop instances set up with Kerberos support,
so some connections require additional setup to work properly.

* ``HDFS``
    Uses `requests-kerberos <https://pypi.org/project/requests-kerberos/>`_ and
    `GSSApi <https://pypi.org/project/gssapi/>`_ for authentication in WebHDFS.
    It also uses ``kinit`` executable to generate Kerberos ticket.

* ``Hive``
    Requires Kerberos ticket to exist before creating Spark session.

So you need to install OS packages with:

    * ``krb5`` libs
    * Headers for ``krb5``
    * ``gcc`` or other compiler for C sources

The exact installation instruction depends on your OS, here are some examples:

.. code:: bash

    dnf install krb5-devel gcc  # CentOS, OracleLinux
    apt install libkrb5-dev gcc  # Debian-based

Also you should pass ``kerberos`` to ``extras`` to install required Python packages:

.. code:: bash

    pip install onetl[kerberos]


.. _full-install:

Full bundle
~~~~~~~~~~~

To install all connectors and dependencies, you can pass ``all`` into ``extras``:

.. code:: bash

    pip install onetl[all]

    # this is just the same as
    pip install onetl[spark,files,kerberos]

.. warning::

    This method consumes a lot of disk space, and requires for Java & Kerberos libraries to be installed into your OS.

.. develops

Develop
-------

Clone repo
~~~~~~~~~~

Clone repo:

.. code:: bash

    git clone git@github.com:MobileTeleSystems/onetl.git -b develop

    cd onetl

Setup environment
~~~~~~~~~~~~~~~~~

Create virtualenv and install dependencies:

.. code:: bash

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
        -r requirements/tests/postgres.txt \
        -r requirements/tests/mongodb.txt \
        -r requirements/tests/mssql.txt \
        -r requirements/tests/mysql.txt \
        -r requirements/tests/oracle.txt \
        -r requirements/tests/postgres.txt \
        -r requirements/tests/spark-3.4.0.txt

Enable pre-commit hooks
~~~~~~~~~~~~~~~~~~~~~~~

Install pre-commit hooks:

.. code:: bash

    pre-commit install --install-hooks

Test pre-commit hooks run:

.. code:: bash

    pre-commit run

.. tests

Tests
~~~~~

Using docker-compose
^^^^^^^^^^^^^^^^^^^^

Build image for running tests:

.. code:: bash

    docker-compose build

Start all containers with dependencies:

.. code:: bash

    docker-compose up -d

You can run limited set of dependencies:

.. code:: bash

    docker-compose up -d mongodb

Run tests:

.. code:: bash

    docker-compose run --rm onetl ./run_tests.sh

You can pass additional arguments, they will be passed to pytest:

.. code:: bash

    docker-compose run --rm onetl ./run_tests.sh -m mongodb -lsx -vvvv --log-cli-level=INFO

You can run interactive bash session and use it:

.. code:: bash

    docker-compose run --rm onetl bash

    ./run_tests.sh -m mongodb -lsx -vvvv --log-cli-level=INFO

See logs of test container:

.. code:: bash

    docker-compose logs -f onetl

Stop all containers and remove created volumes:

.. code:: bash

    docker-compose down -v

Run tests locally
^^^^^^^^^^^^^^^^^

.. warning::

    To run HDFS tests locally you should add the following line to your ``/etc/hosts`` (file path depends on OS):

    .. code::

        127.0.0.1 hdfs

.. note::

    To run Oracle tests you need to install `Oracle instantclient <https://www.oracle.com/database/technologies/instant-client.html>`__,
    and pass its path to ``ONETL_ORA_CLIENT_PATH`` environment variable, e.g. ``ONETL_ORA_CLIENT_PATH=/path/to/client64/lib``.

    It may also require to add the same path into ``LD_LIBRARY_PATH`` environment variable

.. note::

    To run Greenplum tests, you should:

    * Download `Pivotal connector for Spark <https://onetl.org.readthedocs.build/en/latest/db_connection/greenplum/prerequisites.html>`_
    * Either move it to ``~/.ivy2/jars/``, or pass file path to ``CLASSPATH``
    * Set environment variable ``ONETL_DB_WITH_GREENPLUM=true`` to enable adding connector to Spark session

Build image for running tests:

.. code:: bash

    docker-compose build

Start all containers with dependencies:

.. code:: bash

    docker-compose up -d

You can run limited set of dependencies:

.. code:: bash

    docker-compose up -d mongodb

Load environment variables with connection properties:

.. code:: bash

    source .env.local

Run tests:

.. code:: bash

    ./run_tests.sh

You can pass additional arguments, they will be passed to pytest:

.. code:: bash

    ./run_tests.sh -m mongodb -lsx -vvvv --log-cli-level=INFO

Stop all containers and remove created volumes:

.. code:: bash

    docker-compose down -v
