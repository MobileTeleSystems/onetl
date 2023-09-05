Contributing Guide
==================

Welcome! There are many ways to contribute, including submitting bug
reports, improving documentation, submitting feature requests, reviewing
new submissions, or contributing code that can be incorporated into the
project.

Limitations
-----------

We should keep close to these items during development:

* Some companies still use old Spark versions, like 2.3.0. So it is required to keep compatibility if possible, e.g. adding branches for different Spark versions.
* Different users uses onETL in different ways - some uses only DB connectors, some only files. Connector-specific dependencies should be optional.
* Instead of creating classes with a lot of different options, prefer splitting them into smaller classes, e.g. options class, context manager, etc, and using composition.

Initial setup for local development
-----------------------------------

Install Git
~~~~~~~~~~~

Please follow `instruction <https://docs.github.com/en/get-started/quickstart/set-up-git>`_.

Create a fork
~~~~~~~~~~~~~

If you are not a member of a development team building onETL, you should create a fork before making any changes.

Please follow `instruction <https://docs.github.com/en/get-started/quickstart/fork-a-repo>`_.

Clone the repo
~~~~~~~~~~~~~~

Open terminal and run these commands:

.. code:: bash

    git clone git@github.com:myuser/onetl.git -b develop

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
        -r requirements/tests/kafka.txt \
        -r requirements/tests/mongodb.txt \
        -r requirements/tests/mssql.txt \
        -r requirements/tests/mysql.txt \
        -r requirements/tests/postgres.txt \
        -r requirements/tests/oracle.txt \
        -r requirements/tests/spark-3.4.1.txt

Enable pre-commit hooks
~~~~~~~~~~~~~~~~~~~~~~~

Install pre-commit hooks:

.. code:: bash

    pre-commit install --install-hooks

Test pre-commit hooks run:

.. code:: bash

    pre-commit run

How to
------

Run tests locally
~~~~~~~~~~~~~~~~~

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

Without docker-compose
^^^^^^^^^^^^^^^^^^^^^^

.. warning::

    To run HDFS tests locally you should add the following line to your ``/etc/hosts`` (file path depends on OS):

    .. code::

        # HDFS server returns container hostname as connection address, causing error in DNS resolution
        127.0.0.1 hdfs

.. note::

    To run Oracle tests you need to install `Oracle instantclient <https://www.oracle.com/database/technologies/instant-client.html>`__,
    and pass its path to ``ONETL_ORA_CLIENT_PATH`` and ``LD_LIBRARY_PATH`` environment variables,
    e.g. ``ONETL_ORA_CLIENT_PATH=/path/to/client64/lib``.

    It may also require to add the same path into ``LD_LIBRARY_PATH`` environment variable

.. note::

    To run Greenplum tests, you should:

    * Download `Pivotal connector for Spark <https://onetl.readthedocs.io/en/latest/connection/db_connection/greenplum/prerequisites.html>`_
    * Either move it to ``~/.ivy2/jars/``, or pass file path to ``CLASSPATH``
    * Set environment variable ``ONETL_DB_WITH_GREENPLUM=true`` to enable adding connector to Spark session

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


Build documentation
~~~~~~~~~~~~~~~~~~~

Build documentation using Sphinx:

.. code:: bash

    cd docs
    make html

Then open in browser ``docs/_build/index.html``.


Review process
--------------

Please create a new GitHub issue for any significant changes and
enhancements that you wish to make. Provide the feature you would like
to see, why you need it, and how it will work. Discuss your ideas
transparently and get community feedback before proceeding.

Significant Changes that you wish to contribute to the project should be
discussed first in a GitHub issue that clearly outlines the changes and
benefits of the feature.

Small Changes can directly be crafted and submitted to the GitHub
Repository as a Pull Request.

Create pull request
~~~~~~~~~~~~~~~~~~~

Commit your changes:

.. code:: bash

    git commit -m "Commit message"
    git push

Then open Github interface and `create pull request <https://docs.github.com/en/get-started/quickstart/contributing-to-projects#making-a-pull-request>`_.
Please follow guide from PR body template.

After pull request is created, it get a corresponding number, e.g. 123 (``pr_number``).

Write release notes
~~~~~~~~~~~~~~~~~~~

``onETL`` uses `towncrier <https://pypi.org/project/towncrier/>`_
for changelog management.

To submit a change note about your PR, add a text file into the
`docs/changelog/next_release <./next_release>`_ folder. It should contain an
explanation of what applying this PR will change in the way
end-users interact with the project. One sentence is usually
enough but feel free to add as many details as you feel necessary
for the users to understand what it means.

**Use the past tense** for the text in your fragment because,
combined with others, it will be a part of the "news digest"
telling the readers **what changed** in a specific version of
the library *since the previous version*.

You should also use
reStructuredText syntax for highlighting code (inline or block),
linking parts of the docs or external sites.
If you wish to sign your change, feel free to add ``-- by
:user:`github-username``` at the end (replace ``github-username``
with your own!).

Finally, name your file following the convention that Towncrier
understands: it should start with the number of an issue or a
PR followed by a dot, then add a patch type, like ``feature``,
``doc``, ``misc`` etc., and add ``.rst`` as a suffix. If you
need to add more than one fragment, you may add an optional
sequence number (delimited with another period) between the type
and the suffix.

In general the name will follow ``<pr_number>.<category>.rst`` pattern,
where the categories are:

- ``feature``: Any new feature
- ``bugfix``: A bug fix
- ``improvement``: An improvement
- ``doc``: A change to the documentation
- ``dependency``: Dependency-related changes
- ``misc``: Changes internal to the repo like CI, test and build changes

A pull request may have more than one of these components, for example
a code change may introduce a new feature that deprecates an old
feature, in which case two fragments should be added. It is not
necessary to make a separate documentation fragment for documentation
changes accompanying the relevant code changes.

Examples for adding changelog entries to your Pull Requests
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: rst
    :caption: docs/changelog/next_release/1234.doc.1.rst

    Added a ``:github:user:`` role to Sphinx config -- by :github:user:`someuser`

.. code-block:: rst
    :caption: docs/changelog/next_release/2345.bugfix.rst

    Fixed behavior of ``WebDAV`` connector -- by :github:user:`someuser`

.. code-block:: rst
    :caption: docs/changelog/next_release/3456.feature.rst

    Added support of ``timeout`` in ``S3`` connector
    -- by :github:user:`someuser`, :github:user:`anotheruser` and :github:user:`otheruser`

.. tip::

    See `pyproject.toml <../../pyproject.toml>`_ for all available categories
    (``tool.towncrier.type``).

.. _Towncrier philosophy:
    https://towncrier.readthedocs.io/en/stable/#philosophy


Examples for adding changelog entries to your Pull Requests
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: rst
    :caption: docs/changelog/next_release/1234.doc.1.rst

    Added a ``:github:user:`` role to Sphinx config -- by :github:user:`someuser`

.. code-block:: rst
    :caption: docs/changelog/next_release/2345.bugfix.rst

    Fixed behavior of ``WebDAV`` connector -- by :github:user:`someuser`

.. code-block:: rst
    :caption: docs/changelog/next_release/3456.feature.rst

    Added support of ``timeout`` in ``S3`` connector
    -- by :github:user:`someuser`, :github:user:`anotheruser` and :github:user:`otheruser`

.. tip::

    See `pyproject.toml <../../pyproject.toml>`_ for all available categories
    (``tool.towncrier.type``).

.. _Towncrier philosophy:
    https://towncrier.readthedocs.io/en/stable/#philosophy

How to skip change notes check?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Just add ``ci:skip-changelog`` label to pull request.
