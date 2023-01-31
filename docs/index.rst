.. include:: ../README.rst
    :end-before: documentation

.. toctree::
    :maxdepth: 2
    :caption: onETL
    :hidden:

    self
    security
    contributing
    concepts
    quickstart

.. toctree::
    :maxdepth: 2
    :caption: How to
    :hidden:

    install
    develop

..
    TODO (@msmarty5): убрать блоки ниже после перехода на тему Furo.

    В теме ReadTheDocs все заголовки по-умолчанию свернуты, и переопределить это не удается
    Из-за чего документация выглядит не очень

.. toctree::
    :maxdepth: 2
    :caption: DB Connection
    :hidden:

    db_connection/clickhouse
    db_connection/greenplum
    db_connection/hive
    db_connection/mssql
    db_connection/mysql
    db_connection/oracle
    db_connection/postgres
    db_connection/teradata
    db_connection/mongodb

.. toctree::
    :maxdepth: 2
    :caption: File Connection
    :glob:
    :hidden:

    file_connection/ftp
    file_connection/ftps
    file_connection/hdfs
    file_connection/sftp
    file_connection/s3
    file_connection/webdav

.. toctree::
    :maxdepth: 2
    :caption: DB Reader
    :hidden:

    core/db_reader

.. toctree::
    :maxdepth: 2
    :caption: DB Writer
    :hidden:

    core/db_writer

.. toctree::
    :maxdepth: 2
    :caption: File Downloader
    :hidden:

    core/file_downloader
    core/download_result

.. toctree::
    :maxdepth: 2
    :caption: File Uploader
    :hidden:

    core/file_uploader
    core/upload_result

.. toctree::
    :maxdepth: 2
    :caption: File Filter
    :hidden:

    core/file_filter

.. toctree::
    :maxdepth: 2
    :caption: File Limit
    :hidden:

    core/file_limit

..
    Заменить на блок ниже
..
    .. toctree::
        :maxdepth: 2
        :caption: Connection
        :glob:
        :hidden:

        db_connection/index
        file_connection/index

    .. toctree::
        :maxdepth: 2
        :caption: Core
        :hidden:

        core/index

.. toctree::
    :maxdepth: 2
    :caption: HWM and incremental reads
    :hidden:

    hwm/index
    strategy/index
    hwm_store/index


.. toctree::
    :maxdepth: 2
    :caption: Hooks & plugins

    hooks/index
    plugins

.. toctree::
    :maxdepth: 2
    :caption: Misc

    logging
