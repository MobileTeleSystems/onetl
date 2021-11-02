.. include:: ../README.rst
    :end-before: documentation

.. toctree::
    :maxdepth: 2
    :caption: onETL
    :name: onetl
    :hidden:

    self

.. toctree::
    :maxdepth: 2
    :caption: How to
    :name: howto

    install
    develop
    usage

.. toctree::
    :maxdepth: 2
    :hidden:
    :caption: DB Connection
    :name: dbconnection

    db_connection/clickhouse_connection
    db_connection/hive_connection
    db_connection/mssql_connection
    db_connection/mysql_connection
    db_connection/oracle_connection
    db_connection/postgres_connection
    db_connection/teradata_connection

.. toctree::
    :maxdepth: 2
    :caption: File Connection
    :name: fileconnection

    file_connection/ftp_connection
    file_connection/ftps_connection
    file_connection/hdfs_connection
    file_connection/samba_connection
    file_connection/sftp_connection

.. toctree::
    :maxdepth: 2
    :caption: DB Reader
    :name: dbreader

    onetl.db_reader

.. toctree::
    :maxdepth: 2
    :caption: DB Writer
    :name: dbwriter

    onetl.db_writer

.. toctree::
    :maxdepth: 2
    :caption: File Downloader
    :name: filedownloader

    onetl.file_downloader

.. toctree::
    :maxdepth: 2
    :caption: File Uploader
    :name: fileuploader

    onetl.file_uploader

.. toctree::
    :maxdepth: 2
    :caption: HWM and incremental reads
    :name: hwm_incremental

    onetl.hwm
    strategy/index
    hwm_store/index
