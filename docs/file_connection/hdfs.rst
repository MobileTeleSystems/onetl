.. _hdfs:

HDFS connection
===============

.. currentmodule:: onetl.connection.file_connection.hdfs

.. autosummary::

    HDFS
    HDFS.slots

.. autoclass:: HDFS
    :members: __init__, check, path_exists, is_file, is_dir, get_stat, resolve_dir, resolve_file, create_dir, remove_file, remove_dir, rename_file, list_dir, walk, download_file, upload_file

.. currentmodule:: onetl.connection.file_connection.hdfs.HDFS

.. autoclass:: slots
    :members: normalize_cluster_name, normalize_namenode_name, get_known_clusters, get_cluster_namenodes, get_current_cluster, get_webhdfs_port, is_namenode_active
    :member-order: bysource
