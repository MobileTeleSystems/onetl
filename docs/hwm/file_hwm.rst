.. _file-hwm:

File HWM
========

What is HWM?
-------------

Sometimes it's necessary to read/download only new files from a source folder.

For example, there is a folder with files:

.. code:: bash

    $ hdfs dfs -ls /path

    2MB 2023-09-09 10:13 /path/my/file123
    4Mb 2023-09-09 10:15 /path/my/file234

When new file is being added to this folder:

.. code:: bash

    $ hdfs dfs -ls /path

    2MB 2023-09-09 10:13 /path/my/file123
    4Mb 2023-09-09 10:15 /path/my/file234
    5Mb 2023-09-09 10:20 /path/my/file345  # new one

To download only new files, if is required to somehow track them, and then filter using the information
from a previous run.

This technique is called ``High WaterMark`` or ``HWM`` for short.
It is used by different :ref:`strategy` to implement some complex logic
of filtering source data.


Supported types
---------------

There are a several ways to track HWM value:

    * Save the entire file list, and then select only files not present in this list
      (``file_list``)
    * Save max modified time of all files, and then select only files with ``modified_time``
      higher than this value
    * If file name contains some incrementing value, e.g. id or datetime,
      parse it and save max value of all files, then select only files with higher value
    * and so on

Currently the only HWM type implemented for files is ``file_list``. Other ones can be implemented on-demand

See strategies and :ref:`file-downloader` documentation for examples.
