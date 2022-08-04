.. _download-result:

Download result
==============

.. currentmodule:: onetl.core.file_downloader.download_result

Generic methods:

.. autosummary::

    DownloadResult
    DownloadResult.total_count
    DownloadResult.total_size
    DownloadResult.is_empty
    DownloadResult.raise_if_empty
    DownloadResult.details
    DownloadResult.summary
    DownloadResult.dict
    DownloadResult.json

Successful files:

.. autosummary::

    DownloadResult.successful
    DownloadResult.successful_count
    DownloadResult.successful_size
    DownloadResult.raise_if_contains_zero_size

Failed files:

.. autosummary::

    DownloadResult.failed
    DownloadResult.failed_count
    DownloadResult.failed_size
    DownloadResult.raise_if_failed

Skipped files:

.. autosummary::

    DownloadResult.skipped
    DownloadResult.skipped_count
    DownloadResult.skipped_size
    DownloadResult.raise_if_skipped

Missing files:

.. autosummary::

    DownloadResult.missing
    DownloadResult.missing_count
    DownloadResult.raise_if_missing

.. autoclass:: DownloadResult
    :members: successful, failed, skipped, missing, successful_count, failed_count, skipped_count, missing_count, total_count, successful_size, failed_size, skipped_size, total_size, raise_if_failed, reraise_failed, raise_if_missing, raise_if_skipped, raise_if_empty, is_empty, raise_if_contains_zero_size, details, summary, dict, json
