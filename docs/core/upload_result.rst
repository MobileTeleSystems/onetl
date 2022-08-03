.. _upload-result:

Upload result
==============

.. currentmodule:: onetl.core.file_uploader.upload_result

Generic methods:

.. autosummary::

    UploadResult
    UploadResult.total_count
    UploadResult.total_size
    UploadResult.is_empty
    UploadResult.raise_if_empty
    UploadResult.details
    UploadResult.summary
    UploadResult.dict
    UploadResult.json

Successful files:

.. autosummary::

    UploadResult.successful
    UploadResult.successful_count
    UploadResult.successful_size
    UploadResult.raise_if_contains_zero_size

Failed files:

.. autosummary::

    UploadResult.failed
    UploadResult.failed_count
    UploadResult.failed_size
    UploadResult.raise_if_failed

Skipped files:

.. autosummary::

    UploadResult.skipped
    UploadResult.skipped_count
    UploadResult.skipped_size
    UploadResult.raise_if_skipped

Missing files:

.. autosummary::

    UploadResult.missing
    UploadResult.missing_count
    UploadResult.raise_if_missing

.. autoclass:: UploadResult
    :members: successful, failed, skipped, missing, successful_count, failed_count, skipped_count, missing_count, total_count, successful_size, failed_size, skipped_size, total_size, raise_if_failed, reraise_failed, raise_if_missing, raise_if_skipped, raise_if_empty, is_empty, raise_if_contains_zero_size, details, summary, dict, json
