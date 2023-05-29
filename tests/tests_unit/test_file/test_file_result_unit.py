import re
import textwrap
from pathlib import PurePath

import pytest

from onetl.exception import (
    EmptyFilesError,
    FailedFilesError,
    MissingFilesError,
    SkippedFilesError,
    ZeroFileSizeError,
)
from onetl.file.file_result import FileResult
from onetl.file.file_set import FileSet
from onetl.impl import FailedRemoteFile, LocalPath, RemoteFile, RemotePathStat


def test_file_result_deprecated_import():
    msg = textwrap.dedent(
        """
        This import is deprecated since v0.8.0:

            from onetl.core import FileResult

        Please use instead:

            from onetl.file.file_result import FileResult
        """,
    )
    with pytest.warns(UserWarning, match=re.escape(msg)):
        from onetl.core import FileResult as OldFileResult

        assert OldFileResult is FileResult


def test_file_result_successful():
    successful = [
        RemoteFile(path="/successful1", stats=RemotePathStat(st_size=10 * 1024, st_mtime=50)),
        RemoteFile(path="/successful1", stats=RemotePathStat(st_size=10 * 1024, st_mtime=50)),
        RemoteFile(path="/successful2", stats=RemotePathStat(st_size=20 * 1024, st_mtime=50)),
        LocalPath("cannot/detect/size1"),
    ]

    file_result = FileResult(successful=successful)

    assert file_result.successful
    assert not file_result.is_empty

    assert isinstance(file_result.successful, FileSet)
    assert file_result.successful == FileSet(successful)

    assert len(file_result.successful) == 3
    assert file_result.successful_count == 3
    assert file_result.successful_size == (10 + 20) * 1024

    details = """
        Successful 3 files (size='30.7 kB'):
            '/successful1' (size='10.2 kB')
            '/successful2' (size='20.5 kB')
            'cannot/detect/size1'
    """

    assert textwrap.dedent(details).strip() in file_result.details
    assert textwrap.dedent(details).strip() in str(file_result)
    assert "Successful: 3 files (size='30.7 kB')" in file_result.summary


def test_file_result_raise_if_contains_zero_size():
    successful = [
        RemoteFile(path="/empty", stats=RemotePathStat(st_size=0, st_mtime=50)),
        RemoteFile(path="/successful", stats=RemotePathStat(st_size=10 * 1024, st_mtime=50)),
        LocalPath("cannot/detect/size1"),  # missing file does not mean zero size
    ]

    details = """
        1 file out of 3 have zero size:
            '/empty'
    """

    error_message = re.escape(textwrap.dedent(details).strip())

    with pytest.raises(ZeroFileSizeError, match=error_message):
        FileResult(successful=successful).raise_if_contains_zero_size()

    # empty file set does not mean zero files size
    assert not FileResult().raise_if_contains_zero_size()


def test_file_result_failed():
    failed = [
        FailedRemoteFile(
            path="/failed1",
            stats=RemotePathStat(st_size=11 * 1024 * 1024, st_mtime=50),
            exception=FileExistsError("abc"),
        ),
        FailedRemoteFile(
            path="/failed2",
            stats=RemotePathStat(st_size=22 * 1024 * 1024, st_mtime=50),
            exception=FileExistsError("cde\ndef"),
        ),
        LocalPath("cannot/detect/size2"),
    ]

    file_result = FileResult(failed=failed)

    assert file_result.failed
    assert not file_result.is_empty

    assert isinstance(file_result.failed, FileSet)
    assert file_result.failed == FileSet(failed)

    assert len(file_result.failed) == 3
    assert file_result.failed_count == 3
    assert file_result.failed_size == (11 + 22) * 1024 * 1024

    details = """
        Failed 3 files (size='34.6 MB'):
            '/failed1' (size='11.5 MB')
                FileExistsError('abc')

            '/failed2' (size='23.1 MB')
                FileExistsError('cde
                def')

            'cannot/detect/size2'
    """

    assert textwrap.dedent(details).strip() in file_result.details
    assert textwrap.dedent(details).strip() in str(file_result)
    assert "Failed: 3 files (size='34.6 MB')" in file_result.summary


def test_file_result_raise_if_failed():
    failed = [
        FailedRemoteFile(
            path="/failed1",
            stats=RemotePathStat(st_size=11 * 1024 * 1024, st_mtime=50),
            exception=FileExistsError("abc"),
        ),
        FailedRemoteFile(
            path="/failed2",
            stats=RemotePathStat(st_size=22 * 1024 * 1024, st_mtime=50),
            exception=FileExistsError("cde\ndef"),
        ),
        LocalPath("cannot/detect/size2"),
    ]

    details = """
        Failed 3 files (size='34.6 MB'):
            '/failed1' (size='11.5 MB')
                FileExistsError('abc')

            '/failed2' (size='23.1 MB')
                FileExistsError('cde
                def')

            'cannot/detect/size2'
    """

    error_message = re.escape(textwrap.dedent(details).strip())

    with pytest.raises(FailedFilesError, match=error_message):
        FileResult(failed=failed).raise_if_failed()

    assert not FileResult().raise_if_failed()


def test_file_result_skipped():
    skipped = [
        RemoteFile(path="/skipped1", stats=RemotePathStat(st_size=12 * 1024 * 1024 * 1024, st_mtime=50)),
        RemoteFile(path="/skipped1", stats=RemotePathStat(st_size=12 * 1024 * 1024 * 1024, st_mtime=50)),
        LocalPath("cannot/detect/size3"),
    ]

    file_result = FileResult(skipped=skipped)

    assert file_result.skipped
    assert not file_result.is_empty

    assert isinstance(file_result.skipped, FileSet)
    assert file_result.skipped == FileSet(skipped)

    assert len(file_result.skipped) == 2
    assert file_result.skipped_count == 2
    assert file_result.skipped_size == 12 * 1024 * 1024 * 1024

    details = """
        Skipped 2 files (size='12.9 GB'):
            '/skipped1' (size='12.9 GB')
            'cannot/detect/size3'
    """

    assert textwrap.dedent(details).strip() in file_result.details
    assert textwrap.dedent(details).strip() in str(file_result)
    assert "Skipped: 2 files (size='12.9 GB')" in file_result.summary


def test_file_result_raise_if_skipped():
    skipped = [
        RemoteFile(path="/skipped1", stats=RemotePathStat(st_size=12 * 1024 * 1024 * 1024, st_mtime=50)),
        LocalPath("cannot/detect/size3"),
    ]

    details = """
        Skipped 2 files (size='12.9 GB'):
            '/skipped1' (size='12.9 GB')
            'cannot/detect/size3'
    """

    error_message = re.escape(textwrap.dedent(details).strip())

    with pytest.raises(SkippedFilesError, match=error_message):
        FileResult(skipped=skipped).raise_if_skipped()

    assert not FileResult().raise_if_skipped()


def test_file_result_missing():
    missing = [
        PurePath("/missing1"),
        PurePath("/missing2"),
        LocalPath("cannot/detect/size4"),
    ]

    file_result = FileResult(missing=missing)

    assert file_result.missing
    assert file_result.is_empty

    assert isinstance(file_result.missing, FileSet)
    assert file_result.missing == FileSet(missing)

    assert len(file_result.missing) == 3
    assert file_result.missing_count == 3

    details = """
        Missing 3 files:
            '/missing1'
            '/missing2'
            'cannot/detect/size4'
    """

    assert textwrap.dedent(details).strip() in file_result.details
    assert textwrap.dedent(details).strip() in str(file_result)
    assert "Missing: 3 files" in file_result.summary


def test_file_result_raise_if_missing():
    missing = [
        PurePath("/missing1"),
        PurePath("/missing2"),
        LocalPath("cannot/detect/size4"),
    ]

    details = """
        Missing 3 files:
            '/missing1'
            '/missing2'
            'cannot/detect/size4'
    """

    error_message = re.escape(textwrap.dedent(details).strip())

    with pytest.raises(MissingFilesError, match=error_message):
        FileResult(missing=missing).raise_if_missing()

    assert not FileResult().raise_if_missing()


def test_file_result_empty():
    file_result = FileResult()

    assert not file_result.successful
    assert not file_result.failed
    assert not file_result.skipped
    assert not file_result.missing
    assert file_result.is_empty

    assert file_result.successful_count == 0
    assert file_result.failed_count == 0
    assert file_result.skipped_count == 0
    assert file_result.missing_count == 0

    assert file_result.successful_size == 0
    assert file_result.failed_size == 0
    assert file_result.skipped_size == 0

    assert file_result.summary == "No files"

    details = """
        No successful files

        No failed files

        No skipped files

        No missing files
    """

    assert textwrap.dedent(details).strip() == file_result.details == str(file_result)


def test_file_result_raise_if_empty():
    assert not FileResult(
        successful=[RemoteFile(path="/successful", stats=RemotePathStat(st_size=10 * 1024, st_mtime=50))],
    ).raise_if_empty()

    assert not FileResult(
        failed=[
            FailedRemoteFile(
                path="/failed1",
                stats=RemotePathStat(st_size=11 * 1024 * 1024, st_mtime=50),
                exception=FileExistsError("abc"),
            ),
        ],
    ).raise_if_empty()

    assert not FileResult(
        skipped=[RemoteFile(path="/skipped1", stats=RemotePathStat(st_size=12 * 1024 * 1024 * 1024, st_mtime=50))],
    ).raise_if_empty()

    with pytest.raises(EmptyFilesError, match="There are no files in the result"):
        FileResult().raise_if_empty()

    with pytest.raises(EmptyFilesError, match="There are no files in the result"):
        FileResult(missing=[PurePath("missing/file")]).raise_if_empty()


def test_file_result_total():
    file_result = FileResult(
        successful={
            RemoteFile(path="/successful1", stats=RemotePathStat(st_size=1024 * 1024 * 1024, st_mtime=50)),
        },
        failed={
            FailedRemoteFile(
                path="/failed1",
                stats=RemotePathStat(st_size=2 * 1024 * 1024 * 1024, st_mtime=50),
                exception=FileExistsError("abc"),
            ),
        },
        skipped={
            RemoteFile(path="/skipped1", stats=RemotePathStat(st_size=3 * 1024 * 1024 * 1024, st_mtime=50)),
        },
        missing={PurePath("/missing1")},
    )

    assert file_result.total_count == 4
    assert file_result.total_size == 6 * 1024 * 1024 * 1024

    summary = """
        Total: 4 files (size='6.4 GB')
        Successful: 1 file (size='1.1 GB')
        Failed: 1 file (size='2.1 GB')
        Skipped: 1 file (size='3.2 GB')
        Missing: 1 file
    """

    assert textwrap.dedent(summary).strip() == file_result.summary

    details = """
        Total: 4 files (size='6.4 GB')

        Successful 1 file (size='1.1 GB'):
            '/successful1' (size='1.1 GB')

        Failed 1 file (size='2.1 GB'):
            '/failed1' (size='2.1 GB')
                FileExistsError('abc')

        Skipped 1 file (size='3.2 GB'):
            '/skipped1' (size='3.2 GB')

        Missing 1 file:
            '/missing1'
    """

    assert textwrap.dedent(details).strip() == file_result.details == str(file_result)
