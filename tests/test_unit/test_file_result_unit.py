import re
import textwrap
from pathlib import Path, PurePath

import pytest
from ordered_set import OrderedSet

from onetl.core import FileResult, FileSet
from onetl.exception import FileResultError
from onetl.impl import FailedRemoteFile, RemoteFile, RemoteFileStat


def test_file_result_success():
    success = [
        RemoteFile(path="/success1", stats=RemoteFileStat(st_size=10 * 1024, st_mtime=50)),
        RemoteFile(path="/success1", stats=RemoteFileStat(st_size=10 * 1024, st_mtime=50)),
        RemoteFile(path="/success2", stats=RemoteFileStat(st_size=20 * 1024, st_mtime=50)),
        Path("cannot/detect/size1"),
    ]

    file_result = FileResult(success=success)

    assert file_result.success
    assert isinstance(file_result.success, FileSet)
    assert file_result.success == FileSet(success)

    assert len(file_result.success) == 3
    assert file_result.success_count == 3
    assert file_result.success_size == (10 + 20) * 1024

    details = """
        Successful 3 file(s) (30.7 kB):
            /success1 (10.2 kB)
            /success2 (20.5 kB)
            cannot/detect/size1 (? Bytes)
    """

    assert textwrap.dedent(details).strip() in file_result.details
    assert "Successful 3 file(s) (30.7 kB)" in str(file_result)


def test_file_result_raise_if_no_success():
    success = [
        RemoteFile(path="/empty", stats=RemoteFileStat(st_size=0, st_mtime=50)),
        RemoteFile(path="/success", stats=RemoteFileStat(st_size=10 * 1024, st_mtime=50)),
        Path("just/deleted"),
    ]

    assert not FileResult(success=success).raise_if_no_success()

    with pytest.raises(FileResultError, match="There are no successful files in the result"):
        FileResult().raise_if_no_success()


def test_file_result_raise_if_zero_size():
    success = [
        RemoteFile(path="/empty", stats=RemoteFileStat(st_size=0, st_mtime=50)),
        RemoteFile(path="/success", stats=RemoteFileStat(st_size=10 * 1024, st_mtime=50)),
        Path("cannot/detect/size1"),  # missing file does not mean zero size
    ]

    details = """
        1 file(s) out of 3 have zero size:
            /empty
    """

    error_message = re.escape(textwrap.dedent(details).strip())

    with pytest.raises(FileResultError, match=error_message):
        FileResult(success=success).raise_if_zero_size()

    # empty success files does not mean zero files size
    assert not FileResult().raise_if_zero_size()


def test_file_result_failed():
    failed = [
        FailedRemoteFile(
            path="/failed1",
            stats=RemoteFileStat(st_size=11 * 1024 * 1024, st_mtime=50),
            exception=FileExistsError("abc"),
        ),
        FailedRemoteFile(
            path="/failed2",
            stats=RemoteFileStat(st_size=22 * 1024 * 1024, st_mtime=50),
            exception=FileExistsError("cde\ndef"),
        ),
        Path("cannot/detect/size2"),
    ]

    file_result = FileResult(failed=failed)

    assert file_result.failed
    assert isinstance(file_result.failed, FileSet)
    assert file_result.failed == FileSet(failed)

    assert len(file_result.failed) == 3
    assert file_result.failed_count == 3
    assert file_result.failed_size == (11 + 22) * 1024 * 1024

    details = """
        Failed 3 file(s) (34.6 MB):
            /failed1 (11.5 MB) FileExistsError('abc')
            /failed2 (23.1 MB) FileExistsError('cde
                                def')
            cannot/detect/size2 (? Bytes)
    """

    assert textwrap.dedent(details).strip() in file_result.details
    assert "Failed 3 file(s) (34.6 MB)" in str(file_result)


def test_file_result_raise_if_failed():
    failed = [
        FailedRemoteFile(
            path="/failed1",
            stats=RemoteFileStat(st_size=11 * 1024 * 1024, st_mtime=50),
            exception=FileExistsError("abc"),
        ),
        FailedRemoteFile(
            path="/failed2",
            stats=RemoteFileStat(st_size=22 * 1024 * 1024, st_mtime=50),
            exception=FileExistsError("cde\ndef"),
        ),
        Path("cannot/detect/size2"),
    ]

    details = """
        Failed 3 file(s) (34.6 MB):
            /failed1 (11.5 MB) FileExistsError('abc')
            /failed2 (23.1 MB) FileExistsError('cde
                                def')
            cannot/detect/size2 (? Bytes)
    """

    error_message = re.escape(textwrap.dedent(details).strip())

    with pytest.raises(FileResultError, match=error_message):
        FileResult(failed=failed).raise_if_failed()

    assert not FileResult().raise_if_failed()


def test_file_result_skipped():
    skipped = [
        RemoteFile(path="/skipped1", stats=RemoteFileStat(st_size=12 * 1024 * 1024 * 1024, st_mtime=50)),
        RemoteFile(path="/skipped1", stats=RemoteFileStat(st_size=12 * 1024 * 1024 * 1024, st_mtime=50)),
        Path("cannot/detect/size3"),
    ]

    file_result = FileResult(skipped=skipped)

    assert file_result.skipped

    assert isinstance(file_result.skipped, FileSet)
    assert file_result.skipped == FileSet(skipped)

    assert len(file_result.skipped) == 2
    assert file_result.skipped_count == 2
    assert file_result.skipped_size == 12 * 1024 * 1024 * 1024

    details = """
        Skipped 2 file(s) (12.9 GB):
            /skipped1 (12.9 GB)
            cannot/detect/size3 (? Bytes)
    """

    assert textwrap.dedent(details).strip() in file_result.details
    assert "Skipped 2 file(s) (12.9 GB)" in str(file_result)


def test_file_result_raise_if_skipped():
    skipped = [
        RemoteFile(path="/skipped1", stats=RemoteFileStat(st_size=12 * 1024 * 1024 * 1024, st_mtime=50)),
        Path("cannot/detect/size3"),
    ]

    details = """
        Skipped 2 file(s) (12.9 GB):
            /skipped1 (12.9 GB)
            cannot/detect/size3 (? Bytes)
    """

    error_message = re.escape(textwrap.dedent(details).strip())

    with pytest.raises(FileResultError, match=error_message):
        FileResult(skipped=skipped).raise_if_skipped()

    assert not FileResult().raise_if_skipped()


def test_file_result_missing():
    missing = [
        PurePath("/missing1"),
        PurePath("/missing2"),
        Path("missing"),
    ]

    file_result = FileResult(missing=missing)

    assert file_result.missing

    assert isinstance(file_result.missing, OrderedSet)
    assert file_result.missing == OrderedSet(missing)

    assert len(file_result.missing) == 3
    assert file_result.missing_count == 3

    details = """
        Missing 3 file(s):
            /missing1
            /missing2
            missing
    """

    assert textwrap.dedent(details).strip() in file_result.details
    assert "Missing 3 file(s)" in str(file_result)


def test_file_result_raise_if_missing():
    missing = [
        PurePath("/missing1"),
        PurePath("/missing2"),
        Path("missing"),
    ]

    details = """
        Missing 3 file(s):
            /missing1
            /missing2
            missing
    """

    error_message = re.escape(textwrap.dedent(details).strip())

    with pytest.raises(FileResultError, match=error_message):
        FileResult(missing=missing).raise_if_missing()

    assert not FileResult().raise_if_missing()


def test_file_result_empty():
    file_result = FileResult()

    assert not file_result.success
    assert not file_result.failed
    assert not file_result.skipped
    assert not file_result.missing

    assert file_result.success_count == 0
    assert file_result.failed_count == 0
    assert file_result.skipped_count == 0
    assert file_result.missing_count == 0

    assert file_result.success_size == 0
    assert file_result.failed_size == 0
    assert file_result.skipped_size == 0

    assert str(file_result) == "No files"

    details = """
        No successful files

        No failed files

        No skipped files

        No missing files
    """

    assert textwrap.dedent(details).strip() == file_result.details


def test_file_result_raise_if_empty():
    assert not FileResult(
        success=[RemoteFile(path="/success", stats=RemoteFileStat(st_size=10 * 1024, st_mtime=50))],
    ).raise_if_empty()

    assert not FileResult(
        failed=[
            FailedRemoteFile(
                path="/failed1",
                stats=RemoteFileStat(st_size=11 * 1024 * 1024, st_mtime=50),
                exception=FileExistsError("abc"),
            ),
        ],
    ).raise_if_empty()

    assert not FileResult(
        skipped=[RemoteFile(path="/skipped1", stats=RemoteFileStat(st_size=12 * 1024 * 1024 * 1024, st_mtime=50))],
    ).raise_if_empty()

    with pytest.raises(FileResultError, match="There are no files in the result"):
        FileResult().raise_if_empty()

    with pytest.raises(FileResultError, match="There are no files in the result"):
        FileResult(missing=[Path("missing/file")]).raise_if_empty()


def test_file_result_total():
    file_result = FileResult(
        success={
            RemoteFile(path="/success1", stats=RemoteFileStat(st_size=1024 * 1024 * 1024, st_mtime=50)),
        },
        failed={
            FailedRemoteFile(
                path="/failed1",
                stats=RemoteFileStat(st_size=2 * 1024 * 1024 * 1024, st_mtime=50),
                exception=FileExistsError("abc"),
            ),
        },
        skipped={
            RemoteFile(path="/skipped1", stats=RemoteFileStat(st_size=3 * 1024 * 1024 * 1024, st_mtime=50)),
        },
        missing={PurePath("/missing1")},
    )

    assert file_result.total_count == 4
    assert file_result.total_size == 6 * 1024 * 1024 * 1024

    total = """
        Total 4 file(s) (6.4 GB)
        Successful 1 file(s) (1.1 GB)
        Failed 1 file(s) (2.1 GB)
        Skipped 1 file(s) (3.2 GB)
        Missing 1 file(s)
    """

    assert textwrap.dedent(total).strip() == str(file_result)

    details = """
        Total 4 file(s) (6.4 GB)

        Successful: /success1 (1.1 GB)

        Failed: /failed1 (2.1 GB) FileExistsError('abc')

        Skipped: /skipped1 (3.2 GB)

        Missing: /missing1
    """

    assert textwrap.dedent(details).strip() == file_result.details
