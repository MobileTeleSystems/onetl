from pathlib import Path, PurePath

from onetl.core.file_result.file_result import FileResult
from onetl.core.file_result.file_set import FileSet
from onetl.impl import FailedRemoteFile, RemoteFile, RemoteFileStat


def test_file_result_success():
    success = [
        RemoteFile(path="/success1", stats=RemoteFileStat(st_size=10 * 1024, st_mtime=50)),
        RemoteFile(path="/success1", stats=RemoteFileStat(st_size=10 * 1024, st_mtime=50)),
        RemoteFile(path="/success2", stats=RemoteFileStat(st_size=20 * 1024, st_mtime=50)),
        Path("just/deleted"),
    ]

    file_result = FileResult(success=success)

    assert file_result.success
    assert isinstance(file_result.success, FileSet)
    assert file_result.success == FileSet(success)

    assert len(file_result.success) == 3
    assert file_result.success_count == 3
    assert file_result.total_count == 3

    assert file_result.success_size == (10 + 20) * 1024
    assert file_result.total_size == (10 + 20) * 1024

    assert "Successful: 3 file(s) 30.7 kB" in str(file_result)

    assert "Total:" not in str(file_result)
    assert "No files" not in str(file_result)


def test_file_resul_failed():
    failed = [
        FailedRemoteFile(
            path="/failed1",
            stats=RemoteFileStat(st_size=11 * 1024 * 1024, st_mtime=50),
            exception=FileExistsError("abc"),
        ),
        FailedRemoteFile(
            path="/failed2",
            stats=RemoteFileStat(st_size=22 * 1024 * 1024, st_mtime=50),
            exception=FileExistsError("cde"),
        ),
        FailedRemoteFile(
            path="/failed3",
            stats=RemoteFileStat(st_size=33 * 1024 * 1024, st_mtime=50),
            exception=FileExistsError("def"),
        ),
        Path("cannot/detect/size1"),
    ]

    file_result = FileResult(failed=failed)

    assert file_result.failed
    assert isinstance(file_result.failed, FileSet)
    assert file_result.failed == FileSet(failed)

    assert len(file_result.failed) == 4
    assert file_result.failed_count == 4
    assert file_result.total_count == 4

    assert file_result.failed_size == (11 + 22 + 33) * 1024 * 1024
    assert file_result.total_size == (11 + 22 + 33) * 1024 * 1024

    assert "Failed: 4 file(s) 69.2 MB" in str(file_result)

    assert "Total:" not in str(file_result)
    assert "No files" not in str(file_result)


def test_file_result_skipped():
    skipped = [
        RemoteFile(path="/skipped1", stats=RemoteFileStat(st_size=12 * 1024 * 1024 * 1024, st_mtime=50)),
        RemoteFile(path="/skipped1", stats=RemoteFileStat(st_size=12 * 1024 * 1024 * 1024, st_mtime=50)),
        Path("cannot/detect/size2"),
    ]

    file_result = FileResult(skipped=skipped)

    assert file_result.skipped

    assert isinstance(file_result.skipped, FileSet)
    assert file_result.skipped == FileSet(skipped)

    assert len(file_result.skipped) == 2
    assert file_result.skipped_count == 2
    assert file_result.total_count == 2

    assert file_result.skipped_size == 12 * 1024 * 1024 * 1024
    assert file_result.total_size == 12 * 1024 * 1024 * 1024

    assert "Skipped: 2 file(s) 12.9 GB" in str(file_result)

    assert "Total:" not in str(file_result)
    assert "No files" not in str(file_result)


def test_file_result_missing():
    missing = [
        PurePath("/missing1"),
        PurePath("/missing2"),
        Path("missing"),
    ]

    file_result = FileResult(missing=missing)

    assert file_result.missing

    assert isinstance(file_result.missing, set)
    assert file_result.missing == set(missing)

    assert len(file_result.missing) == 3
    assert file_result.missing_count == 3
    assert file_result.total_count == 3

    assert file_result.total_size == 0

    assert "Missing: 3 file(s)" in str(file_result)

    assert "Total:" not in str(file_result)
    assert "No files" not in str(file_result)


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


def test_file_result():
    file_result = FileResult(
        success={RemoteFile(path="/success1", stats=RemoteFileStat(st_size=1024 * 1024 * 1024, st_mtime=50))},
        failed={
            FailedRemoteFile(
                path="/failed1",
                stats=RemoteFileStat(st_size=2 * 1024 * 1024 * 1024, st_mtime=50),
                exception=FileExistsError("abc"),
            ),
        },
        skipped={RemoteFile(path="/skipped1", stats=RemoteFileStat(st_size=3 * 1024 * 1024 * 1024, st_mtime=50))},
        missing={PurePath("/missing1")},
    )

    assert file_result.total_count == 4
    assert file_result.total_size == 6 * 1024 * 1024 * 1024

    assert "No files" not in str(file_result)

    assert "Successful: 1 file(s) 1.1 GB" in str(file_result)
    assert "Failed: 1 file(s) 2.1 GB" in str(file_result)
    assert "Skipped: 1 file(s) 3.2 GB" in str(file_result)
    assert "Total: 4 file(s) 6.4 GB" in str(file_result)
