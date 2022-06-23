import os
import textwrap

import pytest

from onetl.impl import (
    FailedLocalFile,
    FailedRemoteFile,
    LocalPath,
    RemoteDirectory,
    RemoteFile,
    RemoteFileStat,
    RemotePath,
    humanize_path,
)


@pytest.mark.parametrize(
    "path",
    [
        "a/b/c",
        RemotePath("a/b/c"),
    ],
)
def test_remote_directory(path):
    remote_directory = RemoteDirectory(path)

    assert remote_directory.path == RemotePath(path)
    assert remote_directory.exists()
    assert remote_directory.is_dir()
    assert not remote_directory.is_file()

    assert isinstance(remote_directory.parent, RemoteDirectory)

    for parent in remote_directory.parents:
        assert isinstance(parent, RemoteDirectory)

    assert repr(remote_directory) == "RemoteDirectory('a/b/c')"


@pytest.mark.parametrize(
    "path",
    [
        "a/b/c",
        RemotePath("a/b/c"),
    ],
)
def test_remote_file(path):
    file_stat = RemoteFileStat(st_size=10, st_mtime=50)
    remote_file = RemoteFile(path, stats=file_stat)

    assert remote_file.path == RemotePath(path)
    assert remote_file.stats == file_stat

    assert remote_file.exists()
    assert not remote_file.is_dir()
    assert remote_file.is_file()
    assert remote_file.stat() == file_stat

    assert isinstance(remote_file.parent, RemoteDirectory)

    for parent in remote_file.parents:
        assert isinstance(parent, RemoteDirectory)

    assert repr(remote_file) == "RemoteFile('a/b/c')"


@pytest.mark.parametrize(
    "path",
    [
        __file__,
        LocalPath(__file__),
    ],
)
def test_failed_local_file(path):
    exception = FileNotFoundError("abc")
    remote_file = FailedLocalFile(path, exception)

    assert remote_file.path == LocalPath(path)
    assert remote_file.exception == exception

    assert remote_file.exists()
    assert not remote_file.is_dir()
    assert remote_file.is_file()
    assert remote_file.stat()

    assert repr(remote_file) == f"FailedLocalFile('{path}', FileNotFoundError('abc'))"


@pytest.mark.parametrize(
    "path",
    [
        "a/b/c",
        RemotePath("a/b/c"),
    ],
)
def test_failed_remote_file(path):
    exception = FileNotFoundError("abc")
    file_stat = RemoteFileStat(st_size=10, st_mtime=50)
    remote_file = FailedRemoteFile(path=path, stats=file_stat, exception=exception)

    assert remote_file.path == RemotePath(path)
    assert remote_file.exception == exception
    assert remote_file.stats == file_stat

    assert remote_file.exists()
    assert not remote_file.is_dir()
    assert remote_file.is_file()
    assert remote_file.stat() == file_stat

    assert isinstance(remote_file.parent, RemoteDirectory)

    for parent in remote_file.parents:
        assert isinstance(parent, RemoteDirectory)

    assert repr(remote_file) == "FailedRemoteFile('a/b/c', FileNotFoundError('abc'))"


def test_file_stat():
    file_stat = RemoteFileStat(st_size=10, st_mtime=50)
    assert file_stat.st_size == 10
    assert file_stat.st_mtime == 50

    assert file_stat == file_stat  # noqa: WPS312 NOSONAR
    assert RemoteFileStat(st_size=10, st_mtime=50) == RemoteFileStat(st_size=10, st_mtime=50)

    assert RemoteFileStat(st_size=10, st_mtime=50) != RemoteFileStat(st_size=20, st_mtime=50)
    assert RemoteFileStat(st_size=20, st_mtime=50) != RemoteFileStat(st_size=10, st_mtime=50)

    assert RemoteFileStat(st_size=10, st_mtime=50) != RemoteFileStat(st_size=10, st_mtime=60)
    assert RemoteFileStat(st_size=10, st_mtime=60) != RemoteFileStat(st_size=10, st_mtime=50)

    assert RemoteFileStat(st_size=10, st_mtime=50) != RemoteFileStat(st_size=20, st_mtime=60)
    assert RemoteFileStat(st_size=20, st_mtime=60) != RemoteFileStat(st_size=15, st_mtime=50)


@pytest.mark.parametrize(
    "item1, item2",
    [
        (RemotePath("a/b/c"), RemoteDirectory(path="a/b/c")),
        (LocalPath("a/b/c"), FailedLocalFile(path="a/b/c", exception=FileNotFoundError("abc"))),
        (RemotePath("a/b/c"), RemoteFile(path="a/b/c", stats=RemoteFileStat(st_size=0, st_mtime=0))),
        (
            RemotePath("a/b/c"),
            FailedRemoteFile(
                path="a/b/c",
                stats=RemoteFileStat(st_size=0, st_mtime=0),
                exception=FileNotFoundError("abc"),
            ),
        ),
    ],
)
def test_path_compat(item1, item2):
    # PathContainer subclasses can do just the same as other
    assert item1 == item2.path

    assert str(item1) == str(item2)
    assert bytes(item1) == bytes(item2)
    assert os.fspath(item1) == os.fspath(item2)

    assert item1 in {item1}  # noqa: WPS525
    assert item2 in {item2}  # noqa: WPS525
    assert {item1} == {item2} == {item1, item2}
    assert len({item1, item2}) == 1
    assert item1 in {item2}  # noqa: WPS525
    assert item2 in {item1}  # noqa: WPS525

    assert item1 == item2
    assert item2 == item1
    assert item1 in [item1]  # noqa: WPS525, WPS510
    assert item2 in [item2]  # noqa: WPS525, WPS510

    assert [item1] == [item2]
    assert item1 in [item2]  # noqa: WPS525, WPS510
    assert item2 in [item1]  # noqa: WPS525, WPS510

    assert item1 / "d" == item2 / "d"
    assert "d" / item1 == "d" / item2

    assert isinstance(item2 / "d", item1.__class__)
    assert isinstance("d" / item2, item1.__class__)


@pytest.mark.parametrize(
    "item1, item2",
    [
        (RemotePath("a/b/c"), RemoteDirectory(path="a/b/c")),
        (LocalPath("a/b/c"), FailedLocalFile(path="a/b/c", exception=FileNotFoundError("abc"))),
        (RemotePath("a/b/c"), RemoteFile(path="a/b/c", stats=RemoteFileStat(st_size=0, st_mtime=0))),
        (
            RemotePath("a/b/c"),
            FailedRemoteFile(
                path="a/b/c",
                stats=RemoteFileStat(st_size=0, st_mtime=0),
                exception=FileNotFoundError("abc"),
            ),
        ),
    ],
)
def test_path_div(item1, item2):
    assert item1 / "d" == item2 / "d"
    assert "d" / item1 == "d" / item2

    assert isinstance(item2 / "d", item1.__class__)
    assert isinstance("d" / item2, item1.__class__)


def test_remote_directory_eq():
    path1 = "a/b/c"
    path2 = "a/b/c/d"

    assert RemoteDirectory(path1) == RemoteDirectory(path1)
    assert RemoteDirectory(path1) == RemoteDirectory(path1 + "/")

    assert RemoteDirectory(path1) != RemoteDirectory(path2)
    assert RemoteDirectory(path2) != RemoteDirectory(path1)


def test_failed_local_file_eq():
    path1 = "a/b/c"
    path2 = "a/b/c/d"

    exception1 = FileNotFoundError("abc")
    exception2 = FileNotFoundError("cde")

    assert FailedLocalFile(path1, exception1) == FailedLocalFile(path1, exception1)
    assert FailedLocalFile(path1, exception1) == FailedLocalFile(path1 + "/", exception1)

    assert FailedLocalFile(path1, exception1) != FailedLocalFile(path2, exception1)
    assert FailedLocalFile(path2, exception1) != FailedLocalFile(path1, exception1)

    assert FailedLocalFile(path1, exception1) != FailedLocalFile(path1, exception2)
    assert FailedLocalFile(path1, exception2) != FailedLocalFile(path1, exception1)


def test_remote_file_eq():
    path1 = "a/b/c"
    path2 = "a/b/c/d"

    stats1 = RemoteFileStat(st_size=0, st_mtime=0)
    stats2 = RemoteFileStat(st_size=1, st_mtime=0)

    assert RemoteFile(path1, stats1) == RemoteFile(path1, stats1)
    assert RemoteFile(path1, stats1) == RemoteFile(path1 + "/", stats1)

    assert RemoteFile(path1, stats1) != RemoteFile(path2, stats1)
    assert RemoteFile(path2, stats1) != RemoteFile(path1, stats1)

    assert RemoteFile(path1, stats1) != RemoteFile(path1, stats2)
    assert RemoteFile(path1, stats2) != RemoteFile(path1, stats1)


def test_failed_remote_file_eq():
    path1 = "a/b/c"
    path2 = "a/b/c/d"

    exception1 = FileNotFoundError("abc")
    exception2 = FileNotFoundError("cde")

    stats1 = RemoteFileStat(st_size=0, st_mtime=0)
    stats2 = RemoteFileStat(st_size=1, st_mtime=0)

    assert FailedRemoteFile(path1, stats1, exception1) == FailedRemoteFile(path1, stats1, exception1)
    assert FailedRemoteFile(path1, stats1, exception1) == FailedRemoteFile(path1 + "/", stats1, exception1)

    assert FailedRemoteFile(path1, stats1, exception1) != FailedRemoteFile(path2, stats1, exception1)
    assert FailedRemoteFile(path2, stats1, exception1) != FailedRemoteFile(path1, stats1, exception1)

    assert FailedRemoteFile(path1, stats1, exception1) != FailedRemoteFile(path1, stats2, exception1)
    assert FailedRemoteFile(path1, stats2, exception1) != FailedRemoteFile(path1, stats1, exception1)

    assert FailedRemoteFile(path1, stats1, exception1) != FailedRemoteFile(path1, stats1, exception2)
    assert FailedRemoteFile(path1, stats1, exception2) != FailedRemoteFile(path1, stats1, exception1)


def test_humanize_path():
    assert humanize_path("a/b/c") == "a/b/c"
    assert humanize_path("/a/b/c/") == "/a/b/c"
    assert humanize_path(RemotePath("a/b/c")) == "a/b/c"
    assert humanize_path(RemoteDirectory("a/b/c")) == "a/b/c (directory)"
    assert humanize_path(LocalPath("a/b/c")) == "a/b/c (missing)"
    assert humanize_path(RemoteFile("a/b/c", stats=RemoteFileStat(st_size=10, st_mtime=50))) == "a/b/c (10 Bytes)"

    failed_file1 = FailedRemoteFile(
        path="a/b/c",
        stats=RemoteFileStat(st_size=55 * 1024, st_mtime=0),
        exception=FileNotFoundError("abc"),
    )
    failed_file1_str = """
        a/b/c (56.3 kB)
            FileNotFoundError('abc')
    """
    assert humanize_path(failed_file1).strip() == textwrap.dedent(failed_file1_str).strip()

    failed_file2 = FailedRemoteFile(
        path="a/b/c",
        stats=RemoteFileStat(st_size=55 * 1024, st_mtime=0),
        exception=FileExistsError("cde\ndef"),
    )
    failed_file2_str = """
        a/b/c (56.3 kB)
            FileExistsError('cde
            def')
    """
    assert humanize_path(failed_file2).strip() == textwrap.dedent(failed_file2_str).strip()
