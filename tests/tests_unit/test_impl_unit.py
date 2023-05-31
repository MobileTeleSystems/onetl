import os
import stat
import textwrap
from datetime import datetime
from time import time

import pytest

from onetl.impl import (
    FailedLocalFile,
    FailedRemoteFile,
    LocalPath,
    RemoteDirectory,
    RemoteFile,
    RemotePath,
    RemotePathStat,
    path_repr,
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
    file_stat = RemotePathStat(st_size=10, st_mtime=50)
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
    file_stat = RemotePathStat(st_size=10, st_mtime=50)
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
    file_stat = RemotePathStat(st_size=10, st_mtime=50)
    assert file_stat.st_size == 10
    assert file_stat.st_mtime == 50

    assert file_stat == file_stat  # noqa: WPS312 NOSONAR
    assert RemotePathStat(st_size=10, st_mtime=50) == RemotePathStat(st_size=10, st_mtime=50)

    assert RemotePathStat(st_size=10, st_mtime=50) != RemotePathStat(st_size=20, st_mtime=50)
    assert RemotePathStat(st_size=20, st_mtime=50) != RemotePathStat(st_size=10, st_mtime=50)

    assert RemotePathStat(st_size=10, st_mtime=50) != RemotePathStat(st_size=10, st_mtime=60)
    assert RemotePathStat(st_size=10, st_mtime=60) != RemotePathStat(st_size=10, st_mtime=50)

    assert RemotePathStat(st_size=10, st_mtime=50) != RemotePathStat(st_size=20, st_mtime=60)
    assert RemotePathStat(st_size=20, st_mtime=60) != RemotePathStat(st_size=15, st_mtime=50)


@pytest.mark.parametrize(
    "item1, item2",
    [
        (RemotePath("a/b/c"), RemoteDirectory(path="a/b/c")),
        (LocalPath("a/b/c"), FailedLocalFile(path="a/b/c", exception=FileNotFoundError("abc"))),
        (RemotePath("a/b/c"), RemoteFile(path="a/b/c", stats=RemotePathStat(st_size=0, st_mtime=0))),
        (
            RemotePath("a/b/c"),
            FailedRemoteFile(
                path="a/b/c",
                stats=RemotePathStat(st_size=0, st_mtime=0),
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
        (RemotePath("a/b/c"), RemoteFile(path="a/b/c", stats=RemotePathStat(st_size=0, st_mtime=0))),
        (
            RemotePath("a/b/c"),
            FailedRemoteFile(
                path="a/b/c",
                stats=RemotePathStat(st_size=0, st_mtime=0),
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

    stats1 = RemotePathStat(st_size=0, st_mtime=0)
    stats2 = RemotePathStat(st_size=1, st_mtime=0)

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

    stats1 = RemotePathStat(st_size=0, st_mtime=0)
    stats2 = RemotePathStat(st_size=1, st_mtime=0)

    assert FailedRemoteFile(path1, stats1, exception1) == FailedRemoteFile(path1, stats1, exception1)
    assert FailedRemoteFile(path1, stats1, exception1) == FailedRemoteFile(path1 + "/", stats1, exception1)

    assert FailedRemoteFile(path1, stats1, exception1) != FailedRemoteFile(path2, stats1, exception1)
    assert FailedRemoteFile(path2, stats1, exception1) != FailedRemoteFile(path1, stats1, exception1)

    assert FailedRemoteFile(path1, stats1, exception1) != FailedRemoteFile(path1, stats2, exception1)
    assert FailedRemoteFile(path1, stats2, exception1) != FailedRemoteFile(path1, stats1, exception1)

    assert FailedRemoteFile(path1, stats1, exception1) != FailedRemoteFile(path1, stats1, exception2)
    assert FailedRemoteFile(path1, stats1, exception2) != FailedRemoteFile(path1, stats1, exception1)


@pytest.mark.parametrize(
    "kwargs, kind",
    [
        ({}, None),
        ({"st_mode": stat.S_IFSOCK}, "socket"),
        ({"st_mode": stat.S_IFLNK}, "link"),
        ({"st_mode": stat.S_IFREG}, "file"),
        ({"st_mode": stat.S_IFBLK}, "block"),
        ({"st_mode": stat.S_IFDIR}, "directory"),
        ({"st_mode": stat.S_IFCHR}, "char"),
        ({"st_mode": stat.S_IFIFO}, "fifo"),
    ],
)
def test_path_repr_stats_with_kind(kwargs, kind):
    options = {"with_size": False, "with_mode": False, "with_mtime": False, "with_owner": False}

    remote_file = RemoteFile("a/b/c", stats=RemotePathStat(**kwargs))
    remote_directory = RemoteDirectory("a/b/c", stats=RemotePathStat(**kwargs))
    local_file = LocalPath("a/b/c")

    assert path_repr(remote_file, with_kind=True, **options) == f"'a/b/c' (kind='{kind or 'file'}')"
    assert path_repr(remote_directory, with_kind=True, **options) == f"'a/b/c' (kind='{kind or 'directory'}')"
    assert path_repr(local_file, with_kind=True, **options) == "'a/b/c' (kind='missing')"

    assert path_repr(remote_file, with_kind=False, **options) == "'a/b/c'"
    assert path_repr(remote_directory, with_kind=False, **options) == "'a/b/c'"
    assert path_repr(local_file, with_kind=False, **options) == "'a/b/c'"

    # cannot detect kind if input is just str or PurePath
    assert path_repr("a/b/c", with_kind=True, **options) == "'a/b/c'"
    assert path_repr("/a/b/c/", with_kind=True, **options) == "'/a/b/c'"
    assert path_repr(RemotePath("a/b/c"), with_kind=True, **options) == "'a/b/c'"

    assert path_repr("a/b/c", with_kind=False, **options) == "'a/b/c'"
    assert path_repr("/a/b/c/", with_kind=False, **options) == "'/a/b/c'"
    assert path_repr(RemotePath("a/b/c"), with_kind=False, **options) == "'a/b/c'"


@pytest.mark.parametrize(
    "st_size, details",
    [
        (0, ", size='0 Bytes'"),
        (10, ", size='10 Bytes'"),
    ],
)
def test_path_repr_stats_with_size(st_size, details):
    options = {"with_mode": False, "with_mtime": False, "with_owner": False}

    file = RemoteFile("a/b/c", stats=RemotePathStat(st_size=st_size))
    assert path_repr(file, with_size=False, **options) == "'a/b/c' (kind='file')"
    assert path_repr(file, with_size=True, **options) == f"'a/b/c' (kind='file'{details})"

    # even if directory stats contains size info, it will not be printed
    directory = RemoteDirectory("a/b/c", stats=RemotePathStat(st_size=st_size))
    assert path_repr(directory, with_size=False, **options) == "'a/b/c' (kind='directory')"
    assert path_repr(directory, with_size=True, **options) == "'a/b/c' (kind='directory')"


@pytest.mark.parametrize(
    "path_class, kind",
    [
        (RemoteFile, "file"),
        (RemoteDirectory, "directory"),
    ],
)
def test_path_repr_stats_with_mtime(path_class, kind):
    current_timestamp = time()
    current_datetime = datetime.fromtimestamp(current_timestamp).isoformat()
    options = {"with_size": False, "with_mode": False, "with_owner": False}

    file1 = path_class("a/b/c", stats=RemotePathStat(st_mtime=current_timestamp))
    file2 = path_class("a/b/c", stats=RemotePathStat())

    assert path_repr(file1, with_mtime=True, **options) == f"'a/b/c' (kind='{kind}', mtime='{current_datetime}')"
    assert path_repr(file1, with_mtime=False, **options) == f"'a/b/c' (kind='{kind}')"

    # no mtime - nothing to show
    assert path_repr(file2, with_mtime=True, **options) == f"'a/b/c' (kind='{kind}')"
    assert path_repr(file2, with_mtime=False, **options) == f"'a/b/c' (kind='{kind}')"


@pytest.mark.parametrize(
    "mode, mode_str",
    [
        (0o777, "rwxrwxrwx"),
        (0o666, "rw-rw-rw-"),
        (0o644, "rw-r--r--"),
        (0o400, "r--------"),
        (0o200, "-w-------"),
        (0o100, "--x------"),
        (0o40, "---r-----"),
        (0o20, "----w----"),
        (0o10, "-----x---"),
        (0o4, "------r--"),
        (0o2, "-------w-"),
        (0o1, "--------x"),
        (0, "---------"),
    ],
)
@pytest.mark.parametrize(
    "path_class, kind",
    [
        (RemoteFile, "file"),
        (RemoteDirectory, "directory"),
    ],
)
def test_path_repr_stats_with_mode(path_class, kind, mode, mode_str):
    file1 = path_class("a/b/c", stats=RemotePathStat(st_mode=mode))
    file2 = path_class("a/b/c", stats=RemotePathStat())
    options = {"with_size": False, "with_mtime": False, "with_owner": False}

    assert path_repr(file1, with_mode=True, **options) == f"'a/b/c' (kind='{kind}', mode='{mode_str}')"
    assert path_repr(file1, with_mode=False, **options) == f"'a/b/c' (kind='{kind}')"

    # no mode - nothing to show
    assert path_repr(file2, with_mode=True, **options) == f"'a/b/c' (kind='{kind}')"
    assert path_repr(file2, with_mode=False, **options) == f"'a/b/c' (kind='{kind}')"


@pytest.mark.parametrize(
    "user, user_str",
    [
        (123, ", uid=123"),
        ("me", ", uid='me'"),
    ],
)
@pytest.mark.parametrize(
    "group, group_str",
    [
        (123, ", gid=123"),
        ("me", ", gid='me'"),
    ],
)
@pytest.mark.parametrize(
    "path_class, kind",
    [
        (RemoteFile, "file"),
        (RemoteDirectory, "directory"),
    ],
)
def test_path_repr_stats_with_owner(path_class, kind, user, user_str, group, group_str):
    file1 = path_class("a/b/c", stats=RemotePathStat(st_uid=user, st_gid=group))
    file2 = path_class("a/b/c", stats=RemotePathStat())
    options = {"with_size": False, "with_mode": False, "with_mtime": False}

    assert path_repr(file1, with_owner=True, **options) == f"'a/b/c' (kind='{kind}'{user_str}{group_str})"
    assert path_repr(file1, with_owner=False, **options) == f"'a/b/c' (kind='{kind}')"

    # not owner - nothing to show
    assert path_repr(file2, with_owner=True, **options) == f"'a/b/c' (kind='{kind}')"
    assert path_repr(file2, with_owner=False, **options) == f"'a/b/c' (kind='{kind}')"


@pytest.mark.parametrize(
    "exception, exception_str",
    [
        (
            FileNotFoundError("abc"),
            textwrap.dedent(
                """
                'a/b/c' (kind='file', size='56.3 kB')
                    FileNotFoundError('abc')
                """,
            ).strip(),
        ),
        (
            FileExistsError("cde\ndef"),
            textwrap.dedent(
                """
                'a/b/c' (kind='file', size='56.3 kB')
                    FileExistsError('cde
                    def')
                """,
            ).strip(),
        ),
    ],
)
def test_path_repr_with_exception(exception, exception_str):
    file1 = FailedRemoteFile("a/b/c", stats=RemotePathStat(st_size=55 * 1024, st_mtime=0), exception=exception)
    file2 = RemoteFile("a/b/c", stats=RemotePathStat(st_size=55 * 1024, st_mtime=0))

    assert path_repr(file1, with_exception=True).strip() == exception_str
    assert path_repr(file1, with_exception=False).strip() == "'a/b/c' (kind='file', size='56.3 kB')"

    # no exception - nothing to show
    assert path_repr(file2, with_exception=True).strip() == "'a/b/c' (kind='file', size='56.3 kB')"
    assert path_repr(file2, with_exception=False).strip() == "'a/b/c' (kind='file', size='56.3 kB')"
