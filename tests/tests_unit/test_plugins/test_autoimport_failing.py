import textwrap

import pytest

import onetl


def test_autoimport_failing(monkeypatch):
    monkeypatch.delenv("ONETL_PLUGINS_BLACKLIST", raising=False)

    error_msg = textwrap.dedent(
        r"""
        Error while importing plugin 'failing-plugin' from package 'failing' v0.1.0.

        Statement:
            import failing

        Check if plugin is compatible with current onETL version \d+.\d+.\d+.

        You can disable loading this plugin by setting environment variable:
            ONETL_PLUGINS_BLACKLIST='failing-plugin,failing-plugin'

        You can also define a whitelist of packages which can be loaded by onETL:
            ONETL_PLUGINS_WHITELIST='not-failing-plugin1,not-failing-plugin2'

        Please take into account that plugin name may differ from package or module name.
        See package metadata for more details
        """,
    ).strip()

    with pytest.raises(ImportError, match=error_msg):
        onetl.plugins_auto_import()


def test_autoimport_failing_disabled(monkeypatch):
    monkeypatch.setenv("ONETL_PLUGINS_ENABLED", "false")

    # no exception
    onetl.plugins_auto_import()


def test_autoimport_failing_whitelist(monkeypatch):
    monkeypatch.delenv("ONETL_PLUGINS_BLACKLIST", raising=False)

    # skip all plugins instead of some-other-plugin
    monkeypatch.setenv("ONETL_PLUGINS_WHITELIST", "some-other-plugin")

    # no exception
    onetl.plugins_auto_import()

    # import only failing-plugin
    monkeypatch.setenv("ONETL_PLUGINS_WHITELIST", "failing-plugin")
    with pytest.raises(ImportError):
        onetl.plugins_auto_import()


def test_autoimport_failing_blacklist(monkeypatch):
    # ignore failing plugin
    monkeypatch.setenv("ONETL_PLUGINS_BLACKLIST", "failing-plugin")

    # no exception
    onetl.plugins_auto_import()

    # return failing plugin back
    monkeypatch.setenv("ONETL_PLUGINS_BLACKLIST", "some-other-plugin")
    with pytest.raises(ImportError):
        onetl.plugins_auto_import()


def test_autoimport_failing_env_variables_priority(monkeypatch):
    # blacklist is applied after whitelist
    monkeypatch.setenv("ONETL_PLUGINS_BLACKLIST", "failing-plugin")
    monkeypatch.setenv("ONETL_PLUGINS_WHITELIST", "failing-plugin")

    onetl.plugins_auto_import()
