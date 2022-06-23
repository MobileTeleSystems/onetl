import pytest

from onetl.strategy import YAMLHWMStore


@pytest.mark.parametrize(
    "qualified_name, file_name",
    [
        (
            "id|partition=abc/another=cde#mydb.mytable@dbtype://host.name:1234/schema#dag.task.myprocess@myhost",
            "id__partition_abc_another_cde__mydb.mytable__dbtype_host.name_1234_schema__dag.task.myprocess__myhost",
        ),
        (
            "id#mydb.mytable@dbtype://host.name:1234/schema#dag.task.myprocess@myhost",
            "id__mydb.mytable__dbtype_host.name_1234_schema__dag.task.myprocess__myhost",
        ),
        (
            "column__with__underscores#mydb.mytable@dbtype://host.name:1234/schema#"
            "dag__with__underscores.task_with_underscores.myprocess@myhost",
            "column__with__underscores__mydb.mytable__dbtype_host.name_1234_schema"
            "__dag__with__underscores.task_with_underscores.myprocess__myhost",
        ),
        (
            "id#mydb.mytable@cluster#dag.task.myprocess@myhost",
            "id__mydb.mytable__cluster__dag.task.myprocess__myhost",
        ),
        (
            "id#mydb.mytable@dbtype://host.name:1234/schema#myprocess@myhost",
            "id__mydb.mytable__dbtype_host.name_1234_schema__myprocess__myhost",
        ),
        (
            "downloaded_files#/home/user/abc@ftp://my.domain:23#dag.task.myprocess@myhost",
            "downloaded_files__home_user_abc__ftp_my.domain_23__dag.task.myprocess__myhost",
        ),
        (
            "downloaded_files#/home/user/abc@ftp://my.domain:23#myprocess@myhost",
            "downloaded_files__home_user_abc__ftp_my.domain_23__myprocess__myhost",
        ),
    ],
)
def test_yaml_hwm_store_cleanup_file_name(qualified_name, file_name):
    assert YAMLHWMStore.cleanup_file_name(qualified_name) == file_name
