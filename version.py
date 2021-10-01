import os

version = "0.1.{build_num}"  # Bump minor version here


def get_version():
    build_num = 0
    build_info = os.path.join(os.path.dirname(__file__), "..", "build.info")
    if os.path.exists(build_info):
        with open(build_info) as fp:
            build_num = fp.read().strip()

    build_num = os.environ.get("CI_PIPELINE_ID", build_num)
    res = version.format(build_num=build_num)

    branch_name = os.environ.get("gitlabBranch", os.environ.get("CI_COMMIT_BRANCH", ""))
    if branch_name != "master":
        res += ".dev0"

    return res
