import os
from pathlib import Path


here = Path(__file__).parent.resolve()


def get_version():
    if "CI_COMMIT_TAG" in os.environ:
        return os.environ["CI_COMMIT_TAG"]

    version_file = here / "onetl" / "VERSION"
    version = version_file.read_text().strip()  # noqa: WPS410

    build_num = os.environ.get("CI_PIPELINE_IID", "")
    branch_name = os.environ.get("CI_COMMIT_REF_SLUG", "")
    branches_protect = ["master", "develop"]

    if branch_name in branches_protect:
        return f"{version}.dev{build_num}"

    return f"{version}.dev{build_num}+{branch_name}"
