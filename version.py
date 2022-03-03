import os

version = "0.1.2"  # Bump minor version here


def get_version():
    if "CI_COMMIT_TAG" in os.environ:
        return os.environ["CI_COMMIT_TAG"]

    build_num = os.environ.get("CI_PIPELINE_IID", "")
    return f"{version}.dev{build_num}"
