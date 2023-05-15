from setuptools import setup

setup(
    name="failing",
    version="0.1.0",
    description="Setting up a python package",
    entry_points={"onetl.plugins": ["failing-plugin=failing"]},
)
