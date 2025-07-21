from setuptools import find_packages, setup

setup(
    name="nfl_dagster",
    packages=find_packages(exclude=["nfl_dagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
