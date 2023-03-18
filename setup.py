from setuptools import find_packages, setup

setup(
    name="sp_etl",
    packages=find_packages(exclude=["sp_etl_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
s