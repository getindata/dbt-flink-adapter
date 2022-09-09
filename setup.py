#!/usr/bin/env python
from setuptools import find_namespace_packages, setup

package_name = "dbt-flink"
# make sure this always matches dbt/adapters/{adapter}/__version__.py
package_version = "0.0.1"
description = """The Flink adapter plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="GetInData",
    author_email="office@getindata.com",
    url="https://github.com/getindata/flink-dbt-adapter",
    packages=find_namespace_packages(include=["dbt", "dbt.*", "flink", "flink.*"]),
    include_package_data=True,
    install_requires=["dbt-core~=0.0.1.", "requests~=2.28.1"],
)
