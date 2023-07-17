# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from setuptools import setup

VERSION = "1.0.0"

with open('README.md') as f:
    long_description = f.read()

setup(
    name="lakesoul-spark",
    version=VERSION,
    description="Python APIs for using LakeSoul with Apache Spark",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/lakesoul-io/LakeSoul",
    author="LakeSoul Team",
    author_email="lakesoul-technical-discuss@lists.lfaidata.foundation",
    license="Apache-2.0",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python :: 3",
    ],
    keywords='lakesoul',
    package_dir={'': 'python'},
    packages=['lakesoul'],
    python_requires='>=3.6'
)
