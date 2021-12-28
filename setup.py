#
# Copyright [2022] [DMetaSoul Team]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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
    url="https://github.com/meta-soul/LakeSoul",
    author="DMetaSoul Team",
    author_email="opensource@dmetasoul.com",
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
