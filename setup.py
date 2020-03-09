import os
from setuptools import setup, find_packages

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ..


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="pymemdb",
    version="1.0.1",
    author_email="luerhard@googlemail.com",
    description=("A very fast in-memory database with export to sqlite written purely in python"),
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    keywords="database rdbms sqlite relational python dictionary fast build memory",
    license="MIT",
    url="https://github.com/luerhard/pymemdb",
    install_requires=[
        "dataset",
    ],
    packages=find_packages(exclude=["tests/",
                                    ".circleci/",
                                    ]),
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities"
    ],
)
