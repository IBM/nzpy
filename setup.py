#!/usr/bin/env python

from setuptools import setup

long_description = """\

nzpy
----

nzpy is a Pure-Python interface to the Netezza database engine.  It is \
nzpy is somewhat distinctive in that it is written entirely in Python and does not \
rely on any external libraries (such as a compiled python module, or \
PostgreSQL's libpq library). nzpy supports the standard Python DB-API \
version 2.0.
"""

setup(
    name="nzpy",
    version=1.0,
    description="Netezza interface library",
    long_description=long_description,
    author="IBM",
    author_email="shabmoh3@in.ibm.com",
    url="https://github.com/ibm/nzpy",
    license="BSD",
    python_requires='>=3.5',
    install_requires=['scramp==1.1.0'],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: Implementation",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: Jython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Operating System :: OS Independent",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    keywords="Netezza dbapi",
    packages=("nzpy",)
)
