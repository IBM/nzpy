#!/usr/bin/env python
import os

from setuptools import setup

readme = os.path.join(os.path.dirname(__file__), "README.md")

setup(
    name="nzpy",
    version=1.0,
    description="IBM Netezza python driver",
    long_description=open(readme).read(),
    long_description_content_type='text/markdown',
    author="IBM",
    author_email="shabmoh3@in.ibm.com",
    url="https://github.com/ibm/nzpy",
    license="IBM",
    python_requires='>=3.5',
    install_requires=['scramp>=1.1.0, <2.0'],
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
    project_urls={
        "Documentation": "https://github.com/IBM/nzpy/wiki",
        "Source": "https://github.com/IBM/nzpy",
        "Tracker": "https://github.com/IBM/nzpy/issues",
    },

    keywords="Netezza dbapi",
    packages=("nzpy",)
)
