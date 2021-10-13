#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# This code is distributed under the terms and conditions
# from the Apache License, Version 2.0
#
# http://opensource.org/licenses/apache2.0.php

from pathlib import Path
from setuptools import setup

with (Path(__file__).parent / 'README.rst').open() as file:
    readme = file.read()

setup(
    name='expiringsqlitedict',
    version='2.1.0',
    description='Persistent compressed expiring dict in Python, backed up by sqlite3 and pickle',
    long_description=readme,
    long_description_content_type='text/x-rst',

    py_modules=['expiringsqlitedict'],

    author='Taylor C. Richberger',
    author_email="tcr@absolute-performance.com",
    maintainer='Taylor C. Richberger',
    maintainer_email='tcr@absolute-performance.com',

    url='https://github.com/absperf/expiringsqlitedict',
    download_url='http://pypi.python.org/pypi/expiringsqlitedict',

    keywords='sqlite, persistent dict',

    license='Apache 2.0',
    platforms='any',

    classifiers=[  # from http://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.6',
        'Topic :: Database :: Front-Ends',
    ],
)
