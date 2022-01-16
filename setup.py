#!/usr/bin/env python

from setuptools import setup

setup(
    name='tsd-api-lib',
    version='0.0.1',
    description='tsdapililb - tools for building APIs and clients',
    author='Leon du Toit',
    author_email='l.c.d.toit@usit.uio.no',
    url='https://github.com/unioslo/tsd-api-lib',
    packages=['tsdapilib'],
    install_requires=['blake3'],
    python_requires='>=3.6',

)
