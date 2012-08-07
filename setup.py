#!/usr/bin/env python
import os

from redis import __version__

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

f = open(os.path.join(os.path.dirname(__file__), 'README.md'))
long_description = f.read()
f.close()

setup(
    name='redis',
    version=__version__,
    description='Python client for Redis key-value store',
    long_description=long_description,
    url='http://github.com/andymccurdy/redis-py',
    download_url=('http://cloud.github.com/downloads/andymccurdy/'
                  'redis-py/redis-%s.tar.gz' % __version__),
    author='Andy McCurdy',
    author_email='sedrik@gmail.com',
    maintainer='Andy McCurdy',
    maintainer_email='sedrik@gmail.com',
    keywords=['Redis', 'key-value store'],
    license='MIT',
    packages=['redis'],
    test_suite='tests.all_tests',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.5',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        ]
)
