#!/usr/bin/env python

"""
@file setup.py
@author Andy McCurdy
@date 2/12/2010
@brief Setuptools configuration for redis client
"""

version = '1.36'

sdict = {
    'name' : 'redis',
    'version' : version,
    'description' : 'Python client for Redis key-value store',
    'long_description' : 'Python client for Redis key-value store',
    'url': 'http://github.com/andymccurdy/redis-py',
    'download_url' : 'http://cloud.github.com/downloads/andymccurdy/redis-py/redis-%s.tar.gz' % version,
    'author' : 'Andy McCurdy',
    'author_email' : 'sedrik@gmail.com',
    'maintainer' : 'Andy McCurdy',
    'maintainer_email' : 'sedrik@gmail.com',
    'keywords' : ['Redis', 'key-value store'],
    'license' : 'MIT',
    'packages' : ['redis'],
    'test_suite' : 'tests.all_tests',
    'classifiers' : [
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python'],
}

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
    
setup(**sdict)

