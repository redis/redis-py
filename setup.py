#!/usr/bin/env python
from redis import __version__

sdict = {
    'name' : 'redis',
    'version' : __version__,
    'description' : 'Python client for Redis key-value store',
    'long_description' : 'Python client for Redis key-value store',
    'url': 'http://github.com/andymccurdy/redis-py',
    'download_url' : 'http://cloud.github.com/downloads/andymccurdy/redis-py/redis-%s.tar.gz' % __version__,
    'author' : 'Andy McCurdy',
    'author_email' : 'sedrik@gmail.com',
    'maintainer' : 'Andy McCurdy',
    'maintainer_email' : 'sedrik@gmail.com',
    'keywords' : ['Redis', 'key-value store'],
    'license' : 'MIT',
    'packages' : ['redis'],
    'test_suite' : 'tests.all_tests',
    'classifiers' : [
        'Development Status :: 5 - Production/Stable',
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

