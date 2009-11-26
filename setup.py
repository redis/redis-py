#!/usr/bin/env python

"""
@file setup.py
@author Paul Hubbard
@date 10/2/09
@brief Setuptools configuration for redis client
"""

version = '0.6.0'

sdict = {
    'name' : 'redis',
    'version' : version,
    'description' : 'Python client for Redis key-value store',
    'url': 'http://github.com/andymccurdy/redis-py/downloads',
    'download_url' : 'http://cloud.github.com/downloads/andymccurdy/redis-py/redis-%s.tar.gz' % version,
    'author' : 'Andy McCurdy',
    'author_email' : 'sedrik@gmail.com',
    'maintainer' : 'Andy McCurdy',
    'maintainer_email' : 'sedrik@gmail.com',
    'keywords': ['Redis', 'key-value store'],
    'classifiers' : [
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python'],
}

from distutils.core import setup
#setup(**setupdict)
setup(name=sdict['name'],
      version=sdict['version'],
      author=sdict['author'],
      author_email=sdict['author_email'],
      maintainer=sdict['maintainer'],
      maintainer_email=sdict['maintainer_email'],
      url=sdict['url'],
      classifiers=sdict['classifiers'],
      description=sdict['description'],
      long_description=sdict['description'],
      download_url=sdict['download_url'],
      license='MIT',
      py_modules = ['redis'])
