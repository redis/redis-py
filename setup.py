#!/usr/bin/env python
import os
import sys
from setuptools import setup
from setuptools.command.test import test as TestCommand

from redis import __version__


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # import here, because outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.test_args)
        sys.exit(errno)


f = open(os.path.join(os.path.dirname(__file__), 'README.rst'))
long_description = f.read()
f.close()

setup(
    name='redis',
    version=__version__,
    description='Python client for Redis key-value store',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    url='https://github.com/andymccurdy/redis-py',
    author='Andy McCurdy',
    author_email='sedrik@gmail.com',
    maintainer='Andy McCurdy',
    maintainer_email='sedrik@gmail.com',
    keywords=['Redis', 'key-value store'],
    license='MIT',
    packages=['redis'],
    python_requires=">=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*",
    extras_require={
        'hiredis': [
            "hiredis>=0.1.3",
        ],
    },
    tests_require=[
        'mock',
        'pytest>=2.7.0',
    ],
    cmdclass={'test': PyTest},
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ]
)
