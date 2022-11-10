#!/usr/bin/env python
from setuptools import find_packages, setup

setup(
    name="redis",
    description="Python client for Redis database and key-value store",
    long_description=open("README.md").read().strip(),
    long_description_content_type="text/markdown",
    keywords=["Redis", "key-value store", "database"],
    license="MIT",
    version="4.4.0rc3",
    packages=find_packages(
        include=[
            "redis",
            "redis.asyncio",
            "redis.commands",
            "redis.commands.bf",
            "redis.commands.json",
            "redis.commands.search",
            "redis.commands.timeseries",
            "redis.commands.graph",
        ]
    ),
    url="https://github.com/redis/redis-py",
    project_urls={
        "Documentation": "https://redis.readthedocs.io/en/latest/",
        "Changes": "https://github.com/redis/redis-py/releases",
        "Code": "https://github.com/redis/redis-py",
        "Issue tracker": "https://github.com/redis/redis-py/issues",
    },
    author="Redis Inc.",
    author_email="oss@redis.com",
    python_requires=">=3.7",
    install_requires=[
        'importlib-metadata >= 1.0; python_version < "3.8"',
        'typing-extensions; python_version<"3.8"',
        "async-timeout>=4.0.2",
    ],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],
    extras_require={
        "hiredis": ["hiredis>=1.0.0"],
        "ocsp": ["cryptography>=36.0.1", "pyopenssl==20.0.1", "requests>=2.26.0"],
    },
)
