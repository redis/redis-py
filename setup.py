#!/usr/bin/env python
from setuptools import find_packages, setup

setup(
    name="redis",
    description="Python client for Redis database and key-value store",
    long_description=open("README.md").read().strip(),
    long_description_content_type="text/markdown",
    keywords=["Redis", "key-value store", "database"],
    license="MIT",
    version="5.1.1",
    packages=find_packages(
        include=[
            "redis",
            "redis._parsers",
            "redis.asyncio",
            "redis.commands",
            "redis.commands.bf",
            "redis.commands.json",
            "redis.commands.search",
            "redis.commands.timeseries",
            "redis.commands.graph",
            "redis.parsers",
        ]
    ),
    package_data={"redis": ["py.typed"]},
    include_package_data=True,
    url="https://github.com/redis/redis-py",
    project_urls={
        "Documentation": "https://redis.readthedocs.io/en/latest/",
        "Changes": "https://github.com/redis/redis-py/releases",
        "Code": "https://github.com/redis/redis-py",
        "Issue tracker": "https://github.com/redis/redis-py/issues",
    },
    author="Redis Inc.",
    author_email="oss@redis.com",
    python_requires=">=3.8",
    install_requires=[
        'async-timeout>=4.0.3; python_full_version<"3.11.3"',
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
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],
    extras_require={
        "hiredis": ["hiredis>=3.0.0"],
        "ocsp": ["cryptography>=36.0.1", "pyopenssl==23.2.1", "requests>=2.31.0"],
    },
)
