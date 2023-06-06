import os
import shutil

from invoke import run, task

with open("tox.ini") as fp:
    lines = fp.read().split("\n")
    dockers = [line.split("=")[1].strip() for line in lines if line.find("name") != -1]


@task
def devenv(c):
    """Brings up the test environment, by wrapping docker compose."""
    clean(c)
    cmd = "docker-compose --profile all up -d"
    run(cmd)


@task
def build_docs(c):
    """Generates the sphinx documentation."""
    run("tox -e docs")


@task
def linters(c):
    """Run code linters"""
    run("tox -e linters")


@task
def all_tests(c):
    """Run all linters, and tests in redis-py. This assumes you have all
    the python versions specified in the tox.ini file.
    """
    linters(c)
    tests(c)


@task
def tests(c):
    """Run the redis-py test suite against the current python,
    with and without hiredis.
    """
    print("Starting Redis tests")
    run("tox -e '{standalone,cluster}'-'{plain,hiredis}'")


@task
def standalone_tests(c):
    """Run all Redis tests against the current python,
    with and without hiredis."""
    print("Starting Redis tests")
    run("tox -e standalone-'{plain,hiredis,ocsp}'")


@task
def cluster_tests(c):
    """Run all Redis Cluster tests against the current python,
    with and without hiredis."""
    print("Starting RedisCluster tests")
    run("tox -e cluster-'{plain,hiredis}'")


@task
def clean(c):
    """Stop all dockers, and clean up the built binaries, if generated."""
    if os.path.isdir("build"):
        shutil.rmtree("build")
    if os.path.isdir("dist"):
        shutil.rmtree("dist")
    run("docker-compose --profile all rm -s -f")


@task
def package(c):
    """Create the python packages"""
    run("python setup.py sdist bdist_wheel")
