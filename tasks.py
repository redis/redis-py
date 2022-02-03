import os
import shutil

from invoke import run, task

with open("tox.ini") as fp:
    lines = fp.read().split("\n")
    dockers = [line.split("=")[1].strip() for line in lines if line.find("name") != -1]


@task
def devenv(c):
    """Builds a development environment: downloads, and starts all dockers
    specified in the tox.ini file.
    """
    clean(c)
    cmd = "tox -e devenv"
    for d in dockers:
        cmd += f" --docker-dont-stop={d}"
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
    run(f"docker rm -f {' '.join(dockers)}")


@task
def package(c):
    """Create the python packages"""
    run("python setup.py sdist bdist_wheel")
