import os
import shutil
from invoke import task, run

with open('tox.ini') as fp:
    lines = fp.read().split("\n")
    dockers = [line.split("=")[1].strip() for line in lines
               if line.find("name") != -1]


@task
def devenv(c):
    """Builds a development environment: downloads, and starts all dockers
    specified in the tox.ini file.
    """
    clean(c)
    cmd = 'tox -e devenv'
    for d in dockers:
        cmd += " --docker-dont-stop={}".format(d)
    run(cmd)


@task
def linters(c):
    """Run code linters"""
    run("flake8")


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
    run("tox -e plain -e hiredis")
    print("Starting RedisCluster tests")
    run("tox -e plain -e hiredis -- --redis-url=redis://localhost:16379/0")


@task
def clean(c):
    """Stop all dockers, and clean up the built binaries, if generated."""
    if os.path.isdir("build"):
        shutil.rmtree("build")
    if os.path.isdir("dist"):
        shutil.rmtree("dist")
    run("docker rm -f {}".format(' '.join(dockers)))


@task
def package(c):
    """Create the python packages"""
    run("python setup.py build install")
