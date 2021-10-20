import os
import shutil
import tox
from invoke import task, run

with open('tox.ini') as fp:
    lines = fp.read().split("\n")
    dockers = [l.split("=")[1].strip() for l in lines if l.find("name") != -1]

@task
def devenv(c):
    """Builds a development environment: downloads, and starts all dockers specified in the tox.ini file."""
    clean(c)
    cmd = 'tox -e devenv'
    for d in dockers:
        cmd += " --docker-dont-stop={}".format(d)
    print("Running: {}".format(cmd))
    run(cmd)

@task
def lint(c):
    """Run code linters"""
    run("flake8")

@task
def all_tests(c):
    """Run all linters, and tests in redis-py. This assumes you have all pythons specified in tox.ini"""
    lint(c)
    test(c)

@task
def test(c):
    """Run the redis-py test suite against the current python, with and without hiredis"""
    run("tox -e plain")
    run("tox -e hiredis")

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
    run("python setup.py build sdist bdist_wheel")
