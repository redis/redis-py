# https://github.com/pyinvoke/invoke/issues/833
import inspect
import os
import shutil

from invoke import run, task

if not hasattr(inspect, "getargspec"):
    inspect.getargspec = inspect.getfullargspec


@task
def devenv(c):
    """Brings up the test environment, by wrapping docker compose."""
    clean(c)
    cmd = "docker-compose --profile all up -d --build"
    run(cmd)


@task
def build_docs(c):
    """Generates the sphinx documentation."""
    run("pip install -r docs/requirements.txt")
    run("make -C docs html")


@task
def linters(c):
    """Run code linters"""
    run("flake8 tests redis")
    run("black --target-version py37 --check --diff tests redis")
    run("isort --check-only --diff tests redis")
    run("vulture redis whitelist.py --min-confidence 80")
    run("flynt --fail-on-change --dry-run tests redis")


@task
def all_tests(c):
    """Run all linters, and tests in redis-py."""
    linters(c)
    tests(c)


@task
def tests(c, uvloop=False, protocol=2, profile=False):
    """Run the redis-py test suite against the current python."""
    print("Starting Redis tests")
    standalone_tests(c, uvloop=uvloop, protocol=protocol, profile=profile)
    cluster_tests(c, uvloop=uvloop, protocol=protocol, profile=profile)


@task
def standalone_tests(c, uvloop=False, protocol=2, profile=False):
    """Run tests against a standalone redis instance"""
    profile_arg = "--profile" if profile else ""
    if uvloop:
        run(
            f"pytest {profile_arg} --protocol={protocol} --cov=./ --cov-report=xml:coverage_redis.xml -W always -m 'not onlycluster' --uvloop --junit-xml=standalone-uvloop-results.xml"
        )
    else:
        run(
            f"pytest {profile_arg} --protocol={protocol} --cov=./ --cov-report=xml:coverage_redis.xml -W always -m 'not onlycluster' --junit-xml=standalone-results.xml"
        )


@task
def cluster_tests(c, uvloop=False, protocol=2, profile=False):
    """Run tests against a redis cluster"""
    profile_arg = "--profile" if profile else ""
    cluster_url = "redis://localhost:16379/0"
    if uvloop:
        run(
            f"pytest {profile_arg} --protocol={protocol} --cov=./ --cov-report=xml:coverage_cluster.xml -W always -m 'not onlynoncluster and not redismod' --redis-url={cluster_url} --junit-xml=cluster-uvloop-results.xml --uvloop"
        )
    else:
        run(
            f"pytest  {profile_arg} --protocol={protocol} --cov=./ --cov-report=xml:coverage_clusteclient.xml -W always -m 'not onlynoncluster and not redismod' --redis-url={cluster_url} --junit-xml=cluster-results.xml"
        )


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
