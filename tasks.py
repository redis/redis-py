# https://github.com/pyinvoke/invoke/issues/833
import inspect
import os
import shutil

from invoke import run, task

if not hasattr(inspect, "getargspec"):
    inspect.getargspec = inspect.getfullargspec


@task
def devenv(c, endpoints="all"):
    """Brings up the test environment, by wrapping docker compose."""
    clean(c)
    cmd = f"docker compose --profile {endpoints} up -d --build"
    run(cmd)


@task
def build_docs(c):
    """Generates the sphinx documentation."""
    run("pip install -r docs/requirements.txt")
    run("make -C docs html")


@task
def linters(c):
    """Run code linters"""
    run("ruff check tests redis")
    run("ruff format --check --diff tests redis")
    run("vulture redis whitelist.py --min-confidence 80")


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
def standalone_tests(
    c, uvloop=False, protocol=2, profile=False, redis_mod_url=None, extra_markers=""
):
    """Run tests against a standalone redis instance"""
    profile_arg = "--profile" if profile else ""
    redis_mod_url = f"--redis-mod-url={redis_mod_url}" if redis_mod_url else ""
    extra_markers = f" and {extra_markers}" if extra_markers else ""

    if uvloop:
        run(
            f"pytest {profile_arg} --protocol={protocol} {redis_mod_url} --cov=./ --cov-report=xml:coverage_resp{protocol}_uvloop.xml -m 'not onlycluster{extra_markers}' --uvloop --junit-xml=standalone-resp{protocol}-uvloop-results.xml"
        )
    else:
        run(
            f"pytest {profile_arg} --protocol={protocol} {redis_mod_url} --cov=./ --cov-report=xml:coverage_resp{protocol}.xml -m 'not onlycluster{extra_markers}' --junit-xml=standalone-resp{protocol}-results.xml"
        )


@task
def cluster_tests(c, uvloop=False, protocol=2, profile=False):
    """Run tests against a redis cluster"""
    profile_arg = "--profile" if profile else ""
    cluster_url = "redis://localhost:16379/0"
    cluster_tls_url = "rediss://localhost:27379/0"
    if uvloop:
        run(
            f"pytest {profile_arg} --protocol={protocol} --cov=./ --cov-report=xml:coverage_cluster_resp{protocol}_uvloop.xml -m 'not onlynoncluster and not redismod' --redis-url={cluster_url} --redis-ssl-url={cluster_tls_url} --junit-xml=cluster-resp{protocol}-uvloop-results.xml --uvloop"
        )
    else:
        run(
            f"pytest  {profile_arg} --protocol={protocol} --cov=./ --cov-report=xml:coverage_cluster_resp{protocol}.xml -m 'not onlynoncluster and not redismod' --redis-url={cluster_url} --redis-ssl-url={cluster_tls_url} --junit-xml=cluster-resp{protocol}-results.xml"
        )


@task
def clean(c):
    """Stop all dockers, and clean up the built binaries, if generated."""
    if os.path.isdir("build"):
        shutil.rmtree("build")
    if os.path.isdir("dist"):
        shutil.rmtree("dist")
    run("docker compose --profile all rm -s -f")


@task
def package(c):
    """Create the python packages"""
    run("python -m build .")
