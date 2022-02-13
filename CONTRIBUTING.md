# Contributing

## Introduction

First off, thank you for considering contributing to redis-py. We value
community contributions!

## Contributions We Need

You may already know what you want to contribute \-- a fix for a bug you
encountered, or a new feature your team wants to use.

If you don't know what to contribute, keep an open mind! Improving
documentation, bug triaging, and writing tutorials are all examples of
helpful contributions that mean less work for you.

## Your First Contribution

Unsure where to begin contributing? You can start by looking through
[help-wanted
issues](https://github.com/andymccurdy/redis-py/issues?q=is%3Aopen+is%3Aissue+label%3ahelp-wanted).

Never contributed to open source before? Here are a couple of friendly
tutorials:

-   <http://makeapullrequest.com/>
-   <http://www.firsttimersonly.com/>

## Getting Started

Here's how to get started with your code contribution:

1.  Create your own fork of redis-py
2.  Do the changes in your fork
3.
    *Create a virtualenv and install the development dependencies from the dev_requirements.txt file:*

        a.  python -m venv .venv
        b.  source .venv/bin/activate
        c.  pip install -r dev_requirements.txt

4.  If you need a development environment, run `invoke devenv`
5.  While developing, make sure the tests pass by running `invoke tests`
6.  If you like the change and think the project could use it, send a
    pull request

To see what else is part of the automation, run `invoke -l`

## The Development Environment

Running `invoke devenv` installs the development dependencies specified
in the dev_requirements.txt. It starts all of the dockers used by this
project, and leaves them running. These can be easily cleaned up with
`invoke clean`. NOTE: it is assumed that the user running these tests,
can execute docker and its various commands.

-   A master Redis node
-   A Redis replica node
-   Three sentinel Redis nodes
-   A redis cluster
-   An stunnel docker, fronting the master Redis node
-   A Redis node, running unstable - the latest redis

The replica node, is a replica of the master node, using the
[leader-follower replication](https://redis.io/topics/replication)
feature.

The sentinels monitor the master node in a [sentinel high-availability
configuration](https://redis.io/topics/sentinel).

## Testing

Call `invoke tests` to run all tests, or `invoke all-tests` to run linters
tests as well. With the 'tests' and 'all-tests' targets, all Redis and
RedisCluster tests will be run.

It is possible to run only Redis client tests (with cluster mode disabled) by
using `invoke standalone-tests`; similarly, RedisCluster tests can be run by using
`invoke cluster-tests`.

Each run of tox starts and stops the various dockers required. Sometimes
things get stuck, an `invoke clean` can help.

Continuous Integration uses these same wrappers to run all of these
tests against multiple versions of python. Feel free to test your
changes against all the python versions supported, as declared by the
tox.ini file (eg: tox -e py39). If you have the various python versions
on your desktop, you can run *tox* by itself, to test all supported
versions.

### Docker Tips

Following are a few tips that can help you work with the Docker-based
development environment.

To get a bash shell inside of a container:

`$ docker run -it <service> /bin/bash`

**Note**: The term \"service\" refers to the \"services\" defined in the
`tox.ini` file at the top of the repo: \"master\", \"replicaof\",
\"sentinel_1\", \"sentinel_2\", \"sentinel_3\".

Containers run a minimal Debian image that probably lacks tools you want
to use. To install packages, first get a bash session (see previous tip)
and then run:

`$ apt update && apt install <package>`

You can see the logging output of a containers like this:

`$ docker logs -f <service>`

The command make test runs all tests in all tested Python
environments. To run the tests in a single environment, like Python 3.9,
use a command like this:

`$ docker-compose run test tox -e py39 -- --redis-url=redis://master:6379/9`

Here, the flag `-e py39` runs tests against the Python 3.9 tox
environment. And note from the example that whenever you run tests like
this, instead of using make test, you need to pass
`-- --redis-url=redis://master:6379/9`. This points the tests at the
\"master\" container.

Our test suite uses `pytest`. You can run a specific test suite against
a specific Python version like this:

`$ docker-compose run test tox -e py36 -- --redis-url=redis://master:6379/9 tests/test_commands.py`

### Troubleshooting

If you get any errors when running `make dev` or `make test`, make sure
that you are using supported versions of Docker.

Please try at least versions of Docker.

-   Docker 19.03.12

## How to Report a Bug

### Security Vulnerabilities

**NOTE**: If you find a security vulnerability, do NOT open an issue.
Email [Redis Open Source (<oss@redis.com>)](mailto:oss@redis.com) instead.

In order to determine whether you are dealing with a security issue, ask
yourself these two questions:

-   Can I access something that's not mine, or something I shouldn't
    have access to?
-   Can I disable something for other people?

If the answer to either of those two questions are *yes*, then you're
probably dealing with a security issue. Note that even if you answer
*no*  to both questions, you may still be dealing with a security
issue, so if you're unsure, just email [us](mailto:oss@redis.com).

### Everything Else

When filing an issue, make sure to answer these five questions:

1.  What version of redis-py are you using?
2.  What version of redis are you using?
3.  What did you do?
4.  What did you expect to see?
5.  What did you see instead?

## How to Suggest a Feature or Enhancement

If you'd like to contribute a new feature, make sure you check our
issue list to see if someone has already proposed it. Work may already
be under way on the feature you want -- or we may have rejected a
feature like it already.

If you don't see anything, open a new issue that describes the feature
you would like and how it should work.

## Code Review Process

The core team looks at Pull Requests on a regular basis. We will give
feedback as as soon as possible. After feedback, we expect a response
within two weeks. After that time, we may close your PR if it isn't
showing any activity.
