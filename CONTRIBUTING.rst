Contributing
============

Introduction
------------

First off, thank you for considering contributing to redis-py. We value community contributions!

Contributions We Need
----------------------

You may already know what you want to contribute -- a fix for a bug you encountered, or a new feature your team wants to use.

If you don't know what to contribute, keep an open mind! Improving documentation, bug triaging, and writing tutorials are all examples of helpful contributions that mean less work for you.

Your First Contribution
-----------------------
Unsure where to begin contributing? You can start by looking through `help-wanted issues <https://github.com/andymccurdy/redis-py/issues?q=is%3Aopen+is%3Aissue+label%3ahelp-wanted>`_.

Never contributed to open source before? Here are a couple of friendly tutorials:

- http://makeapullrequest.com/
- http://www.firsttimersonly.com/

Getting Started
---------------

Here's how to get started with your code contribution:

1. Create your own fork of redis-py
2. Do the changes in your fork
3. If you need a development environment, run ``make dev``
4. While developing, make sure the tests pass by running ``make test``
5. If you like the change and think the project could use it, send a pull request

The Development Environment
---------------------------

Running ``make dev`` will create a Docker-based development environment that starts the following containers:

* A master Redis node
* A slave Redis node
* Three sentinel Redis nodes
* A test container

The slave is a replica of the master node, using the `leader-follower replication <https://redis.io/topics/replication>`_ feature.

The sentinels monitor the master node in a `sentinel high-availability configuration <https://redis.io/topics/sentinel>`_.

Meanwhile, the `test` container hosts the code from your checkout of ``redis-py`` and allows running tests against many Python versions.

Docker Tips
^^^^^^^^^^^

Following are a few tips that can help you work with the Docker-based development environment.

To get a bash shell inside of a container:

``$ docker-compose run <service> /bin/bash``
 
**Note**: The term "service" refers to the "services" defined in the ``docker-compose.yml`` file: "master", "slave", "sentinel_1", "sentinel_2", "sentinel_3", "test".

Containers run a minimal Debian image that probably lacks tools you want to use. To install packages, first get a bash session (see previous tip) and then run:

``$ apt update && apt install <package>``

You can see the combined logging output of all containers like this:

``$ docker-compose logs``

The command `make test` runs all tests in all tested Python environments. To run the tests in a single environment, like Python 3.6, use a command like this:

``$ docker-compose run test tox -e py36 -- --redis-url=redis://master:6379/9``

Here, the flag ``-e py36`` runs tests against the Python 3.6 tox environment. And note from the example that whenever you run tests like this, instead of using `make test`, you need to pass ``-- --redis-url=redis://master:6379/9``. This points the tests at the "master" container.

Our test suite uses ``pytest``. You can run a specific test suite against a specific Python version like this:

``$ docker-compose run test tox -e py36 -- --redis-url=redis://master:6379/9 tests/test_commands.py``

Troubleshooting
^^^^^^^^^^^^^^^
If you get any errors when running ``make dev`` or ``make test``, make sure that you
are using supported versions of Docker and docker-compose.

The included Dockerfiles and docker-compose.yml file work with the following
versions of Docker and docker-compose:

* Docker 19.03.12
* docker-compose 1.26.2

How to Report a Bug
-------------------

Security Vulnerabilities
^^^^^^^^^^^^^^^^^^^^^^^^

**NOTE**: If you find a security vulnerability, do NOT open an issue. Email Andy McCurdy (sedrik@gmail.com) instead.

In order to determine whether you are dealing with a security issue, ask yourself these two questions:

* Can I access something that's not mine, or something I shouldn't have access to?
* Can I disable something for other people?

If the answer to either of those two questions are "yes", then you're probably dealing with a security issue. Note that even if you answer "no" to both questions, you may still be dealing with a security issue, so if you're unsure, just email Andy at sedrik@gmail.com.

Everything Else
^^^^^^^^^^^^^^^

When filing an issue, make sure to answer these five questions:

1. What version of redis-py are you using?
2. What version of redis are you using?
3. What did you do?
4. What did you expect to see?
5. What did you see instead?

How to Suggest a Feature or Enhancement
---------------------------------------

If you'd like to contribute a new feature, make sure you check our issue list to see if someone has already proposed it. Work may already be under way on the feature you want -- or we may have rejected a feature like it already.

If you don't see anything, open a new issue that describes the feature you would like and how it should work.

Code Review Process
-------------------

The core team looks at Pull Requests on a regular basis. We will give feedback as as soon as possible. After feedback, we expect a response within two weeks. After that time, we may close your PR if it isn't showing any activity.
