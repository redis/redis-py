Contributing
============

Introduction
------------

First off, thank you for considering contributing to redis-py. We value community contributions!

Contributions We Need
----------------------

You may already know what you want to contribute -- a fix for a bug you encountered, or a new feature your team wants to use.

If you don't know what to contribute, keep an open mind! Improving documentation, bug triaging, or writing tutorials are all examples of helpful contributions that mean less work for you.

Your First Contribution
-----------------------
Unsure where to begin contributing? You can start by looking through help-wanted issues: https://github.com/andymccurdy/redis-py/issues?q=is%3Aopen+is%3Aissue+label%3ahelp-wanted

Never contributed to open source before? Here are a couple of friendly tutorials:

- http://makeapullrequest.com/
- http://www.firsttimersonly.com/

Getting Started
---------------

Here's how to get started with your code contribution:

1. Create your own fork of redis-py
2. When you've checked out the fork locally, build the docker containers: `make build`
2. Do the changes in your fork
3. Make sure the tests pass by running: `make test`
4. If you like the change and think the project could use it, send a pull request

Troubleshooting
^^^^^^^^^^^^^^^

If you get any errors when running `make build` or `make test`, make sure that you
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
