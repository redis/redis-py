#!/bin/sh

# This is the entrypoint for "make test". It invokes Tox. If running
# within the CI environment, it also runs codecov

set -eu

REDIS_MASTER="${REDIS_MASTER_HOST}":"${REDIS_MASTER_PORT}"
echo "Testing against Redis Server: ${REDIS_MASTER}"

# use the wait-for-it util to ensure the server is running before invoking Tox
util/wait-for-it.sh ${REDIS_MASTER} -- tox -- --redis-url=redis://"${REDIS_MASTER}"/9

# if the TRAVIS env var is defined, invoke "codecov"
if [ ! -z ${TRAVIS-} ]; then
    codecov
fi
