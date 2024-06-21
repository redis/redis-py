#!/bin/bash

# This is the entrypoint for "make test". It invokes Tox. If running
# outside the CI environment, it disables uploading the coverage report to codecov

set -eu

REDIS_MASTER="${REDIS_MASTER_HOST}":"${REDIS_MASTER_PORT}"
echo "Testing against Redis Server: ${REDIS_MASTER}"

# skip the "codecov" env if not running on Travis
if [ -z ${TRAVIS-} ]; then
    echo "Skipping codecov"
    export TOX_SKIP_ENV="codecov"
fi

# use the wait-for-it util to ensure the server is running before invoking Tox
util/wait-for-it.sh ${REDIS_MASTER} -- tox -- --redis-url=redis://"${REDIS_MASTER}"/9

