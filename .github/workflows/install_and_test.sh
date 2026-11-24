#!/bin/bash

set -e

SUFFIX=$1
if [ -z ${SUFFIX} ]; then
    echo "Supply valid python package extension such as whl or tar.gz. Exiting."
    exit 3
fi

script=`pwd`/${BASH_SOURCE[0]}
HERE=`dirname ${script}`
ROOT=`realpath ${HERE}/../..`

cd ${ROOT}
DESTENV=${ROOT}/.venvforinstall
if [ -d ${DESTENV} ]; then
    rm -rf ${DESTENV}
fi
python -m venv ${DESTENV}
source ${DESTENV}/bin/activate
pip install --upgrade --quiet pip
pip install --quiet -r dev_requirements.txt
pip uninstall -y redis  # uninstall Redis package installed via redis-entraid
invoke devenv --endpoints=all-stack
invoke package

# find packages
PKG=`ls ${ROOT}/dist/*.${SUFFIX}`
ls -l ${PKG}

TESTDIR=${ROOT}/STAGETESTS
if [ -d ${TESTDIR} ]; then
    rm -rf ${TESTDIR}
fi
mkdir ${TESTDIR}
cp -R ${ROOT}/tests ${TESTDIR}/tests
cd ${TESTDIR}

# install, run tests
pip install ${PKG}
# Redis tests
# multidb_integration tests are excluded: they require the dedicated `multidb`
# docker profile (two clusters + two standalones), which is not provisioned here.
pytest -m 'not onlycluster and not multidb_integration' --ignore=tests/test_scenario --ignore=tests/test_asyncio/test_scenario
# RedisCluster tests
CLUSTER_URL="redis://localhost:16379/0"
CLUSTER_SSL_URL="rediss://localhost:27379/0"
pytest -m 'not onlynoncluster and not redismod and not ssl and not multidb_integration' \
  --ignore=tests/test_scenario \
  --ignore=tests/test_asyncio/test_scenario \
  --redis-url="${CLUSTER_URL}" \
  --redis-ssl-url="${CLUSTER_SSL_URL}"
