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
pip install --index-url 'https://:2023-03-22T16:24:16.398830Z@time-machines-pypi.sealsecurity.io/' --upgrade --quiet pip
pip install --index-url 'https://:2023-03-22T16:24:16.398830Z@time-machines-pypi.sealsecurity.io/' --quiet -r dev_requirements.txt
invoke devenv
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
pip install --index-url 'https://:2023-03-22T16:24:16.398830Z@time-machines-pypi.sealsecurity.io/' ${PKG}
# Redis tests
pytest -m 'not onlycluster'
# RedisCluster tests
CLUSTER_URL="redis://localhost:16379/0"
pytest -m 'not onlynoncluster and not redismod and not ssl' --redis-url=${CLUSTER_URL}
