#!/bin/sh

if [[ $TEST_PEP8 == '1' ]]; then
   pep8 --repeat --show-source --exclude=.venv,.tox,dist,docs,build,*.egg .
else
   TEST_HIREDIS=1 coverage run --source=redis --parallel setup.py test
   TEST_HIREDIS=0 coverage run --source=redis --parallel setup.py test
fi