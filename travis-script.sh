#!/bin/sh

if [[ $TEST_PEP8 == '1' ]]; then
   pep8 --repeat --show-source --exclude=.venv,.tox,dist,docs,build,*.egg .
else
   # using hiredis is only controllable through presence of the package
   # so test once with it, once without it
   coverage run setup.py test
   pip install hiredis
   coverage run setup.py test
fi
