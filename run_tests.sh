#!/bin/bash
export PYTHONPATH=$PYTHONPATH:`find_up_dir env.sh`/..

find . -name '__pycache__' | xargs rm -rf
find . -name '*pyc' | xargs rm -f
dropdb pyutil_testdb
createdb pyutil_testdb

cd tests && python -m unittest discover -fv . "test*$1*.py"
