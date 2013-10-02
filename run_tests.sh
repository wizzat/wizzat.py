#!/bin/bash
# oh my, that's so terrible.  I should definitely decide on a better name for this.
export PYTHONPATH=`find_up_dir env.sh`/..

find . -name '*pyc' | xargs rm -f
dropdb pyutil_testdb
createdb pyutil_testdb

cd tests && python -m unittest discover -fv . "test*$1*.py"
