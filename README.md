wizzat.py
======
Python Utilities includes an assortment of decorators, utility methods, and utility classes.  It's intended that this package can
serve to bootstrap a fresh Python 2 project.

Modules:
- The _decorators_ module primarily contains memoization, benchmarking, coroutine, tail call recursion, and test skipping decorators.
- The _queuefile_ module contains a thread and process safe file writer.
- The _util_ module contains utility functions.
- The _dateutil_ module contains date utils for working on top of python-dateutil and pytz.
- The _pghelper_ module contains utilities for working with raw psycopg2 connections and a light weight named connection manager.
- The _formattedtable_ module contains a wrapper on top of texttable for ensuring items are formatted before being printed
- The _serialization_ module contains methods for space and time efficient serialization of integer sets and lists.
- The _testutil_ module contains test cases, asserts, and mixins for getting various test behaviors.
- The _mathutil_ module contains various math utilites as well as logarithmic percentile approximation and running average.
- The _runner_ module contains a base class that handles much of the common boilerplate in setting up runners.
- The _sqlutil_ module contains a series of utility classes for sqlalchemy
- The _dbtable_ module contains a light weight ORM for Postgres
- The _kvtable_ module contains a light weight ORM for a generic KV Store
- The _cbtable_ module contains a light weight ORM for Couchbase
- The _s3table_ module contains a light weight ORM for S3

The most interesting functions are likely:
- decorators.memoize()
- dateutil.\*
- util.\*
- mathutil.Percentile

Documentation is maintained on individual functions

The official repository is http://www.github.com/wizzat.py
