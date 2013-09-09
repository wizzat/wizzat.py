pyutil
======

Python Utilities

decorators:
- @memoize(): A performance optimized memoize function with centralized control of memoize data for easy cache clearing (Such as in tests)
- @benchmark: A benchmark decorator with centralized access to benchmark information
- @coroutine: A decorator for creating and starting coroutines
- @tail\_call\_optimized: The infamous tail_call_optimized decorator

queuefile:
- QueueFile: A thread and process safe file writer

util:
- carp: A Python equivalent to Carp::cluck
- chunks: Iterate over a large iterable in bite sized chunks
- import\_class: import a class by name and return a reference.  Yes, this is potentially dangerous.
- merge\_dicts: merge dictionaries.  Successive values of the same key are overwritten
- swallow: Swallow an exception.  Mostly used to replace try/except/pass blocks.

dateutil:
- set\_now:
- reset\_now:
- now:
- coerce\_date:

pghelper:
- execute: Executes a SQL command against a particular psycopg2 connection.  Iteration over the result set is not supported.
- fetch\_result\_rows: Executes a SQL command and reads the entire result set into memory immediately.  Trades memory for the ability to immediately execute another query.
- iter\_result\_rows: Executes a SQL command for iteration.  Ideal for keeping the memory footprint low.
- set\_sql\_log\_func: Calls the supplied function with raw SQL for every SQL query executed.
- relation\_info: Obtains the relation info for the named relname/relkind for the current database
- table\_exists: Convenience alias for relation\_info(table\_name, 'r')
- view\_exists: Convenience alias for relation\_info(view\_name, 'v')
- nextval: Obtains the next value for the named sequence
- currval: Obtains the current value for the named sequence
