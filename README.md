pyutil
======
Python Utilities

##### decorators #####
This is a collection of decorators that I've written or have found elsewhere.  Most have been modified at least a little.

- @memoize(): A performance optimized memoize function with centralized control of memoize data for easy cache clearing (Such as in tests)
- @benchmark: A benchmark decorator with centralized access to benchmark information
- @coroutine: A decorator for creating and starting coroutines
- @tail\_call\_optimized: The infamous tail_call_optimized decorator

##### queuefile #####
This is a thread and process safe file writer.  Data is put onto an internal queue and flushed to disk every .25 seconds.

##### util #####
This is a series of random python utilities.

- carp: A Python equivalent to Carp::cluck
- chunks: Iterate over a large iterable in bite sized chunks
- import\_class: import a class by name and return a reference.  Yes, this is potentially dangerous.
- merge\_dicts: merge dictionaries.  Successive values of the same key are overwritten
- swallow: Swallow an exception.  Mostly used to replace try/except/pass blocks.

##### dateutil #####
I've always found date handling in Python to be more arduous than it should be, even with some handy dandy date helpers like pytz and python-dateutil.  In theory I should push to have these (or things like them) included in python-dateutil.

- set\_now: Sets `pyutil.now()` function to return the specified datetime
- reset\_now: All future calls to `pyutil.now()` will return the current time as of reset\_now()
- now: Returns the current timestamp.  Can be manipulated or frozen with `pyutil.set\_now` and `pyutil.reset\_now`, generally for testing purposes.
- coerce\_date: Coerces a value into a datetime.datetime
- from\_epoch: Returns the epoch in UTC from a given epoch value (in seconds)
- parse\_date: Iterates through all registerd date formats and returns the first that succeeds
- register\_date\_format: Registers a date format to be attempted via strptime for parse\_date and coerce\_date
- clear\_date\_formats: Removes all registered date formats
- to\_epoch: Converts a datetime into Unix epoch (in seconds)
- to\_second: Truncates a datetime to second
- to\_minute: Truncates a datetime to minute
- to\_hour: Truncates a datetime to hour
- to\_day: Truncates a datetime to day
- to\_week: Truncates a datetime to day.  Monday is assumed to be the start of the week.
- to\_month: Truncates a datetime to month
- to\_year: Truncates a datetime to year
- to\_quarter: Truncates a datetime to quarter

        The quarters are truncated as follows:
            Jan, Feb, Mar -> Jan 1
            Apr, May, Jun -> Apr 1
            Jul, Aug, Sep -> Jul 1
            Oct, Nov, Dec -> Oct 1

##### pghelper #####
This is a series of methods that revolve around directly manipulating a psycopg2 connection.  It's largely unnecessary when using sqlalchemy or django, but can be useful when you don't want to include them as requirements for the project.  For instance, if you are writing a library.
- execute: Executes a SQL command against a particular psycopg2 connection.  Iteration over the result set is not supported.
- fetch\_result\_rows: Executes a SQL command and reads the entire result set into memory immediately.  Trades memory for the ability to immediately execute another query.
- iter\_result\_rows: Executes a SQL command for iteration.  Ideal for keeping the memory footprint low.
- set\_sql\_log\_func: Calls the supplied function with raw SQL for every SQL query executed.
- relation\_info: Obtains the relation info for the named relname/relkind for the current database
- table\_exists: Convenience alias for relation\_info(table\_name, 'r')
- view\_exists: Convenience alias for relation\_info(view\_name, 'v')
- nextval: Obtains the next value for the named sequence
- currval: Obtains the current value for the named sequence
