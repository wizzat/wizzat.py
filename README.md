pyutil
======

Python Utilities

decorators:
- @memoize(): A performance optimized memoize function with centralized control of memoize data for easy cache clearing (Such as in tests)
- @benchmark: A benchmark decorator with centralized access to benchmark information
- @coroutine: A decorator for creating and starting coroutines
- @tail_call_optimized: The infamous tail_call_optimized decorator

queuefile:
- QueueFile: A thread and process safe file writer

util:
- carp: A Python equivalent to Carp::cluck
- chunks: Iterate over a large iterable in bite sized chunks
- import_class: import a class by name and return a reference.  Yes, this is potentially dangerous.
- merge_dicts: merge dictionaries.  Successive values of the same key are overwritten
- swallow: Swallow an exception.  Mostly used to replace try/except/pass blocks.
