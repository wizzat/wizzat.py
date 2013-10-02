import sys, inspect, errno, os

__all__ = [
    'carp',
    'chunks',
    'import_class',
    'merge_dicts',
    'set_defaults',
    'swallow',
    'mkdirp',
    'slurp',
]

def mkdirp(path):
    """
        Ensure that directory :path exists.
        Analogous to mkdir -p

        See http://stackoverflow.com/questions/10539823/python-os-makedirs-to-recreate-path
    """
    try:
        os.makedirs(path)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise

def merge_dicts(*iterable):
    """
        Merge a list of dictionaries.
        Successive keys are updated.

        See http://dietbuddha.blogspot.com/2013/04/python-expression-idiom-merging.html
    """
    return reduce(lambda a, b: a.update(b) or a, iterable, {})

def swallow(err_type, func, *args, **kwargs):
    """
        Swallow an exception.

        swallow(KeyError, lambda: dictionary[x])

        vs

        try:
            dictionary[x]
        except KeyError:
            pass

    """
    try:
        return func(*args, **kwargs)
    except err_type:
        pass

def carp(msg, file_obj = None):
    """
        Python's equivalent of Perl's carp

        http://stackoverflow.com/questions/8275745/warnings-from-callers-perspective-aka-python-equivalent-of-perls-carp
    """

    # grab the current call stack, and remove the stuff we don't want
    file_obj = file_obj or sys.stderr

    stack = inspect.stack()
    stack = stack[1:]

    caller_func = stack[0][1]
    caller_line = stack[0][2]
    file_obj.write('%s at %s line %d\n' % (msg, caller_func, caller_line))

    for idx, frame in enumerate(stack[1:]):
        # The frame, one up from `frame`
        upframe = stack[idx]
        upframe_record = upframe[0]
        upframe_func   = upframe[3]
        upframe_module = inspect.getmodule(upframe_record).__name__

        # The stuff we need from the current frame
        frame_file = frame[1]
        frame_line = frame[2]

        file_obj.write('\t%s.%s ' % (upframe_module, upframe_func))
        file_obj.write('called at %s line %d\n' % (frame_file, frame_line))

def import_class(name):
    """
        Import a class a string and return a reference to it.
        THIS IS A GIANT SECURITY VULNERABILITY.

        See: http://stackoverflow.com/questions/547829/how-to-dynamically-load-a-python-class
    """
    if type(name) != str:
        return name

    package    = ".".join(name.split(".")[: - 1])
    class_name = name.split(".")[  - 1]

    mod = __import__(package, fromlist=[class_name])
    return getattr(mod, class_name)

def chunks(iterable, chunk_size):
    """
        Iterate across iterable in chunks of chunk_size

        Example:
        for chunk in chunks(some_long_iterable, 500):
            for element in chunk:
                element.perform_operation()
    """
    for chunk_no in xrange(0, len(iterable), chunk_size):
        yield iterable[chunk_no:chunk_no+chunk_size]

def set_defaults(kwargs, defaults = {}, **default_values):
    """
    Returns kwargs with defaults set.
    Has two forms:
    kwargs = set_defaults(kwargs, { 'value1' : 'value1', 'value2' : 'value2' })
    kwargs = set_defaults(kwargs,
        value1 = 'value1',
        value2 = 'value2',
    )
    """
    if defaults:
        defaults = dict(defaults)
        defaults.update(kwargs)
        return defaults
    else:
        default_values.update(kwargs)
        return default_values

def slurp(filename):
    """
    Find the named file, read it into memory, and return it as a string.
    """
    with open(filename, 'r') as fp:
        return fp.read()
