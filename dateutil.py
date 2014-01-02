import datetime, types, calendar, contextlib

__all__ = [
    'clear_date_formats',
    'coerce_date',
    'coerce_day',
    'day_to_ushort',
    'days',
    'format_day',
    'format_hour',
    'format_month',
    'format_week',
    'from_epoch',
    'from_epoch_millis',
    'hours',
    'intervals',
    'minutes',
    'now',
    'parse_date',
    'register_date_format',
    'reset_now',
    'seconds',
    'set_millis',
    'set_millis_ctx',
    'set_now',
    'to_day',
    'to_epoch',
    'to_epoch_millis',
    'to_hour',
    'to_minute',
    'to_month',
    'to_quarter',
    'to_second',
    'to_week',
    'to_year',
    'today',
    'ushort_to_day',
    'weeks',
    'yesterday',
]

_now = None
def set_now(dt):
    """
    Sets `pyutil.now()` function to return the specified datetime
    """
    global _now
    _now = coerce_date(dt)

def now():
    """
    Returns the current timestamp.
    Can be manipulated or frozen with `pyutil.set_now` and `pyutil.reset_now`, generally for testing purposes.
    """
    global _now
    return _now or datetime.datetime.utcnow()

def reset_now():
    """
    All future calls to `pyutil.now()` will return the current time as of reset_now()
    """
    set_now(None)
    set_now(now())

_millis = False
def set_millis(value):
    global _millis
    _millis = value

@contextlib.contextmanager
def set_millis_ctx(value):
    global _millis
    prev_millis = _millis
    _millis = value
    yield
    _millis = prev_millis

# Day Utils
def today():
    """
    Returns today as a datetime.date (possibly adjusted for set_now)
    """
    return coerce_day(now())

def yesterday():
    """
    Returns yesterday as a datetime.date (possibly adjusted for set_now)
    """
    return today() - days(1)

epoch_day = datetime.date(1970, 1, 1)
def day_to_ushort(dt):
    """
    Converts a datetime.date or datetime.datetime object to the number of days since Jan 1 1970
    """
    global epoch_day
    return (coerce_day(dt) - epoch_day).days

def ushort_to_day(intval):
    """
    Conerts a number of days since Jan 1 1970 to a datetime.date
    """
    global epoch_day
    return epoch_day + days(intval)


# Datetime Utils
_date_formats = [ '%Y-%m-%d %H:%M:%S' ]
def clear_date_formats():
    """
    Removes all registered date formats except '%Y-%m-%d %H:%M:%S'
    """
    global _date_formats
    _date_formats = [ '%Y-%m-%d %H:%M:%S' ]

def register_date_format(str_format, append=True):
    """
    Registers a date format to be attempted via strptime for parse_date and coerce_date
    append=False inserts the date format at the beginning of the list and makes it
    the default date format.
    """
    global _date_formats
    # Yes, this could be a set but I am interested in order.
    if str_format not in _date_formats:
        if append:
            _date_formats.append(str_format)
        else:
            _date_formats.insert(0, str_format)

def format_date(dt, fmt = None):
    """
    Returns the formatted date.
    The format is optional and defaults to the first registered date format.
    """
    return dt.strftime(fmt or _date_formats[0])

def format_day(dt, fmt = None):
    """
    Returns the formatted date truncated to the day.
    The format is optional and defaults to the first registered date format.
    """
    return dt.strftime(fmt or "%Y-%m-%d")

def format_hour(dt, fmt = None):
    """
    Returns the formatted date truncated to the month
    The format is optional and defaults to the first registered date format.
    """
    return format_date(to_hour(dt), fmt)

def format_week(dt, fmt = None):
    """
    Returns the formatted date truncated to the month
    The format is optional and defaults to the first registered date format.
    """
    return format_day(to_week(dt), fmt)

def format_month(dt, fmt = None):
    """
    Returns the formatted date truncated to the month
    The format is optional and defaults to the first registered date format.
    """
    return format_day(to_month(dt), fmt = None)

def parse_date(dt):
    """
    Parses the date based on registered date formats.
    """
    global _date_formats
    if isinstance(dt, datetime.datetime):
        return dt
    for fmt in _date_formats:
        try:
            return datetime.datetime.strptime(dt, fmt)
        except ValueError as e:
            pass
    raise ValueError("Unable to parse date ({}) with any format ({})".format(dt, _date_formats))

def coerce_day(dt):
    """
    Coerces a value into a datetime.date
    """
    global _millis
    if isinstance(dt, datetime.datetime):
        return dt.date()
    elif isinstance(dt, datetime.date):
        return dt
    elif isinstance(dt, types.NoneType):
        return dt
    elif isinstance(dt, (int, long, float)) and _millis:
        return from_epoch_millis(dt).date()
    elif isinstance(dt, (int, long, float)):
        return from_epoch(dt).date()
    elif isinstance(dt, (str, unicode)):
        return parse_date(dt).date()
    else:
        return datetime.date(dt)

def coerce_date(dt):
    """
    Coerces a value into a datetime.datetime
    """
    global _millis
    if isinstance(dt, datetime.datetime):
        return dt
    elif isinstance(dt, datetime.date):
        return datetime.datetime(dt.year, dt.month, dt.day)
    elif isinstance(dt, types.NoneType):
        return dt
    elif isinstance(dt, (int, long, float)) and _millis:
        return from_epoch_millis(dt)
    elif isinstance(dt, (int, long, float)):
        return from_epoch(dt)
    elif isinstance(dt, (str, unicode)):
        return parse_date(dt)
    else:
        return datetime.datetime(dt)

def from_epoch(epoch):
    """
    Returns a datetime.datetime object from the specified UTC time (in seconds).
    """
    return datetime.datetime.utcfromtimestamp(epoch)

def from_epoch_millis(epoch):
    """
    Returns a datetime.datetime object from the specified UTC time (in milliseconds).
    """
    return datetime.datetime.utcfromtimestamp(epoch/1000)

def to_epoch(dt):
    """
    Converts a datetime into Unix epoch (in seconds)
    """
    if isinstance(dt, (int, long, float)):
        return dt
    return calendar.timegm(coerce_date(dt).timetuple())

def to_epoch_millis(dt):
    """
    Converts a datetime into Unix Epoch (in milliseconds).
    Assumes that any integer type value is already milliseconds.
    """
    if isinstance(dt, (int, long, float)):
        return dt
    return int(1000 * calendar.timegm(coerce_date(dt).timetuple()))

def to_second(dt):
    """
    Truncates a datetime to second
    """
    return dt.replace(microsecond = 0)

def to_minute(dt):
    """
    Truncates a datetime to minute
    """
    return dt.replace(
        second      = 0,
        microsecond = 0,
    )

def to_hour(dt):
    """
    Truncates a datetime to hour
    """
    return dt.replace(
        minute      = 0,
        second      = 0,
        microsecond = 0,
    )

def to_day(dt):
    """
    Truncates a datetime to day
    """
    return dt.replace(
        hour        = 0,
        minute      = 0,
        second      = 0,
        microsecond = 0,
    )

def to_week(dt):
    """
    Truncates a datetime to day.  Monday is assumed to be the start of the week.
    """
    return to_day(dt) - datetime.timedelta(days=dt.weekday())

def to_month(dt):
    """
    Truncates a datetime to month
    """
    return dt.replace(
        day         = 1,
        hour        = 0,
        minute      = 0,
        second      = 0,
        microsecond = 0,
    )

def to_quarter(dt):
    """
    Truncates a datetime to quarter
    The quarters are truncated as follows:
        Jan, Feb, Mar -> Jan 1
        Apr, May, Jun -> Apr 1
        Jul, Aug, Sep -> Jul 1
        Oct, Nov, Dec -> Oct 1
    """
    return dt.replace(
        month       = (dt.month-1)//3*3 + 1,
        day         = 1,
        hour        = 0,
        minute      = 0,
        second      = 0,
        microsecond = 0,
    )

def to_year(dt):
    """
    Truncates a datetime to year
    """
    return dt.replace(
        month       = 1,
        day         = 1,
        hour        = 0,
        minute      = 0,
        second      = 0,
        microsecond = 0,
    )

def seconds(n):
    """
    Returns a datetime.timedelta object for n seconds
    """
    return datetime.timedelta(seconds = n)

def minutes(n):
    """
    Returns a datetime.timedelta object for n minutes
    """
    return datetime.timedelta(minutes = n)

def hours(n):
    """
    Returns a datetime.timedelta object for n hours
    """
    return datetime.timedelta(hours = n)

def days(n):
    """
    Returns a datetime.timedelta object for n days
    """
    return datetime.timedelta(days = n)

def weeks(n):
    """
    Returns a datetime.timedelta object for n weeks
    """
    return datetime.timedelta(weeks = n)

def intervals(duration, dt=None):
    if not dt:
        dt = to_epoch(now())
    else:
        dt = to_epoch(coerce_date(dt))

    return int(1 + dt/duration) * duration
