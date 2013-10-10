import datetime, types, calendar

__all__ = [
    'clear_date_formats',
    'coerce_date',
    'days',
    'from_epoch',
    'hours',
    'minutes',
    'now',
    'parse_date',
    'register_date_format',
    'reset_now',
    'seconds',
    'set_now',
    'to_day',
    'to_epoch',
    'to_hour',
    'to_minute',
    'to_month',
    'to_quarter',
    'to_second',
    'to_week',
    'to_year',
    'weeks',
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


_date_formats = []
def clear_date_formats():
    """
    Removes all registered date formats
    """
    global _date_formats
    _date_formats = []

def register_date_format(str_format):
    """
    Registers a date format to be attempted via strptime for parse_date and coerce_date
    """
    global _date_formats
    _date_formats.append(str_format)

def parse_date(dt):
    """
    Iterates through all registerd date formats and returns the first that succeeds
    """
    global _date_formats
    if isinstance(dt, datetime.datetime):
        return dt
    for fmt in _date_formats:
        try:
            return datetime.datetime.strptime(dt, fmt)
        except ValueError, e:
            pass
    raise ValueError("Unable to parse date ({}) with any format ({})".format(dt, _date_formats))

def coerce_date(dt):
    """
    Coerces a value into a datetime.datetime
    """
    if isinstance(dt, datetime.datetime):
        return dt
    elif isinstance(dt, types.NoneType):
        return dt
    elif isinstance(dt, int) or isinstance(dt, long) or isinstance(dt, float):
        return from_epoch(dt)
    elif isinstance(dt, str) or isinstance(dt, unicode):
        return parse_date(dt)
    else:
        return datetime.datetime(dt)

def from_epoch(epoch):
    """
    Returns the epoch in UTC from a given epoch value (in seconds)
    """
    return datetime.datetime.utcfromtimestamp(epoch)

def to_epoch(dt):
    """
    Converts a datetime into Unix epoch (in seconds)
    """
    if isinstance(dt, int) or isinstance(dt, long) or isinstance(dt, float):
        return dt
    if isinstance(dt, datetime.datetime):
        return calendar.timegm(dt.timetuple())

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
