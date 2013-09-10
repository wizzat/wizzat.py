import datetime, types, calendar

__all__ = [
    'now',
    'set_now',
    'reset_now',
    'coerce_date',
    'from_epoch',
    'to_epoch',
    'to_second',
    'to_minute',
    'to_hour',
    'to_day',
    'to_week',
    'to_month',
    'to_quarter',
    'to_year',
    'register_date_format',
    'clear_date_formats',
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
    global _date_formats
    _date_formats = []

def register_date_format(str_format):
    global _date_formats
    _date_formats.append(str_format)

def coerce_date(dt):
    """
    Coerces a value into a datetime.datetime.  Checks for epoch (second) and certain string formats
    Currently, these formats are:
    """
    if isinstance(dt, datetime.datetime):
        return dt
    elif isinstance(dt, types.NoneType):
        return dt
    elif isinstance(dt, int) or isinstance(dt, long) or isinstance(dt, float):
        return from_epoch(dt)
    elif isinstance(dt, str) or isinstance(dt, unicode):
        global _date_formats
        for fmt in _date_formats:
            try:
                return datetime.datetime.strptime(dt, fmt)
            except ValueError, e:
                pass
        raise ValueError("Unable to parse date ({}) with any format ({})".format(dt, _date_formats))
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
    Truncates a datetime to minute
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
        month       = dt.month/3*3 + 1,
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

