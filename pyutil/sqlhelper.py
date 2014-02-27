__all__ = [
    'set_sql_log_func',
    'execute',
    'iter_results',
    'fetch_results',
]

_log_func = None
def set_sql_log_func(func):
    """
    Sets the log function for execute.  It should look something like:

    def log_func(cur, sql, bind_params):
        pass

    For Postgresql, this might be a useful function:

    def log_func(cur, sql, bind_params):
        logging.info("Executing query\n%s", cur.mogrify(sql, bind_params))

    pyutil.dbhelper.set_sql_log_func(log_func)
    """
    global _log_func
    _log_func = func

def execute(conn, sql, **bind_params):
    """
    Executes a SQL command against the connection with optional bind params.
    """
    global _log_func

    try:
        cur = conn.cursor()

        if _log_func:
            _log_func(cur, sql, bind_params)

        cur.execute(sql, bind_params)
    finally:
        cur.close()

def iter_results(conn, sql, **bind_params):
    """
    Delays fetching the SQL results into memory until iteration
    Keeps memory footprint low
    """
    global _log_func
    try:
        cur = conn.cursor()
        if _log_func:
            _log_func(cur, sql, bind_params)

        cur.execute(sql, bind_params)
        for row in cur:
            yield row
    finally:
        cur.close()

def fetch_results(conn, sql, **bind_params):
    """
    Immediatly fetches the SQL results into memory
    Trades memory for the ability to immediately execute another query
    """
    global _log_func
    try:
        cur = conn.cursor()
        if _log_func:
            _log_func(cur, sql, bind_params)

        cur.execute(sql, bind_params)
        return cur.fetchall()
    finally:
        cur.close()

