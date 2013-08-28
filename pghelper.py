import psycopg2
import psycopg2.extras

__all__ = [
    'execute',
    'iter_result_rows',
    'fetch_result_rows',
    'set_sql_log_func',
    'relation_info',
    'table_exists',
    'view_exists',
    #'vacuum',
    #'currval',
    #'nextval',
]

_log_func = None
def set_sql_log_func(func):
    """
    Sets the log function for execute.  It should look something like:

    def log_func(sql):
        pass

    pyutil.dbhelper.set_sql_log_func(log_func)
    """
    global _log_func
    _log_func = func


def execute(conn, sql, **bind_params):
    """
    Executes a SQL command against the connection with optional bind params.
    """
    global _log_func

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        bound_sql = cur.mogrify(sql, bind_params)

        if _log_func:
            _log_func(bound_sql)

        cur.execute(sql, bind_params)

def iter_result_rows(conn, sql, **bind_params):
    """
    Delays fetching the SQL results into memory until iteration
    Keeps memory footprint low
    """
    global _log_func
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        bound_sql = cur.mogrify(sql, bind_params)

        if _log_func:
            _log_func(bound_sql)

        cur.execute(sql, bind_params)
        for row in cur:
            yield row

def fetch_result_rows(conn, sql, **bind_params):
    """
    Immediatly fetches the SQL results into memory
    Trades memory for the ability to immediately execute another query
    """
    global _log_func
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        bound_sql = cur.mogrify(sql, bind_params)

        if _log_func:
            _log_func(bound_sql)

        cur.execute(sql, bind_params)
        return cur.fetchall()

def copy_from(conn, fp, table_name, columns = None):
    fp.seek(0)
    conn.cursor().copy_from(fp, table_name, columns = columns)

def relation_info(conn, relname, relkind = 'r'):
    """
    Fetch object information from the pg catalog
    """
    return fetch_result_rows(conn, """
        SELECT *
        FROM pg_class
        WHERE relname = %(relname)s
            AND relkind = %(relkind)s
    """,
        relname = relname,
        relkind = relkind,
    )

def table_exists(conn, table_name):
    """
    Determine whether a table exists in the current database
    """
    return len(relation_info(conn, table_name, 'r')) > 0

def view_exists(conn, view_name):
    """
    Determine whether a view exists in the current database
    """
    return len(relation_info(conn, view_name, 'v')) > 0

def vacuum(conn, table_name):
    raise NotImplemented()

def currval(conn, sequence):
    raise NotImplemented()

def nextval(conn, sequence):
    raise NotImplemented()
