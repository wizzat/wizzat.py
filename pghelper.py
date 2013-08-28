import psycopg2
import psycopg2.extras

__all__ = [
    'execute',
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

        for result in cur:
            yield result

def fetch_result_rows(conn, sql, **bind_params):
    """
    Immediatly fetches the SQL results into memory
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
    Fetch table information from the pg catalog
    """
    results = fetch_result_rows(conn, """
        SELECT *
        from pg_class
        where relname = %(relname)s
            and relkind = %(relkind)s
    """,
        relname = relname,
        relkind = relkind,
    )

    print results

    return results

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
