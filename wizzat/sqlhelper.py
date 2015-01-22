from __future__ import (absolute_import, division, print_function, unicode_literals)
from builtins import *

__all__ = [
    'set_sql_log_func',
    'execute',
    'iter_results',
    'fetch_results',
    'fetch_one',
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

    wizzat.dbhelper.set_sql_log_func(log_func)
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
    Keeps memory footprint low, but means you cannot run another query
    on this connection.
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
    Immediately fetches the SQL results into memory
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

def fetch_one(conn, sql, **bind_params):
    """
    Immediately fetches the SQL results into memory, and verifies that there is exactly one result
    """
    global _log_func
    try:
        cur = conn.cursor()
        if _log_func:
            _log_func(cur, sql, bind_params)

        cur.execute(sql, bind_params)
        results = cur.fetchall()
        assert len(results) == 1
        return results[0]
    finally:
        cur.close()

