from __future__ import (absolute_import, division, print_function, unicode_literals)
from builtins import *
from future.utils import iteritems

try:
    from psycopg2cffi import compat
    compat.register()
except ImportError:
    pass

import collections
import io
import threading

import psycopg2, psycopg2.extras, psycopg2.pool
from wizzat.sqlhelper import *
from wizzat.util import set_defaults

__all__ = [
    'ConnMgr',
    'PgIntegrityError',
    'PgOperationalError',
    'PgProgrammingError',
    'analyze',
    'copy_from',
    'copy_from_rows',
    'currval',
    'drop_table',
    'execute',
    'fetch_one',
    'fetch_results',
    'iter_results',
    'nextval',
    'relation_info',
    'set_sql_log_func',
    'sql_where_from_params',
    'table_columns',
    'table_exists',
    'vacuum',
    'view_exists',
]

PgIntegrityError   = psycopg2.IntegrityError
PgOperationalError = psycopg2.OperationalError
PgProgrammingError = psycopg2.ProgrammingError
PgPoolError        = psycopg2.pool.PoolError

def copy_from(conn, fp, table_name, columns = None):
    """
    Resets the file pointer and initiates a pg_copy.

    This method requires postgresql
    """
    fp.seek(0)
    conn.cursor().copy_from(fp, table_name, columns = columns)

def copy_from_rows(conn, table_name, columns, rows):
    """
    Creates a file object containing the tab separated rows.

    This method requires postgresql
    """
    fp = io.StringIO()
    for row in rows:
        fp.write('\t'.join(row))
        fp.write('\n')

    copy_from(conn, fp, table_name, columns = columns)
    del fp

def relation_info(conn, relname, relkind = 'r'):
    """
    Fetch object information from the pg catalog

    This method requires postgresql
    """

    if '.' in relname:
        relschema, relname = relname.split('.')
    else:
        relschema = 'public'

    return fetch_results(conn, """
        SELECT *
        FROM pg_class
            INNER JOIN pg_namespace
                ON pg_class.relnamespace = pg_namespace.oid
        WHERE relname = %(relname)s
            AND relkind = %(relkind)s
    """,
        relname   = relname,
        relschema = relschema,
        relkind   = relkind,
    )

def table_columns(conn, table_name):
    """
    Gets the column names and data types for the table

    This method requires postgresql
    """
    return fetch_results(conn, """
        SELECT
            column_name,
            data_type
        FROM information_schema.columns
        WHERE table_name = %(table)s
        ORDER BY column_name, data_type
    """, table = table_name)

def drop_table(conn, table_name):
    """
    Drops a table
    """
    execute(conn, "DROP TABLE IF EXISTS {}".format(table_name))

def table_exists(conn, table_name):
    """
    Determine whether a table exists in the current database

    This method requires postgresql
    """
    return len(relation_info(conn, table_name, 'r')) > 0

def view_exists(conn, view_name):
    """
    Determine whether a view exists in the current database

    This method requires postgresql
    """
    return len(relation_info(conn, view_name, 'v')) > 0

def analyze(conn, table_name):
    """
    Analyzes a table

    This method requires postgresql
    """
    execute(conn, "analyze {}".format(table_name))

def vacuum(conn, table_name):
    """
    Vacuums a table

    This method requires postgresql
    """
    raise NotImplemented()

def currval(conn, sequence):
    """
    Obtains the current value of a sequence

    This method requires postgresql
    """
    return fetch_results(conn, "select currval(%(sequence)s)", sequence = sequence)[0][0]

def nextval(conn, sequence):
    """
    Obtains the next value of a sequence

    This method requires postgresql
    """
    return fetch_results(conn, "select nextval(%(sequence)s)", sequence = sequence)[0][0]

def sql_where_from_params(**kwargs):
    """
    Utility function for converting a param dictionary into a where clause
    Lists and tuples become in clauses
    """
    clauses = [ 'true' ]
    type_handler = collections.OrderedDict()
    type_handler[type(None)] = "{0} is null"
    type_handler[list]       = "{0} in (%({0})s)"
    type_handler[tuple]      = "{0} in %({0})s"

    for key, value in sorted(iteritems(kwargs)):
        if isinstance(value, (tuple, list)):
            if not value:
                clauses = [ 'true = false' ]
                break

        for proposed_type, pattern in iteritems(type_handler):
            if isinstance(value, proposed_type):
                clauses.append(pattern.format(key))
                break
        else:
            clauses.append("{0} = %({0})s".format(key))

    return ' and '.join(clauses)

def pg_conn(conn_info):
    conn_info = set_defaults(conn_info,
        cursor_factory = psycopg2.extras.DictCursor,
        autocommit     = False,
    )
    autocommit = conn_info.pop('autocommit')

    conn = psycopg2.connect(**conn_info)
    conn.autocommit = autocommit

    return conn

##############################################################################################################

import wizzat.objpool

class ConnMgr(wizzat.objpool.ObjPool):
    def __init__(self, conn_info, **kwargs):
        self.conn_info = conn_info
        super().__init__(**kwargs)

    def new_func(self):
        return pg_conn(self.conn_info)

    def put_func(self, conn):
        conn.rollback()
