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

##############################################################################################################


class ConnMgr(object):
    """
    A PostgreSQL connection manager supporting 'named connections' and connection pooling.

    Example:
        m = ConnMgr(
            **db_conn_info
            minconn = 0,
            maxconn = 5,
        )

        conn1 = m.conn
        conn2 = m.iterconn
        conn3 = m.lockconn
        conn4 = m.fooconn
        conn5 = m.abcconn
        conn6 = m.defconn # This will hang
        m.putconn('abcconn')
        # Conn6 now unblocks

    Another pattern of use:
        m = ConnMgr.default_from_info(**config)
        conn1 = m.conn

        m = ConnMgr.default_from_info(**config)
        conn2 = m.conn
        conn3 = m.conn2

        conn1 == conn2
        conn2 != conn3
    """
    all_mgrs = []
    def __init__(self, **conn_info):
        self.conn_info = conn_info
        self.minconn   = self.conn_info.pop('minconn', 0)
        self.maxconn   = self.conn_info.pop('maxconn', 5)
        self.conn_info.setdefault('cursor_factory', psycopg2.extras.DictCursor)
        self.pool      = psycopg2.pool.ThreadedConnectionPool(self.minconn, self.maxconn, **self.conn_info)
        self.connections = {}
        self.all_mgrs.append(self)

    default_mgr = None
    @classmethod
    def default(cls):
        return cls.default_mgr

    @classmethod
    def default_from_info(cls, **info):
        if not cls.default_mgr or cls.default_mgr.pool.closed:
            mgr = cls(**info)
            mgr.setdefault()

        return cls.default_mgr

    def setdefault(self):
        type(self).default_mgr = self

    def getconn(self, name):
        if not hasattr(self, name):
            conn = self.pool.getconn()
            conn.autocommit = False
            self.connections[name] = conn
            setattr(self, name, conn)

        return getattr(self, name)

    def putconn(self, name, commit = True):
        conn = self.connections.pop(name)
        delattr(self, name)

        if commit:
            conn.commit()
        else:
            conn.rollback()

        self.pool.putconn(conn)

    def commit(self):
        for key, conn in iteritems(self.connections):
            conn.commit()

    def rollback(self):
        for key, conn in iteritems(self.connections):
            conn.rollback()

    def putall(self):
        for k in self.connections.keys():
            self.putconn(k)

    def closeall(self):
        self.putall()
        self.pool.closeall()
