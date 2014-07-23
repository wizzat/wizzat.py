try:
    from psycopg2cffi import compat
    compat.register()
except ImportError:
    pass

import copy, tempfile, types, cStringIO
import psycopg2, psycopg2.extras, psycopg2.pool
from types import *
from sqlhelper import *
from util import set_defaults

__all__ = [
    #'vacuum',
    'ConnMgr',
    'DBTable',
    'analyze',
    'copy_from',
    'copy_from_rows',
    'currval',
    'drop_table',
    'execute',
    'fetch_results',
    'fetch_one',
    'iter_results',
    'nextval',
    'relation_info',
    'set_sql_log_func',
    'table_columns',
    'table_exists',
    'view_exists',
    'PgIntegrityError',
    'PgOperationalError',
    'PgProgrammingError',
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
    fp = cStringIO.StringIO()
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
    for key, value in sorted(kwargs.iteritems()):
        if isinstance(value, list) or isinstance(value, tuple):
            if not value:
                clauses = [ 'true = false' ]
                break

        clauses.append({
            NoneType : "{0} is null".format(key),
            list     : "{0} in (%({0})s)".format(key),
            tuple    : "{0} in %({0})s".format(key),
        }.get(type(value), "{0} = %({0})s".format(key)))

    return ' and '.join(clauses)

##############################################################################################################

class DBTableError(Exception): pass
class DBTableConfigError(DBTableError): pass
class DBTableImmutableFieldError(DBTableError): pass

class DBTableMeta(type):
    def __init__(cls, name, bases, dct):
        super(DBTableMeta, cls).__init__(name, bases, dct)
        if 'table_name' not in dct or not isinstance(dct['table_name'], basestring):
            raise DBTableConfigError("table_name is required, and should be a string")

        if 'fields' not in dct or not isinstance(dct['fields'], (list, tuple)):
            raise DBTableConfigError("fields is required, and should be a list or tuple")

        if 'id_field' in dct:
            if not isinstance(dct['id_field'], (basestring, types.NoneType)):
                raise DBTableConfigError('id_field is not required, but should be a string or None')
        else:
            cls.id_field = None

        if 'key_fields' in dct:
            if not isinstance(dct['key_fields'], (list, tuple)):
                raise DBTableConfigError('key_fields is not required, but should be a list of strings or None')
            for field in dct['key_fields']:
                if not isinstance(field, basestring):
                    raise DBTableConfigError('key_fields is not required, but should be a list of strings or None')
        else:
            cls.key_fields = []

        if dct.get('id_field') and dct['id_field'] not in dct['fields']:
            raise DBTableConfigError('id field {} not in fields'.format(dct['id_field']))

        for field in dct.get('key_fields', []):
            if field not in dct['fields']:
                raise DBTableConfigError('key field {} not in fields'.format(field))

        cls.id_cache      = {}
        cls.key_cache     = {}
        cls.default_funcs = {}
        cls._conn         = None

        for field in dct['fields']:
            func_name = 'default_{}'.format(field)
            if func_name in dct:
                cls.default_funcs[field] = dct[func_name]


class DBTable(object):
    """
    This is a micro-ORM for the purposes of not having dependencies on Django or SQLAlchemy.
    Philosophically, it also supports merely the object abstraction and super simple sql generation.
    It requires full knowledge of SQL.
    """
    __metaclass__ = DBTableMeta
    memoize       = False
    table_name    = ''
    id_field      = ''
    key_fields    = []
    fields        = []

    def __init__(self, _is_in_db = False, **kwargs):
        self.db_fields = kwargs if _is_in_db else {}

        for field in self.fields:
            if field in kwargs:
                field_value = kwargs[field]
            elif field in self.default_funcs:
                field_value = self.default_funcs[field](self)
            else:
                field_value = None

            setattr(self, field, copy.deepcopy(field_value))

        self.on_init()
        self.cache_obj(self)

    def on_init(self):
        pass

    def get_dict(self):
        return { field : copy.deepcopy(getattr(self, field)) for field in self.fields }

    @classmethod
    def check_key_cache(cls, key_fields):
        if cls.memoize:
            cache_key = tuple(key_fields)
            return cls.key_cache.get(cache_key, None)

    @classmethod
    def check_id_cache(cls, id):
        if cls.memoize:
            return cls.id_cache.get(id, None)

    @classmethod
    def cache_obj(cls, obj):
        if cls.memoize:
            if obj and cls.id_field:
                cache_key = getattr(obj, cls.id_field)
                cls.id_cache[cache_key] = obj

            if obj and cls.key_fields:
                cache_key = tuple(getattr(obj, field) for field in cls.key_fields)
                cls.key_cache[cache_key] = obj

    @classmethod
    def clear_cache(cls):
        cls.id_cache.clear()
        cls.key_cache.clear()

    @classmethod
    def uncache_obj(cls, obj):
        if cls.id_field:
            cache_key = getattr(obj, cls.id_field)
            cls.id_cache.pop(cache_key, None)

        if cls.key_fields:
            cache_key = tuple(getattr(obj, field) for field in cls.key_fields)
            cls.key_cache.pop(cache_key, None)

    @classmethod
    def find_by_id(cls, id):
        obj = cls.check_id_cache(id)
        if obj:
            return obj

        return cls.find_one(**{ cls.id_field : id })

    @classmethod
    def find_by_key(cls, *keys):
        obj = cls.check_key_cache(keys)
        if obj:
            return obj

        return cls.find_one(**{ field : value for field,value in zip(cls.key_fields, keys) })

    @classmethod
    def find_one(cls, **kwargs):
        found = list(cls.find_by(**kwargs))
        if not found:
            return None
        assert len(found) == 1
        return found[0]

    @classmethod
    def create(cls, *keys, **kwargs):
        kwargs = set_defaults(kwargs, { field : value for field, value in zip(cls.key_fields, keys) })
        return cls(**kwargs).update()

    @classmethod
    def find_or_create(cls, *args, **kwargs):
        return cls.find_by_key(*args) or cls.create(*args, **kwargs)

    @classmethod
    def find_by(cls, for_update = False, nowait = False, **kwargs):
        """
        Returns rows which match all key/value pairs
        Additionally, accepts for_update = True/False, nowait = True/False
        """
        for_update = 'for update' if for_update else ''
        nowait = 'nowait' if nowait else ''

        sql = """
            SELECT *
            FROM {table_name}
            where {where_clause}
            {for_update} {nowait}
        """.format(
            table_name = cls.table_name,
            where_clause = sql_where_from_params(**kwargs),
            for_update = for_update,
            nowait = nowait,
        )

        return cls.find_by_sql(sql, **kwargs)

    @classmethod
    def find_by_sql(cls, sql, **bind_params):
        for row in iter_results(cls.conn, sql, **bind_params):
            yield cls(_is_in_db = True, **row)

    def rowlock(self, nowait = False):
        """
        Locks a row in the database for update.  Requires a primary key.
        """
        nowait = "nowait" if nowait else ""
        if self.id_field:
            fields = [ self.id_field ]
        elif self.key_fields:
            fields = self.key_fields
        else:
            fields = self.fields

        filter_clause = ' and '.join([ '{0} = %(orig_{0})s'.format(field) for field in fields ])
        bind_params = { 'orig_{}'.format(x) : self.db_fields[x] for x in fields }

        sql = """
            select *
            from {table_name}
            where {filter_clause}
            for update
            {nowait}
        """.format(
            table_name    = self.table_name,
            filter_clause = filter_clause,
            nowait        = nowait
        )

        execute(self.conn, sql, **bind_params)

        return self

    def update(self):
        """
        Ensures the row exists is serialized to the database
        """
        if self.db_fields:
            self.on_update()
            return self._update()
        else:
            self.on_insert()
            obj = self._insert()
            self.after_insert()

            return obj

    def on_insert(self):
        pass

    def after_insert(self):
        pass

    def _insert(self):
        """
        Inserts a row into the database, and returns that row.
        """
        kv = { x:y for x,y in self.get_dict().iteritems() if y != None }
        fields = kv.keys()
        values = [ kv[x] for x in fields ]
        sql = "INSERT INTO {table_name} ({fields}) VALUES ({values}) RETURNING *".format(
            table_name = self.table_name,
            fields = ', '.join(fields),
            values = ', '.join([ "%({})s".format(x) for x in fields ]),
        )

        self.db_fields = fetch_results(self.conn, sql, **kv)[0]
        assert self.db_fields

        for k, v in self.db_fields.iteritems():
            setattr(self, k, copy.deepcopy(v))

        return self

    def on_update(self):
        pass

    def _update(self):
        """
        Updates a row in the database, and returns that row.
        """
        new_values = self.get_dict()
        bind_params = { x : new_values[x] for x in self.fields if new_values[x] != self.db_fields[x] }
        if not bind_params:
            return self

        # Verify id field didn't change
        if self.id_field:
            if getattr(self, self.id_field) != self.db_fields[self.id_field]:
                raise ValueError("id field {} changed from {} to {}".format(
                    self.id_field,
                    self.db_fields[self.id_field],
                    getattr(self, self.id_field),
                ))

        # Verify key fields didn't change
        if self.key_fields:
            for key_field in self.key_fields:
                if getattr(self, key_field) != self.db_fields[key_field]:
                    raise ValueError("key field {} changed from {} to {}".format(
                        key_field,
                        self.db_fields[key_field],
                        getattr(self, key_field),
                    ))

        field_equality = ', '.join([ "{0} = %({0})s".format(x) for x in bind_params.iterkeys() ])

        if self.id_field:
            fields = [ self.id_field ]
        elif self.key_fields:
            fields = self.key_fields
        else:
            fields = self.db_fields.keys()

        filter_clause = ' and '.join([ '{0} = %(orig_{0})s'.format(field) for field in fields ])
        bind_params.update({ "orig_{}".format(x) : y for x, y in self.db_fields.iteritems() })

        sql = """
            UPDATE {table_name}
            SET {field_equality}
            WHERE {filter_clause}
            RETURNING *
        """.format(
            table_name     = self.table_name,
            field_equality = field_equality,
            filter_clause  = filter_clause,
        )

        self.db_fields = fetch_results(self.conn, sql, **bind_params)[0]
        assert self.db_fields
        for k, v in self.db_fields.iteritems():
            setattr(self, k, copy.deepcopy(v))

        return self

    def delete(self):
        """
        Deletes row(s) in the database that share all fields with the current row, and returns those rows.
        """
        if not self.db_fields:
            return []

        if self.id_field:
            fields = [ self.id_field ]
        elif self.key_fields:
            fields = self.key_fields
        else:
            fields = self.fields

        filter_clause = ' and '.join([ '{0} = %(orig_{0})s'.format(field) for field in fields ])
        bind_params = { 'orig_{}'.format(x) : self.db_fields[x] for x in fields }

        sql = """
            DELETE FROM {table_name}
            WHERE {filter_clause}
            RETURNING *
        """.format(
            table_name    = self.table_name,
            filter_clause = filter_clause,
        )

        objs = fetch_results(self.conn, sql, **bind_params)
        assert objs

        return objs

    def to_dict(self):
        return { field : getattr(self, field) for field in self.fields }

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    @property
    def conn(cls):
        return cls._conn

    @conn.setter
    def get_conn(cls, new_conn):
        cls._conn = new_conn


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
        for key, conn in self.connections.iteritems():
            conn.commit()

    def rollback(self):
        for key, conn in self.connections.iteritems():
            conn.rollback()

    def putall(self):
        for k in self.connections.keys():
            self.putconn(k)

    def closeall(self):
        self.putall()
        self.pool.closeall()
