try:
    from psycopg2cffi import compat
    compat.register()
except ImportError:
    pass

import psycopg2
import psycopg2.extras

__all__ = [
    'execute',
    'iter_results',
    'fetch_results',
    'set_sql_log_func',
    'relation_info',
    'table_exists',
    'view_exists',
    #'vacuum',
    'currval',
    'nextval',
    'DBTable',
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

def iter_results(conn, sql, **bind_params):
    """
    Delays fetching the SQL results into memory until iteration
    Keeps memory footprint low
    """
    global _log_func
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        if _log_func:
            bound_sql = cur.mogrify(sql, bind_params)
            _log_func(bound_sql)

        cur.execute(sql, bind_params)
        for row in cur:
            yield row

def fetch_results(conn, sql, **bind_params):
    """
    Immediatly fetches the SQL results into memory
    Trades memory for the ability to immediately execute another query
    """
    global _log_func
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        if _log_func:
            bound_sql = cur.mogrify(sql, bind_params)
            _log_func(bound_sql)

        try:
            cur.execute(sql, bind_params)
        except psycopg2.ProgrammingError:
            print cur.mogrify(sql, bind_params)
            raise
        return cur.fetchall()

def copy_from(conn, fp, table_name, columns = None):
    fp.seek(0)
    conn.cursor().copy_from(fp, table_name, columns = columns)

def relation_info(conn, relname, relkind = 'r'):
    """
    Fetch object information from the pg catalog
    """
    return fetch_results(conn, """
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
    """
    Obtains the current value of a sequence
    """
    return fetch_results(conn, "select currval(%(sequence)s)", sequence = sequence)[0][0]

def nextval(conn, sequence):
    """
    Obtains the next value of a sequence
    """
    return fetch_results(conn, "select nextval(%(sequence)s)", sequence = sequence)[0][0]

def sql_where_from_params(**kwargs):
    """
    Utility function for converting a param dictionary into a where clause
    Lists and tuples become in clauses
    """
    clauses = [ 'true' ]
    for key, value in kwargs.iteritems():
        if isinstance(value, list) or isinstance(value, tuple):
            if not value:
                clauses = [ 'true = false' ]
                break

        clauses.append({
            None  : "{0} is null".format(key),
            list  : "{0} in (%({0})s)".format(key),
            tuple : "{0} in (%({0})s)".format(key),
        }.get(type(value), "{0} = %({0})s".format(key)))

    return ' and '.join(clauses)

##############################################################################################################

class DBTable(object):
    """
    This is a micro-ORM for the purposes of not having dependencies on Django or SQLAlchemy.
    """

    def __init__(self, _is_in_db = False, **kwargs):
        self.db_fields = kwargs if _is_in_db else {}

        for field in self.fields:
            setattr(self, field, kwargs.get(field, None))

    def get_dict(self):
        return { field : getattr(self, field) for field in self.fields }

    @classmethod
    def find_by(cls, **kwargs):
        """
        Returns rows which match all key/value pairs
        Additionally, accepts for_update = True/False, nowait = True/False
        """
        for_update = kwargs.pop('for_update', False)
        nowait = kwargs.pop('nowait', False)

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

        for row in iter_results(cls.conn, sql, **kwargs):
            yield cls(_is_in_db = True, **row)

    def lock_for_processing(self, nowait = False):
        """
        Locks a row in the database for update.  Requires a primary key.
        """
        nowait = "nowait" if nowait else ""
        sql = "SELECT * FROM {table_name} WHERE {key_field} = %({key_field})s FOR UPDATE {nowait}".format(
            table_name = self.table_name,
            key_field  = self.key_field,
            nowait     = nowait,
        )

        execute(self.conn, sql, **self.get_dict())

        return self

    def update(self):
        if self.db_fields:
            return self._update()
        else:
            return self._insert()

    def _insert(self):
        """
        Inserts a row into the database, and returns that row.
        """
        kv = { x:y for x,y in self.get_dict().iteritems() if y }
        fields = kv.keys()
        values = [ kv[x] for x in fields ]
        sql = "INSERT INTO {table_name} ({fields}) VALUES ({values}) RETURNING *".format(
            table_name = self.table_name,
            fields = ', '.join(fields),
            values = ', '.join([ "%({})s".format(x) for x in fields ]),
        )

        self.db_fields = dict(fetch_results(self.conn, sql, **kv)[0])
        assert self.db_fields

        for k, v in self.db_fields.iteritems():
            setattr(self, k, v)

        return self

    def _update(self):
        """
        Updates a row in the database, and returns that row.
        """
        new_values = self.get_dict()
        bind_params = { x : new_values[x] for x in self.fields if new_values[x] != self.db_fields[x] }
        if not bind_params:
            return self

        field_equality = ', '.join([ "{0} = %({0})s".format(x) for x in bind_params.iterkeys() ])

        if self.key_field:
            assert getattr(self, self.key_field) == self.db_fields[self.key_field]
            filter_clause = '{0} = %({0})s'.format(self.key_field)
            bind_params[self.key_field] = getattr(self, self.key_field)
        else:
            filter_clause = ' and '.join([ '{0} = %(orig_{0})s'.format(field) for field in self.db_fields ])
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

        self.db_fields = dict(fetch_results(self.conn, sql, **bind_params)[0])
        assert self.db_fields
        for k, v in self.db_fields.iteritems():
            setattr(self, k, v)

        return self

    def delete(self):
        if not self.db_fields:
            return []

        if self.key_field:
            sql = "DELETE FROM {table_name} WHERE {key_field} = %({key_field})s RETURNING *".format(
                table_name = self.table_name,
                key_field  = self.key_field,
            )
        else:
            field_equality = ' AND '.join([ "{0} = %({0})s".format(x) for x in self.db_fields.iterkeys() ])
            sql = "DELETE FROM {table_name} WHERE {field_equality} RETURNING *".format(
                table_name = self.table_name,
                key_field  = self.key_field,
                field_equality = field_equality,
            )

        return fetch_results(self.conn, sql, **self.db_fields)

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()
