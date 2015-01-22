from __future__ import (absolute_import, division, print_function, unicode_literals)
from builtins import *
from future.utils import with_metaclass

import copy
import types
import wizzat.decorators
from wizzat.pghelper import *
from wizzat.util import set_defaults

__all__ = [
    'DBTable',
    'DBTableError',
    'DBTableConfigError',
    'DBTableImmutableFieldError',
]

class DBTableError(Exception): pass
class DBTableConfigError(DBTableError): pass
class DBTableImmutableFieldError(DBTableError): pass

class DBTableMeta(type):
    def __init__(cls, name, bases, dct):
        super(DBTableMeta, cls).__init__(name, bases, dct)
        if 'table_name' not in dct or not isinstance(dct['table_name'], str):
            raise DBTableConfigError("table_name is required, and should be a string")

        if 'fields' not in dct or not isinstance(dct['fields'], (list, tuple)):
            raise DBTableConfigError("fields is required, and should be a list or tuple")

        if 'id_field' in dct:
            if not isinstance(dct['id_field'], (type(None), str)):
                raise DBTableConfigError('id_field is not required, but should be a string or None')
        else:
            cls.id_field = None

        if 'key_fields' in dct:
            if not isinstance(dct['key_fields'], (list, tuple)):
                raise DBTableConfigError('key_fields is not required, but should be a list of strings or None')
            for field in dct['key_fields']:
                if not isinstance(field, str):
                    raise DBTableConfigError('key_fields is not required, but should be a list of strings or None')
        else:
            cls.key_fields = []

        if dct.get('id_field') and dct['id_field'] not in dct['fields']:
            raise DBTableConfigError('id field {} not in fields'.format(dct['id_field']))

        for field in dct.get('key_fields', []):
            if field not in dct['fields']:
                raise DBTableConfigError('key field {} not in fields'.format(field))

        if dct.get('memoize'):
            cls.id_cache = wizzat.decorators.create_cache_obj(
                max_size  = dct.get('memoize_size', 0),
                max_bytes = dct.get('memoize_bytes', 0),
            )

            cls.key_cache = wizzat.decorators.create_cache_obj(
                max_size  = dct.get('memoize_size', 0),
                max_bytes = dct.get('memoize_bytes', 0),
            )

        cls._conn = None
        cls.default_funcs = {}

        for field in dct['fields']:
            func_name = 'default_{}'.format(field)
            if func_name in dct:
                cls.default_funcs[field] = dct[func_name]


class DBTable(with_metaclass(DBTableMeta)):
    """
    This is a micro-ORM for the purposes of not having dependencies on Django or SQLAlchemy.
    Philosophically, it also supports merely the object abstraction and super simple sql generation.
    It requires full knowledge of SQL.

    Params:
    table_name:         string, the name of the table to query
    id_field:           string, the name of the id field (generally a surrogate key)
    key_fields:         list[string], the names of the key fields (generally primary or unique key)
    fields:             list[string], the names of all fields on the object
    --
    memoize:            bool, caches objects from the database locally
    memoize_size:       int, maximum number of objects to cache from the database (LRU ejection)
    memoize_bytes:      int, maximum size objects to cache from the database (LRU ejection).
                        Note that there are two caches, and while references are shared the
                        cache size here is not absolute.
    default_{field}:    func, define functions for default behaviors.  These functions are executed
                        in order of definition in the fields array.

    """
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
        if cls.memoize:
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
    def find_or_create_many(cls, *rows):
        for row in rows:
            return cls.find_or_create(*row)

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

    def should_update(self):
        curr_values = self.to_dict()
        return any(curr_values[field] != self.db_fields[field] for field in self.fields)

    def update(self, force = False):
        """
        Ensures the row exists is serialized to the database
        """
        if self.db_fields:
            if force or self.should_update():
                self.on_update()
                self._update(force)
                self.after_update()
        else:
            self.on_insert()
            self._insert(force)
            self.after_insert()

        return self

    def on_insert(self):
        pass

    def after_insert(self):
        pass

    def on_update(self):
        pass

    def after_update(self):
        pass

    def _insert(self, force = False):
        """
        Inserts a row into the database, and returns that row.
        """
        kv = { x:y for x,y in self.to_dict().items() if y != None }
        fields = kv.keys()
        values = [ kv[x] for x in fields ]
        sql = "INSERT INTO {table_name} ({fields}) VALUES ({values}) RETURNING *".format(
            table_name = self.table_name,
            fields = ', '.join(fields),
            values = ', '.join([ "%({})s".format(x) for x in fields ]),
        )

        self.db_fields = fetch_results(self.conn, sql, **kv)[0]
        assert self.db_fields

        for k, v in self.db_fields.items():
            setattr(self, k, copy.deepcopy(v))

    def _update(self, force = False):
        """
        Updates a row in the database, and returns that row.
        """
        new_values = self.to_dict()
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
                if not force and getattr(self, key_field) != self.db_fields[key_field]:
                    raise ValueError("key field {} changed from {} to {}".format(
                        key_field,
                        self.db_fields[key_field],
                        getattr(self, key_field),
                    ))

        field_equality = ', '.join([ "{0} = %({0})s".format(x) for x in bind_params.keys() ])

        if self.id_field:
            fields = [ self.id_field ]
        elif self.key_fields:
            fields = self.key_fields
        else:
            fields = self.db_fields.keys()

        filter_clause = ' and '.join([ '{0} = %(orig_{0})s'.format(field) for field in fields ])
        bind_params.update({ "orig_{}".format(x) : y for x, y in self.db_fields.items() })

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
        for k, v in self.db_fields.items():
            setattr(self, k, copy.deepcopy(v))

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

