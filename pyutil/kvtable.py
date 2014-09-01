import decorators
from util import set_defaults

__all__ = [
    'KVTableError',
    'KVTableConfigError',
    'KVTableImmutableFieldError',
    'KVTable',
    'DictKVTable',
]

class KVTableError(Exception): pass
class KVTableConfigError(KVTableError): pass
class KVTableImmutableFieldError(KVTableError): pass

class KVTableMeta(type):
    def __init__(cls, name, bases, dct):
        super(KVTableMeta, cls).__init__(name, bases, dct)
        if 'table_name' not in dct or not isinstance(dct['table_name'], basestring):
            raise KVTableConfigError("table_name is required, and should be a string")

        if 'fields' not in dct or not isinstance(dct['fields'], (list, tuple)):
            raise KVTableConfigError("fields is required, and should be a list or tuple")

        if 'key_fields' in dct:
            if not isinstance(dct['key_fields'], (list, tuple)):
                raise KVTableConfigError('key fields is not a list or tuple')

            def key_func(cls, args):
                if len(args) < len(dct['key_fields']):
                    raise ValueError("Insufficient keys for key_fields")

                return '{table_name}/{key}'.format(
                    table_name = cls.table_name,
                    key = '/'.join(unicode(x) for x in args[:len(dct['key_fields'])])
                )

            cls.key_func = classmethod(key_func)

            for key in dct['key_fields']:
                if key not in dct['fields']:
                    raise KVTableConfigError('{} (key field) is not in fields'.format(key))


        if dct.get('memoize'):
            cls.id_cache = decorators.create_cache_obj(
                max_size  = dct.get('memoize_size', 0),
                max_bytes = dct.get('memoize_bytes', 0),
            )

            cls.key_cache = decorators.create_cache_obj(
                max_size  = dct.get('memoize_size', 0),
                max_bytes = dct.get('memoize_bytes', 0),
            )


        cls.default_funcs = {}
        cls._conn = None
        for field in dct['fields']:
            func_name = 'default_{}'.format(field)

            if func_name in dct:
                cls.default_funcs[field] = dct[func_name]

            def getter(self, field=field):
                return self._data[field]

            def setter(self, new_value, field=field):
                if field in dct['key_fields']:
                    raise KVTableImmutableFieldError(field)

                if new_value != self._data.get(field):
                    self._changed = True
                    self._data[field] = new_value

            setattr(cls, field, property(
                getter,
                setter,
            ))


class KVTable(object):
    """
    Abstract micro-ORM for working with KV stores.

    Params:
    table_name:         string, the name of the "table" (object type) to query
    key_fields:         list[string], the names of the key fields (used in construction of the obj key)
    key_func:           Optionally provide your own key_func (usually generated from key_fields)
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
    __metaclass__ = KVTableMeta
    table_name    = ''
    memoize       = False
    key_fields    = []
    fields        = []

    def __init__(self, key, data, kv_data = None):
        self._key     = key
        self._data    = data
        self._kv_data = kv_data
        self._changed = False

        self.setup_fields()
        self.on_init()
        self.cache_obj(self)

    def setup_fields(self):
        self.changed = False
        for field in self.fields:
            self.init_field(field)

    def init_field(self, field):
        if field in self._data:
            pass
        elif field in self.default_funcs:
            self._data[field] = self.default_funcs[field](self)
        else:
            self._data[field] = None

    def on_init(self):
        pass

    def on_insert(self):
        pass

    def after_insert(self):
        pass

    def on_update(self):
        pass

    def after_update(self):
        pass

    @classmethod
    def check_key_cache(cls, key):
        if cls.memoize:
            return cls.key_cache.get(key, None)

    @classmethod
    def cache_obj(cls, obj):
        if cls.memoize and obj:
            cls.key_cache[obj._key] = obj

    @classmethod
    def clear_cache(cls):
        cls.key_cache.clear()

    @classmethod
    def uncache_obj(cls, obj):
        if cls.memoize and obj:
            cls.key_cache.pop(obj._key, None)

    @classmethod
    def find_by_key(cls, *keys):
        key = cls.key_func(keys)
        obj = cls.check_key_cache(key)

        if not obj:
            kv_data, data = cls._find_by_key(key)
            if kv_data:
                obj = cls(
                    key     = key,
                    data    = data,
                    kv_data = kv_data,
                )
                cls.cache_obj(obj)

        return obj

    @classmethod
    def create(cls, *keys, **kwargs):
        return cls(
            key     = cls.key_func(keys),
            data    = set_defaults(kwargs, { field : value for field, value in zip(cls.key_fields, keys) }),
            kv_data = None
        ).update()

    @classmethod
    def find_or_create(cls, *args, **kwargs):
        return cls.find_by_key(*args) or cls.create(*args, **kwargs)

    @classmethod
    def find_or_create_many(cls, *rows, **kwargs):
        for row in rows:
            return cls.find_or_create(*row, **kwargs)

    def update(self, force = False):
        """
        Ensures the row exists and is serialized to the data store
        """
        if self._kv_data:
            if force or self._changed:
                self.on_update()
                self._kv_data = self._update(force)
                self.after_update()
        else:
            self.on_insert()
            self._kv_data = self._insert(force)
            self.after_insert()

        self._changed = False

        return self

    def delete(self, force = False):
        """
        Deletes the object from the data store
        """
        self._kv_data = self._delete(force)

class DictKVTable(KVTable):
    table_name = ''
    memoize    = False
    key_fields = []
    fields     = []
    kv_store   = {}

    @classmethod
    def _find_by_key(cls, key):
        data = cls.kv_store.get(key, None)

        if data:
            return True, data
        else:
            return False, None

    def _insert(self, force=False):
        self.kv_store[self._key] = self._data
        return True

    def _update(self, force=False):
        self.kv_store[self._key] = self._data
        return True

    def _delete(self, force=False):
        del self.kv_store[self._key]
        return None
