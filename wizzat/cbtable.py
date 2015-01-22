from __future__ import (absolute_import, division, print_function, unicode_literals)
from builtins import *

import couchbase
import wizzat.kvtable

__all__ = [
    'CBTable',
]

class CBTable(wizzat.kvtable.KVTable):
    """
    This is a micro-ORM for working with Couchbase.  It attempts to work with CAS values
    as much as possible for maximum safety.  It's relatively easy to structure concurrency
    around KeyExistsError.

    Relevant options (on top of KVTable options):
    - replicate_to: int, the number of nodes to replicate the change to
    - persist_to:   int, the number of nodes to persist (to disk) the change to
    """
    memoize       = False
    table_name    = ''
    key_fields    = []
    fields        = []
    persist_to    = 0
    replicate_to  = 0

    @classmethod
    def _find_by_key(cls, kv_key):
        try:
            rv = cls.conn.get(kv_key)
            return rv, rv.value
        except couchbase.exceptions.NotFoundError:
            return None, None

    def _insert(self, force=False):
        return self.conn.add(self._key, self._data,
            persist_to   = self.persist_to,
            replicate_to = self.replicate_to,
        )

    def _update(self, force=False):
        return self.conn.set(self._key, self._data,
            persist_to   = self.persist_to,
            replicate_to = self.replicate_to,
            cas          = self._kv_data.cas,
        )

    def _delete(self, force=False):
        return self.conn.delete(self._key,
            persist_to   = self.persist_to,
            replicate_to = self.replicate_to,
            cas          = self._kv_data.cas,
        )
