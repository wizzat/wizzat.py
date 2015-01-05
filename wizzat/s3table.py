try:
    import boto.exception
    import kvtable
    import cStringIO
    import json
    from decorators import memoize
    from boto.s3.key import Key, compute_md5

    __all__ = [
        'S3Table',
    ]

    class S3Table(kvtable.KVTable):
        """
        This is a micro-ORM for working with S3.

        Relevant options (on top of KVTable options):
        - bucket:               The S3 bucket name to store this table in
        - json_encoder:         func, the json encoder (typically, staticmethod(json.dumps))
        - json_encoder:         func, the json decoder (typically, staticmethod(json.loads))
        - reduced_redundancy:   bool, Whether or not to store the key with S3 reduced redundancy
        - encrypt_key:          bool, Use S3 encryption
        - policy:               CannedACLStrings, The S3 policy to apply to new objects in S3
        """
        memoize            = False
        table_name         = ''
        key_fields         = []
        fields             = []
        bucket             = None
        policy             = None
        encrypt_key        = False
        reduced_redundancy = False
        json_encoder       = staticmethod(json.dumps)
        json_decoder       = staticmethod(json.loads)

        @classmethod
        def _remote_bucket(cls):
            return cls.conn.get_bucket(cls.bucket)

        @classmethod
        def delete_key(cls, kv_key):
            cls._remote_bucket().delete_key(kv_key)

        @classmethod
        def _remote_key(cls, kv_key):
            return Key(cls._remote_bucket(), kv_key)

        @classmethod
        def _find_by_key(cls, kv_key):
            try:
                content_str = cls._remote_key(kv_key).get_contents_as_string()
                return True, cls.json_decoder(content_str)
            except boto.exception.S3ResponseError:
                return None, None


        def _insert(self, force=False):
            content_str = self.json_encoder(self._data)
            md5, b64, file_size = compute_md5(cStringIO.StringIO(content_str))

            self._remote_key(self._key).set_contents_from_string(content_str,
                md5                = (md5, b64, file_size),
                policy             = self.policy,
                encrypt_key        = self.encrypt_key,
                reduced_redundancy = self.reduced_redundancy,
            )

            return True

        _update = _insert

        def _delete(self, force=False):
            self.delete_key(self._key)
            return False
except ImportError:
    pass
