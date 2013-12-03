from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import ClauseElement
import os

__all__ = [
    'DBTableMixin',
    'get_session',
    'default_session',
    'get_engine',
    'set_config',
]

# This doesn't currently work

class DBTableMixin(object):
    """
    Utility mixin for SQLAlchemy
    """
    @classmethod
    def get(cls, session = None, **kwargs):
        if not session:
            session = default_session()
        return session.query(cls).filter_by(**kwargs).first()

    @classmethod
    def get_all(cls, session = None, **kwargs):
        if not session:
            session = default_session()
        return session.query(cls).filter_by(**kwargs).all()

    @classmethod
    def get_or_create(cls, session = None, defaults=None, **kwargs):
        """
        With slight modification:
        http://stackoverflow.com/questions/2546207/does-sqlalchemy-have-an-equivalent-of-djangos-get-or-create
        """
        if not session:
            session = default_session()
        defaults = defaults or {}
        #kwargs = { k : v for k,v in kwargs.iteritems() if cls.wrapped_column(k) in cls.__table__.columns }
        instance = session.query(cls).filter_by(**kwargs).first()
        if instance:
            return instance
        else:
            params = { k : v for k, v in kwargs.iteritems() if not isinstance(v, ClauseElement) }
            params.update(defaults)
            instance = cls(**params)
            session.add(instance)
            return instance

_maker = None
def get_session():
    global _maker
    if not _maker:
        _maker = sessionmaker(bind=get_engine())
    return _maker()

_default_session = None
def default_session():
    global _default_session
    if not _default_session:
        _default_session = get_session()
    return _default_session

_engine = None
def get_engine():
    global _engine, _config

    if not _engine:
        _engine = create_engine(_config['db_conn_str'])
    return _engine

_config = None
def set_config(config):
    """
    Accepts a JSON application config.  For this particular application, it should look something like this:

    {
        "conn_infos" : {
            ...
            "db_name" : "some_conn_str",
            ...
        }
    }
    """
    global _config
    _config = config
