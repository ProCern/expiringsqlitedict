#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This code is distributed under the terms and conditions
# from the Apache License, Version 2.0
#
# http://opensource.org/licenses/apache2.0.php
#
# This code was inspired by:
#  * http://code.activestate.com/recipes/576638-draft-for-an-sqlite3-based-dbm/
#  * http://code.activestate.com/recipes/526618/

"""
A lightweight wrapper around Python's sqlite3 database, with a MutableMapping interface:

>>> with SqliteDict('some.db') as mydict: # the mapping will be persisted to file `some.db`
>>>     mydict['some_key'] = any_picklable_object
>>>     print mydict['some_key']
>>>     print len(mydict) # etc... all dict functions work

You can specify your own serializer as well, with ``loads()`` and ``dumps()`` methods.

>>> class JsonSerializer:
>>>     @staticmethod
>>>     def loads(data: sqlite3.Binary) -> Any:
>>>         return json.loads(data)
>>>     @staticmethod
>>>     def dumps(value: Any) -> sqlite3.Binary:
>>>         json.dumps(data)
>>>
>>> with SqliteDict('some.db', serializer=JsonSerializer()) as mydict:
>>>     mydict['some_key'] = some_json_encodable_object
>>>     print mydict['some_key']
"""

from datetime import timedelta
import pickle
import sqlite3
import zlib
from typing import Any
from collections import MutableMapping

class ZlibPickleSerializer:
    '''Serializer that pickles and optionally zlib-compresses data.
    '''

    @staticmethod
    def dumps(obj: Any) -> sqlite3.Binary:
        """Serialize an object using pickle to a binary format accepted by SQLite."""

        pickled = pickle.dumps(obj)
        compressed = zlib.compress(pickled)

        # If compression didn't fail to save space:
        if len(compressed) < len(pickled):
            data = b'Z' + compressed
        else:
            data = b'R' + pickled

        return sqlite3.Binary(data)

    @staticmethod
    def loads(data: sqlite3.Binary) -> Any:
        """Deserialize objects retrieved from SQLite."""
        flag = data[0:1]
        data = data[1:]

        if flag == b'Z':
            pickled = zlib.decompress(data)
        else:
            pickled = data

        return pickle.loads(pickled)

class SqliteDict:
    """
    Set up the sqlite dictionary manager.

    This needs to be used as a context manager.  It will not operate at all otherwise.
    args and kwargs are directly passed to sqlite3.connect.  Use these to
    customize your connection, such as making it read-only.
    """

    def __init__(self, *args, serializer = ZlibPickleSerializer(), lifespan: timedelta = timedelta(days=7), **kwargs):
        self._db = sqlite3.connect(*args, **kwargs)
        self._db.isolation_level = None
        self._serializer = serializer
        self._lifespan = lifespan

    def __enter__(self):
        self._db.execute('BEGIN TRANSACTION')
        return Connection(
            self._db,
            serializer=self._serializer,
            lifespan=self._lifespan,
        )

    def __exit__(self, type, value, traceback):
        if type is None:
            self._db.execute('COMMIT')
        else:
            self._db.execute('ROLLBACK')

    def __del__(self):
        self._db.close()

def AutocommitSqliteDict(*args, serializer = ZlibPickleSerializer(), lifespan: timedelta = timedelta(days=7), **kwargs):
    """
    Set up the sqlite dictionary manager as a non-contextmanager in autocommit mode.
    """
    db = sqlite3.connect(*args, **kwargs)
    db.isolation_level = None
    return Connection(
        db,
        serializer=serializer,
        lifespan=lifespan,
    )

class Connection(MutableMapping):
    def __init__(self, connection: sqlite3.Connection, serializer, lifespan: timedelta) -> None:
        self._lifespan = lifespan.total_seconds();
        self._serializer = serializer
        self._connection = connection

        self._connection.execute('''
        CREATE TABLE IF NOT EXISTS expiringsqlitedict (
            key TEXT UNIQUE NOT NULL,
            expire INTEGER NOT NULL,
            value BLOB NOT NULL)
        ''')
        self._connection.execute('CREATE INDEX IF NOT EXISTS expiringsqlitedict_expire_index ON expiringsqlitedict (expire)')

        self._connection.execute(
            '''
            CREATE TRIGGER IF NOT EXISTS expiringsqlitedict_insert_trigger AFTER INSERT ON expiringsqlitedict
            BEGIN
                DELETE FROM expiringsqlitedict WHERE expire <= strftime('%s', 'now');
            END
            '''
        )

        self._connection.execute(
            '''
            CREATE TRIGGER IF NOT EXISTS expiringsqlitedict_update_trigger AFTER UPDATE ON expiringsqlitedict
            BEGIN
                DELETE FROM expiringsqlitedict WHERE expire <= strftime('%s', 'now');
            END
            '''
        )

    def __len__(self):
        for row in self._connection.execute('SELECT COUNT(*) FROM expiringsqlitedict'):
            return row[0]
        return 0

    def __bool__(self):
        return len(self) > 0

    def keys(self):
        for row in self._connection.execute('SELECT key FROM expiringsqlitedict'):
            yield row[0]

    __iter__ = keys

    def values(self):
        for row in self._connection.execute('SELECT value FROM expiringsqlitedict'):
            yield self._serializer.loads(row[0])

    def items(self):
        for row in self._connection.execute('SELECT key, value FROM expiringsqlitedict'):
            yield row[0], self._serializer.loads(row[1])

    def __contains__(self, key: str):
        for _ in self._connection.execute('SELECT 1 FROM expiringsqlitedict WHERE key = ?', (key,)):
            return True
        return False

    def __getitem__(self, key):
        for row in self._connection.execute('SELECT value FROM expiringsqlitedict WHERE key = ?', (key,)):
            return self._serializer.loads(row[0])
        raise KeyError(key)

    def __setitem__(self, key, value):
        self._connection.execute(
            "REPLACE INTO expiringsqlitedict (key, expire, value) VALUES (?, strftime('%s', 'now') + ?, ?)",
            (key, self._lifespan, self._serializer.dumps(value)),
            )

    def __delitem__(self, key):
        if key not in self:
            raise KeyError(key)
        self._connection.execute('DELETE FROM expiringsqlitedict WHERE key=?', (key,))

    def clear(self):
        self._connection.execute('DELETE FROM expiringsqlitedict')
