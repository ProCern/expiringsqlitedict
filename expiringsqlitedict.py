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
A lightweight wrapper around Python's sqlite3 database, with a MutableMapping interface::

    with SqliteDict('some.db') as mydict: # the mapping will be persisted to file `some.db`
        mydict['some_key'] = any_picklable_object
        print(mydict['some_key'])
        print(len(mydict)) # etc... all dict functions work

You can specify your own serializer as well, with ``loads()`` and ``dumps()`` methods::

    class JsonSerializer:
        @staticmethod
        def loads(data: str) -> Any:
            return json.loads(data)

        @staticmethod
        def dumps(value: Any) -> str:
            return json.dumps(value, separators=(',', ':'))

You can also actually just use the json module itself instead, if you don't want to customize dumping or loading.

Note that the loads and dumps should take in data in types that it expects
sqlite to hold, and put out data that sqlite can store.  If you expect a bytes
but sqlite gives you a string, that's on you.  You should do proper type
checking, or make sure you never put data in a type you don't want to get back.
"""

from collections.abc import MutableMapping
from contextlib import contextmanager
from datetime import timedelta
from typing import Any, Iterator, Tuple
import json
import pickle
import sqlite3
import zlib

@contextmanager
def _cursor(db: sqlite3.Connection):
    '''Wrap the cursor in a context manager that closes it on exit, rather than waiting until __del__.
    '''
    cur = db.cursor()
    try:
        yield cur
    finally:
        cur.close()

class ZlibPickleSerializer:
    '''Serializer that pickles and optionally zlib-compresses data.
    '''

    @staticmethod
    def dumps(value: Any) -> bytes:
        """Serialize an object using pickle to a binary format accepted by SQLite."""

        pickled = pickle.dumps(value)
        compressed = zlib.compress(pickled)

        # If compression didn't fail to save space:
        if len(compressed) < len(pickled):
            data = b'Z' + compressed
        else:
            data = b'R' + pickled

        return data

    @staticmethod
    def loads(data: bytes) -> Any:
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

    def __init__(self, *args, serializer: Any = json, lifespan: timedelta = timedelta(days=7), transaction: str = 'IMMEDIATE', **kwargs) -> None:
        self._db = sqlite3.connect(*args, isolation_level=None, **kwargs)
        with _cursor(self._db) as cursor:
            cursor.execute('PRAGMA journal_mode=WAL')
            cursor.execute('PRAGMA synchronous=NORMAL')
        self._serializer = serializer
        self._lifespan = lifespan
        self._begin = f'BEGIN {transaction} TRANSACTION'

    def __enter__(self) -> 'Connection':
        if self._db is None:
            raise RuntimeError('Can not use after close')
        with _cursor(self._db) as cursor:
            cursor.execute(self._begin)

        return Connection(
            self._db,
            serializer=self._serializer,
            lifespan=self._lifespan,
        )

    def __exit__(self, type, value, traceback) -> None:
        if self._db is None:
            raise RuntimeError('Can not use after close')

        if (type and value and traceback) is None:
            with _cursor(self._db) as cursor:
                cursor.execute('COMMIT')
        else:
            with _cursor(self._db) as cursor:
                cursor.execute('ROLLBACK')

    def close(self):
        if self._db is not None:
            try:
                with _cursor(self._db) as cursor:
                    cursor.execute('PRAGMA analysis_limit=8192')
                    cursor.execute('PRAGMA optimize')
                self._db.close()
            finally:
                self._db = None

    def __del__(self):
        self.close()

class OnDemand:
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._manager = None

    def __enter__(self):
        if self._manager is not None:
            raise RuntimeError("Can not enter SelfContained before it has been exited")

        self._manager = SqliteDict(*self._args, **self._kwargs)
        return self._manager.__enter__()

    def __exit__(self, type, value, traceback):
        if self._manager is None:
            raise RuntimeError("Can not exit SelfContained before it has been entered")

        try:
            try:
                self._manager.__exit__(type, value, traceback)
            finally:
                self._manager.close()
        finally:
            self._manager = None

def AutocommitSqliteDict(*args, serializer: Any = json, lifespan: timedelta = timedelta(days=7), **kwargs) -> 'Connection':
    """
    Set up the sqlite dictionary manager as a non-contextmanager in autocommit mode.
    Kwargs in this dict may customize the isolation_level, if you wish.

    Unlike the normal SqliteDict, this won't try to optimize the database on __del__.
    """

    if 'isolation_level' not in kwargs:
        kwargs['isolation_level'] = None

    db = sqlite3.connect(*args, **kwargs)
    with _cursor(db) as cursor:
        cursor.execute('PRAGMA journal_mode=WAL')
        cursor.execute('PRAGMA synchronous=NORMAL')

    return Connection(
        db,
        serializer=serializer,
        lifespan=lifespan,
    )

class Connection(MutableMapping):
    def __init__(self, connection: sqlite3.Connection, serializer: Any, lifespan: timedelta) -> None:
        self._lifespan = lifespan.total_seconds();
        self._serializer = serializer
        self._connection = connection
        with _cursor(self._connection) as cursor:
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS expiringsqlitedict (
                key TEXT PRIMARY KEY NOT NULL,
                expire INTEGER NOT NULL,
                value BLOB NOT NULL)
            ''')
            cursor.execute('CREATE INDEX IF NOT EXISTS expiringsqlitedict_expire_index ON expiringsqlitedict (expire)')

            cursor.execute(
                '''
                CREATE TRIGGER IF NOT EXISTS expiringsqlitedict_insert_trigger AFTER INSERT ON expiringsqlitedict
                BEGIN
                    DELETE FROM expiringsqlitedict WHERE expire <= strftime('%s', 'now');
                END
                '''
            )

            cursor.execute(
                '''
                CREATE TRIGGER IF NOT EXISTS expiringsqlitedict_update_trigger AFTER UPDATE ON expiringsqlitedict
                BEGIN
                    DELETE FROM expiringsqlitedict WHERE expire <= strftime('%s', 'now');
                END
                '''
            )

    def __len__(self) -> int:
        with _cursor(self._connection) as cursor:
            for row in cursor.execute('SELECT COUNT(*) FROM expiringsqlitedict'):
                return row[0]
        return 0

    def __bool__(self) -> bool:
        return len(self) > 0

    def keys(self) -> Iterator[str]:
        with _cursor(self._connection) as cursor:
            for row in cursor.execute('SELECT key FROM expiringsqlitedict'):
                yield row[0]

    __iter__ = keys

    def values(self) -> Iterator[Any]:
        with _cursor(self._connection) as cursor:
            for row in cursor.execute('SELECT value FROM expiringsqlitedict'):
                yield self._serializer.loads(row[0])

    def items(self) -> Iterator[Tuple[str, Any]]:
        with _cursor(self._connection) as cursor:
            for row in cursor.execute('SELECT key, value FROM expiringsqlitedict'):
                yield row[0], self._serializer.loads(row[1])

    def __contains__(self, key: str) -> bool:
        with _cursor(self._connection) as cursor:
            for _ in cursor.execute('SELECT 1 FROM expiringsqlitedict WHERE key = ?', (key,)):
                return True
        return False

    def __getitem__(self, key: str) -> Any:
        with _cursor(self._connection) as cursor:
            for row in cursor.execute('SELECT value FROM expiringsqlitedict WHERE key = ?', (key,)):
                return self._serializer.loads(row[0])
        raise KeyError(key)

    def __setitem__(self, key: str, value: Any) -> None:
        with _cursor(self._connection) as cursor:
            cursor.execute(
                "REPLACE INTO expiringsqlitedict (key, expire, value) VALUES (?, strftime('%s', 'now') + ?, ?)",
                (key, self._lifespan, self._serializer.dumps(value)),
                )

    def __delitem__(self, key: str) -> None:
        if key not in self:
            raise KeyError(key)
        with _cursor(self._connection) as cursor:
            cursor.execute('DELETE FROM expiringsqlitedict WHERE key=?', (key,))

    def clear(self) -> None:
        with _cursor(self._connection) as cursor:
            cursor.execute('DELETE FROM expiringsqlitedict')

    def postpone(self, key: str) -> None:
        '''Push back the expiration date of the given entry, if it exists.
        '''
        with _cursor(self._connection) as cursor:
            cursor.execute(
                "UPDATE expiringsqlitedict SET expire=strftime('%s', 'now') + ? WHERE key=?",
                (self._lifespan, key),
                )
