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


from collections.abc import Mapping, MutableMapping
from contextlib import contextmanager, closing
from datetime import timedelta
from typing import Any, Iterator, Optional, Tuple, Union
import json
import pickle
import sqlite3
import zlib
from weakref import finalize

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

def _close(db):
    '''Optimize and close the database.
    '''
    with closing(db) as d, closing(d.cursor()) as cursor:
        cursor.execute('PRAGMA analysis_limit=8192')
        cursor.execute('PRAGMA optimize')

class SqliteDict:
    """
    Set up the sqlite dictionary manager.

    This needs to be used as a context manager.  It will not operate at all
    otherwise. args and kwargs are directly passed to sqlite3.connect.  Use
    these to customize your connection, such as making it read-only.
    """

    def __init__(self, *args, serializer: Any = json, lifespan: timedelta = timedelta(weeks=1), transaction: str = 'IMMEDIATE', table: str = 'expiringsqlitedict', **kwargs) -> None:
        self._db = sqlite3.connect(*args, isolation_level=None, **kwargs)
        with closing(self._db.cursor()) as cursor:
            cursor.execute('PRAGMA journal_mode=WAL')
            cursor.execute('PRAGMA synchronous=NORMAL')
        self._serializer = serializer
        self._lifespan = lifespan
        self._begin = f'BEGIN {transaction} TRANSACTION'
        self._table = table
        self._finalizer = finalize(self, _close, self._db)

    @property
    def lifespan(self) -> timedelta:
        '''The current lifespan.

        Changing this will change the calculated expiration time of future set
        items.  It will not retroactively apply to existing items unless you explicitly
        postpone them.
        '''
        return self._lifespan

    @lifespan.setter
    def lifespan(self, value: timedelta) -> None:
        self._lifespan = value

    def __enter__(self) -> 'Connection':
        with closing(self._db.cursor()) as cursor:
            cursor.execute(self._begin)

        return Connection(
            self._db,
            serializer=self._serializer,
            lifespan=self._lifespan,
            table=self._table,
        )

    def __exit__(self, type, value, traceback) -> None:
        if (type and value and traceback) is None:
            with closing(self._db.cursor()) as cursor:
                cursor.execute('COMMIT')
        else:
            with closing(self._db.cursor()) as cursor:
                cursor.execute('ROLLBACK')

    def close(self):
        '''Close and optimize the database.'''

        self._finalizer()

@contextmanager
def OnDemand(*args, **kwargs):
    '''A wrapper around a database that is a context-manager that opens the
    database on-demand and closes it immediately when finished with it.
    '''
    manager = SqliteDict(*args, **kwargs)
    with closing(manager), manager as connection:
        yield connection

def AutocommitSqliteDict(*args, serializer: Any = json, lifespan: timedelta = timedelta(weeks=1), table: str = 'expiringsqlitedict', **kwargs) -> 'Connection':
    """
    Set up the sqlite dictionary manager as a non-contextmanager in autocommit mode.
    """

    db = sqlite3.connect(*args, isolation_level=None, **kwargs)
    with closing(db.cursor()) as cursor:
        cursor.execute('PRAGMA journal_mode=WAL')
        cursor.execute('PRAGMA synchronous=NORMAL')

    connection = Connection(
        db,
        serializer=serializer,
        lifespan=lifespan,
        table=table,
    )

    finalize(connection, _close, db)

    return connection

class Connection(MutableMapping):
    '''The actual connection object, as a MutableMapping[str, Any].

    Items are expired when a value is inserted or updated.  Deletion or postponement does not expire items.
    '''

    def __init__(self, connection: sqlite3.Connection, serializer: Any, lifespan: timedelta, table: str = 'expiringsqlitedict') -> None:
        self._lifespan = lifespan.total_seconds()
        self._serializer = serializer
        self._connection = connection
        self._table = table
        with closing(self._connection.cursor()) as cursor:
            cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table} (
                key TEXT PRIMARY KEY NOT NULL,
                expire INTEGER NOT NULL,
                value BLOB NOT NULL) WITHOUT ROWID
            ''')
            cursor.execute(f'CREATE INDEX IF NOT EXISTS expiringsqlitedict_expire_index ON {table} (expire)')

            cursor.execute(
                f'''
                CREATE TRIGGER IF NOT EXISTS expiringsqlitedict_insert_trigger AFTER INSERT ON {table}
                BEGIN
                    DELETE FROM {table} WHERE expire <= strftime('%s', 'now');
                END
                '''
            )

            cursor.execute(
                f'''
                CREATE TRIGGER IF NOT EXISTS expiringsqlitedict_update_trigger AFTER UPDATE OF value ON {table}
                BEGIN
                    DELETE FROM {table} WHERE expire <= strftime('%s', 'now');
                END
                '''
            )

    @property
    def lifespan(self) -> timedelta:
        '''The current lifespan.

        Changing this will change the calculated expiration time of future set
        items.  It will not retroactively apply to existing items unless you explicitly
        postpone them.
        '''
        return timedelta(seconds=self._lifespan)

    @lifespan.setter
    def lifespan(self, value: timedelta) -> None:
        self._lifespan = value.total_seconds()

    def __len__(self) -> int:
        '''Get the count of keys in the table.
        '''

        with closing(self._connection.cursor()) as cursor:
            for row in cursor.execute(f'SELECT COUNT(*) FROM {self._table}'):
                return row[0]
        return 0

    def __bool__(self) -> bool:
        '''Check if the table is not empty.'''

        return len(self) > 0

    def keys(self) -> Iterator[str]:
        '''Iterate over keys in the table.
        '''

        with closing(self._connection.cursor()) as cursor:
            for row in cursor.execute(f'SELECT key FROM {self._table}'):
                yield row[0]

    __iter__ = keys

    def values(self) -> Iterator[Any]:
        '''Iterate over values in the table.
        '''

        with closing(self._connection.cursor()) as cursor:
            for row in cursor.execute(f'SELECT value FROM {self._table}'):
                yield self._serializer.loads(row[0])

    def items(self) -> Iterator[Tuple[str, Any]]:
        '''Iterate over keys and values in the table.
        '''

        with closing(self._connection.cursor()) as cursor:
            for row in cursor.execute(f'SELECT key, value FROM {self._table}'):
                yield row[0], self._serializer.loads(row[1])

    def __contains__(self, key: str) -> bool:
        '''Check if the table contains the given key.
        '''

        with closing(self._connection.cursor()) as cursor:
            for _ in cursor.execute(f'SELECT 1 FROM {self._table} WHERE key = ?', (key,)):
                return True
        return False

    def __getitem__(self, key: str) -> Any:
        '''Fetch the key.
        '''

        with closing(self._connection.cursor()) as cursor:
            for row in cursor.execute(f'SELECT value FROM {self._table} WHERE key = ?', (key,)):
                return self._serializer.loads(row[0])
        raise KeyError(key)

    def __setitem__(self, key: str, value: Any) -> None:
        '''Set or replace the item.

        This also triggers cleaning up expired values.
        '''

        with closing(self._connection.cursor()) as cursor:
            cursor.execute(
                f"REPLACE INTO {self._table} (key, expire, value) VALUES (?, strftime('%s', 'now') + ?, ?)",
                (key, self._lifespan, self._serializer.dumps(value)),
                )

    def __delitem__(self, key: str) -> None:
        '''Delete an item from the table.
        '''

        if key not in self:
            raise KeyError(key)
        with closing(self._connection.cursor()) as cursor:
            cursor.execute(f'DELETE FROM {self._table} WHERE key=?', (key,))

    def clear(self) -> None:
        '''Delete all items from the table.
        '''

        with closing(self._connection.cursor()) as cursor:
            cursor.execute(f'DELETE FROM {self._table}')

    def postpone(self, key: str) -> None:
        '''Push back the expiration date of the given entry, if it exists.
        '''
        with closing(self._connection.cursor()) as cursor:
            cursor.execute(
                f"UPDATE {self._table} SET expire=strftime('%s', 'now') + ? WHERE key=?",
                (self._lifespan, key),
                )

    def postpone_all(self) -> None:
        '''Push back the expiration date of all entries at once.
        '''
        with closing(self._connection.cursor()) as cursor:
            cursor.execute(
                f"UPDATE {self._table} SET expire=strftime('%s', 'now') + ?",
                (self._lifespan,),
                )
