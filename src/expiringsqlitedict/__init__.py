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


import json
import sqlite3
from collections.abc import MutableMapping
from contextlib import ExitStack, closing, contextmanager
from datetime import timedelta
from types import TracebackType
from typing import Any, Generator, Iterator, Optional, Tuple, Type
from weakref import finalize

@contextmanager
def _transaction(
    connection: sqlite3.Connection,
    begin: str,
) -> Generator[None, None, None]:
    with closing(connection.cursor()) as cursor:
        cursor.execute(begin)
    try:
        yield
    except:
        with closing(connection.cursor()) as cursor:
            cursor.execute('ROLLBACK')
        raise
    else:
        with closing(connection.cursor()) as cursor:
            cursor.execute('COMMIT')

class SqliteDict:
    """
    Set up the sqlite dictionary manager.

    This needs to be used as a context manager.  It will not operate at all
    otherwise. args and kwargs are directly passed to sqlite3.connect.  Use
    these to customize your connection, such as making it read-only.

    This is lazy, and won't even open the database until it is entered.  It may
    be re-opened after it has closed.
    """

    __slots__ = (
        '_args',
        '_kwargs',
        '_connection',
        '_exit_stack',
        '_serializer',
        '_lifespan',
        '_begin',
        '_table',
        '__weakref__'
    )

    def __init__(self,
        *args,
        serializer: Any = json,
        lifespan: timedelta = timedelta(weeks=1),
        transaction: str = 'IMMEDIATE',
        table: str = 'expiringsqlitedict',
        **kwargs,
    ) -> None:
        self._args = args
        self._kwargs = kwargs
        self._serializer = serializer
        self._lifespan = lifespan
        self._begin = f'BEGIN {transaction} TRANSACTION'
        self._table = table

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
        with ExitStack() as exit_stack:
            self._connection = exit_stack.enter_context(closing(sqlite3.connect(
                *self._args,
                isolation_level=None,
                **self._kwargs,
            )))

            with closing(self._connection.cursor()) as cursor:
                cursor.execute('PRAGMA journal_mode=WAL')
                cursor.execute('PRAGMA synchronous=NORMAL')

            def optimize() -> None:
                with closing(self._connection.cursor()) as cursor:
                    cursor.execute('PRAGMA analysis_limit=8192')
                    cursor.execute('PRAGMA optimize')

            exit_stack.callback(optimize)

            exit_stack.enter_context(_transaction(self._connection, self._begin))

            connection = Connection(
                self._connection,
                serializer=self._serializer,
                lifespan=self._lifespan,
                table=self._table,
            )
            self._exit_stack = exit_stack.pop_all()
            return connection

        assert False, 'UNREACHABLE'

    def __exit__(
        self,
        type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool:
        try:
            return self._exit_stack.__exit__(type, value, traceback)
        finally:
            del self._connection, self._exit_stack

def SimpleSqliteDict(
    *args,
    serializer: Any = json,
    lifespan: timedelta = timedelta(weeks=1),
    isolation_level: Optional[str] = None,
    table: str = 'expiringsqlitedict',
    **kwargs,
) -> 'Connection':
    """
    Set up the sqlite dictionary manager as a non-contextmanager with a finalizer.

    If you set the isolation_level, you will be responsible for calling
    d.connection.commit() and d.connection.rollback() appropriately.
    """

    db = sqlite3.connect(*args, isolation_level=isolation_level, **kwargs)
    with closing(db.cursor()) as cursor:
        cursor.execute('PRAGMA journal_mode=WAL')
        cursor.execute('PRAGMA synchronous=NORMAL')

    connection = Connection(
        db,
        serializer=serializer,
        lifespan=lifespan,
        table=table,
    )

    def _close():
        '''Optimize and close the database.
        '''
        with closing(db) as d, closing(d.cursor()) as cursor:
            cursor.execute('PRAGMA analysis_limit=8192')
            cursor.execute('PRAGMA optimize')

    finalize(connection, _close)

    return connection

_trailers = []
if sqlite3.sqlite_version_info >= (3, 8, 2):
    _trailers.append('WITHOUT ROWID')

if sqlite3.sqlite_version_info >= (3, 37):
    _trailers.append('STRICT')
    _valuetype = 'ANY'
else:
    _valuetype = 'BLOB'

_trailer = ', '.join(_trailers)

if sqlite3.sqlite_version_info >= (3, 38):
    _unixepoch = 'UNIXEPOCH()'
else:
    _unixepoch = "CAST(strftime('%s', 'now') AS INTEGER)"

class Connection(MutableMapping):
    '''The actual connection object, as a MutableMapping[str, Any].

    Items are expired when a value is inserted or updated.  Deletion or
    postponement does not expire items.
    '''

    __slots__ = (
        '_lifespan',
        '_serializer',
        '_connection',
        '_table',
        '_safe_table',
        '__weakref__',
    )

    def __init__(self,
        connection: sqlite3.Connection,
        serializer: Any = json,
        lifespan: timedelta = timedelta(weeks=1),
        table: str = 'expiringsqlitedict',
    ) -> None:
        self._lifespan = lifespan.total_seconds()
        self._serializer = serializer
        self._connection = connection
        self._table = table
        self._safe_table = table.replace('"', '""')

        with closing(self._connection.cursor()) as cursor:
            create_statement = f'''
            CREATE TABLE IF NOT EXISTS "{self._safe_table}" (
                key TEXT PRIMARY KEY NOT NULL,
                expire INTEGER NOT NULL,
                value {_valuetype} NOT NULL){_trailer}'''


            cursor.execute(create_statement)
            cursor.execute(
                f'CREATE INDEX IF NOT EXISTS "{self._safe_table}_expire_index"'
                f' ON "{self._safe_table}" (expire)'
            )

            cursor.execute(
                f'''
                CREATE TRIGGER IF NOT EXISTS "{self._safe_table}_insert_trigger"'''
                f''' AFTER INSERT ON "{self._safe_table}"
                BEGIN
                    DELETE FROM "{self._safe_table}" WHERE expire <= {_unixepoch};
                END
                '''
            )

            cursor.execute(
                f'''
                CREATE TRIGGER IF NOT EXISTS "{self._safe_table}_update_trigger"'''
                f''' AFTER UPDATE OF value ON "{self._safe_table}"
                BEGIN
                    DELETE FROM "{self._safe_table}" WHERE expire <= {_unixepoch};
                END
                '''
            )
    @property
    def connection(self) -> sqlite3.Connection:
        return self._connection

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
            for row in cursor.execute(f'SELECT COUNT(*) FROM "{self._safe_table}"'):
                return row[0]
        return 0

    def __bool__(self) -> bool:
        '''Check if the table is not empty.'''

        return len(self) > 0

    def keys(self) -> Iterator[str]:
        '''Iterate over keys in the table.
        '''

        with closing(self._connection.cursor()) as cursor:
            for row in cursor.execute(f'SELECT key FROM "{self._safe_table}"'):
                yield row[0]

    __iter__ = keys

    def values(self) -> Iterator[Any]:
        '''Iterate over values in the table.
        '''

        with closing(self._connection.cursor()) as cursor:
            for row in cursor.execute(f'SELECT value FROM "{self._safe_table}"'):
                yield self._serializer.loads(row[0])

    def items(self) -> Iterator[Tuple[str, Any]]:
        '''Iterate over keys and values in the table.
        '''

        with closing(self._connection.cursor()) as cursor:
            for row in cursor.execute(f'SELECT key, value FROM "{self._safe_table}"'):
                yield row[0], self._serializer.loads(row[1])

    def __contains__(self, key: str) -> bool:
        '''Check if the table contains the given key.
        '''

        with closing(self._connection.cursor()) as cursor:
            for _ in cursor.execute(
                f'SELECT 1 FROM "{self._safe_table}" WHERE key = ?',
                (key,),
            ):
                return True
        return False

    def __getitem__(self, key: str) -> Any:
        '''Fetch the key.
        '''

        with closing(self._connection.cursor()) as cursor:
            for row in cursor.execute(
                f'SELECT value FROM "{self._safe_table}" WHERE key = ?', (key,)
            ):
                return self._serializer.loads(row[0])
        raise KeyError(key)

    def __setitem__(self, key: str, value: Any) -> None:
        '''Set or replace the item.

        This also triggers cleaning up expired values.
        '''

        with closing(self._connection.cursor()) as cursor:
            cursor.execute(
                f'REPLACE INTO "{self._safe_table}" (key, expire, value)'
                f'VALUES (?, {_unixepoch} + ?, ?)',
                (key, self._lifespan, self._serializer.dumps(value)),
                )

    def __delitem__(self, key: str) -> None:
        '''Delete an item from the table.
        '''

        with closing(self._connection.cursor()) as cursor:
            cursor.execute(f'DELETE FROM "{self._safe_table}" WHERE key=?', (key,))
            if cursor.rowcount != 1:
                raise KeyError(key)

    def clear(self) -> None:
        '''Delete all items from the table.
        '''

        with closing(self._connection.cursor()) as cursor:
            cursor.execute(f'DELETE FROM "{self._safe_table}"')

    def postpone(self, key: str) -> None:
        '''Push back the expiration date of the given entry, if it exists.
        '''
        with closing(self._connection.cursor()) as cursor:
            cursor.execute(
                f'UPDATE "{self._safe_table}" SET expire={_unixepoch} + ? WHERE key=?',
                (self._lifespan, key),
            )

    def postpone_all(self) -> None:
        '''Push back the expiration date of all entries at once.
        '''
        with closing(self._connection.cursor()) as cursor:
            cursor.execute(
                f'UPDATE "{self._safe_table}" SET expire={_unixepoch} + ?',
                (self._lifespan,),
            )
