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
A lightweight wrapper around Python's sqlite3 database, with a dict-like interface:

>>> with SqliteDict('some.db') as mydict: # the mapping will be persisted to file `some.db`
>>>     mydict['some_key'] = any_picklable_object
>>>     print mydict['some_key']
>>>     print len(mydict) # etc... all dict functions work

Pickle is used internally to serialize the values. Keys are strings.  Values
are automatically compressed if possible, keys expire after a certain time, and
the database is automatically vacuumed.  This hits many of the same uses as
sqlitedict (you should probably prefer the original if it fits all your
use-cases), but this is more aimed toward a persistent database that is
possibly accessed from multiple different processes (hence file locking), and
which needs keys that will expire automatically (hence the expiry and
vacuuming), and expects to possibly handle keys that take up a lot of space
(hence the compression).
"""

from datetime import datetime, timedelta
from fcntl import lockf, LOCK_UN, LOCK_SH, LOCK_EX
import logging
import os
import random
import sqlite3
import sys
import tempfile
import traceback
import zlib

try:
    __version__ = __import__('pkg_resources').get_distribution('expiringsqlitedict').version
except:
    __version__ = '?'

major_version = sys.version_info[0]
if major_version < 3:  # py <= 2.x
    if sys.version_info[1] < 5:  # py <= 2.4
        raise ImportError("sqlitedict requires python 2.5 or higher (python 3.3 or higher supported)")

try:
    from cPickle import HIGHEST_PROTOCOL as PICKLE_PROTOCOL
    import cPickle as pickle
except ImportError:
    from pickle import HIGHEST_PROTOCOL as PICKLE_PROTOCOL
    import pickle

from collections import MutableMapping

logger = logging.getLogger(__name__)

_open = open

def open(*args, **kwargs):
    """See documentation of the SqliteDict class."""
    return SqliteDict(*args, **kwargs)


def encode(obj):
    """Serialize an object using pickle to a binary format accepted by SQLite."""

    pickled = pickle.dumps(obj, protocol=PICKLE_PROTOCOL)
    compressed = zlib.compress(pickled)

    # If compression didn't fail to save space:
    if len(compressed) < len(pickled):
        data = b'Z' + compressed
    else:
        data = b'R' + pickled

    return sqlite3.Binary(data)

def decode(obj):
    """Deserialize objects retrieved from SQLite."""
    data = bytes(obj)
    flag = data[0:1]
    data = data[1:]

    if flag == b'Z':
        pickled = zlib.decompress(data)
    else:
        pickled = data

    return pickle.loads(pickled)

def totimestamp(dt):
    if major_version > 2:
        return dt.timestamp()
    else:
        return (dt - datetime(1970, 1, 1)).total_seconds()
    

class SqliteDict(MutableMapping):
    VALID_FLAGS = ['c', 'r', 'w', 'n']

    def __init__(self, filename, flag='c', encode=encode, decode=decode, lifespan=None, vacuuminterval=None):
        """
        Initialize a sqlite-backed dictionary. The dictionary will be table
        expiringsqlitedict in database file `filename`. A single file
        (=database) may contain multiple tables.

        It is very strongly suggested that you use this class as a context
        manager.  There are wrappers to automatically do this for you, but you
        can quite easily accidentally leave a lockfile open forever if you
        partially iterate a generator.  If you really want to work without a
        context manager, make sure that you fully iterate all iterable methods
        to ensure the lock is released when it needs to be.

        The `flag` parameter. Exactly one of:
          'c': default mode, open for read/write, creating the db/table if necessary.
          'w': open for r/w, but drop expiringsqlitedict contents first (start with empty table)
          'r': open as read-only
          'n': create a new database (erasing any existing tables, not just expiringsqlitedict!).

        The `encode` and `decode` parameters are used to customize how the values
        are serialized and deserialized.
        The `encode` parameter must be a function that takes a single Python
        object and returns a serialized representation.
        The `decode` function must be a function that takes the serialized
        representation produced by `encode` and returns a deserialized Python
        object.
        The default is to use pickle and attempt a zlib compression, leading
        the field with a single-byte header showing whether compression was
        used or not.
        `lifespan` is a timedelta object.   If it is None, it defaults to 7
        days.  It specifies the lifespan of new and changed keys.
        `vacuuminterval` is a timedelta object.   If it is None, it defaults to
        4 weeks.  It specifies how often the database will be vacuumed.

        Vacuuming and expired key cleaning are done on opening the database.
        Cleaning of expired keys happens on every open, and a vacuum happens on
        the given interval.

        """
        self.in_temp = filename is None
        if self.in_temp:
            randpart = hex(random.randint(0, 0xffffff))[2:]
            filename = os.path.join(tempfile.gettempdir(), 'exsqldict' + randpart)

        self.lockfilename = filename + '.lock'

        if lifespan is None:
            self.lifespan = timedelta(days=7)
        else:
            self.lifespan = lifespan

        if vacuuminterval is None:
            self.vacuuminterval = timedelta(weeks=4)
        else:
            self.vacuuminterval = vacuuminterval

        if flag not in SqliteDict.VALID_FLAGS:
            raise RuntimeError("Unrecognized flag: %s" % flag)
        self.flag = flag

        if flag == 'r':
            self.lock_mode = 'r'
        else:
            self.lock_mode = 'w'

        dirname = os.path.dirname(filename)
        if dirname:
            if not os.path.exists(dirname):
                raise RuntimeError('Error! The directory does not exist, %s' % dirname)

        self.filename = filename
        self.encode = encode
        self.decode = decode

    def lock(self):
        '''Grab and lock the lockfile.  Called automatically in a context manager.'''
        if self.lock_mode == 'r':
            lock_type = LOCK_SH
        else:
            lock_type = LOCK_EX

        # First touch to make sure the lockfile exists.  This is necessary if
        # the file is opened for reading before it's opened for writing.
        with _open(self.lockfilename, 'a'):
            pass

        self.lockfile = _open(self.lockfilename, self.lock_mode)
        lockf(self.lockfile, lock_type)

    def unlock(self):
        '''Release and unlock the lockfile.  Called automatically in a context manager.'''
        lockf(self.lockfile, LOCK_UN)
        self.lockfile.close()

    def clean(self):
        '''Clean up expired columns'''

        if self.flag == 'r':
            raise RuntimeError('Refusing to clean SqliteDict')

        self._execute(
            "DELETE FROM expiringsqlitedict WHERE expire<=STRFTIME('%s', 'now', 'utc')"
            )

    def check_vacuum(self):
        '''Tests to see if it's time to vacuum, and vacuums if so'''
        if self.flag == 'r':
            raise RuntimeError('Refusing to vacuum SqliteDict')

        self.conn.cursor().execute(
            "SELECT value FROM expiringsqlitedictmeta WHERE key=?",
            ('nextvacuum',),
        )
        nextvacuum = datetime.fromtimestamp(float(cur.fetchone()[0]))
        if datetime.utcnow() >= nextvacuum:
            logger.info("vacuuming Sqlite file {}".format(self.filename))
            self.conn.cursor().execute('VACUUM')
            self.conn.cursor().execute(
                'REPLACE INTO expiringsqlitedictmeta (key, value) VALUES (?, ?)',
                ('nextvacuum', totimestamp(datetime.utcnow() + self.vacuuminterval)),
            )
            # Force a commit so that we're sure vacuum time is updated
            self.conn.commit()

    def __enter__(self):
        self.lock()
        if self.flag == 'n':
            if os.path.exists(self.filename):
                os.remove(self.filename)
        logger.info("opening Sqlite file {}".format(self.filename))
        self.conn = sqlite3.connect(self.filename).__enter__()
        self._execute('''
        CREATE TABLE IF NOT EXISTS expiringsqlitedict (
            key TEXT UNIQUE NOT NULL,
            expire INTEGER NOT NULL,
            value BLOB NOT NULL)
        ''')
        self._execute('CREATE INDEX IF NOT EXISTS expiringsqlitedict_expire_index ON expiringsqlitedict (expire)')
        self._execute('''
        CREATE TABLE IF NOT EXISTS expiringsqlitedictmeta (
            key TEXT UNIQUE NOT NULL,
            value TEXT NOT NULL)
        ''')
        self._execute(
            'INSERT OR IGNORE INTO expiringsqlitedictmeta (key, value) VALUES (?, ?)',
            ('nextvacuum', totimestamp(datetime.utcnow() + self.vacuuminterval)),
        )
        if self.flag == 'w':
            self.clear()

        if self.flag != 'r':
            self.clean()
            self.check_vacuum()
        return self

    def __exit__(self, *exc_info):
        logger.debug("closing %s" % self)
        # Handle rollback/commit automatically
        self.conn.__exit__(*exc_info)
        del self.conn
        self.close()
        self.unlock()

    def __str__(self):
        return "SqliteDict(%s)" % (self.filename)

    def __repr__(self):
        return str(self)  # no need of something complex

    def __len__(self):
        # `select count (*)` is super slow in sqlite (does a linear scan!!)
        # As a result, len() is very slow too once the table size grows beyond trivial.
        # We could keep the total count of rows ourselves, by means of triggers,
        # but that seems too complicated and would slow down normal operation
        # (insert/delete etc).
        rows = self._select_one('SELECT COUNT(*) FROM expiringsqlitedict')[0]
        return rows if rows is not None else 0

    def _select_one(self, *args, **kwargs):
        '''Runs an execute and a fetchone.
        
        Used internally to simplify context manager logic.
        '''
        if self.connection_opened():
            cur = self.conn.cursor()
            cur.execute(*args, **kwargs)
            return cur.fetchone()
        else:
            with self:
                return self._select_one(*args, **kwargs)

    def _select(self, *args, **kwargs):
        '''Runs an execute and a select, iterating a cursor.

        This is a generator instead of returning a cursor, because returning a
        cursor would close the lockfile.  We need the context manager open
        until the cursor is finished.
        '''

        if self.connection_opened():
            cur = self.conn.cursor()
            cur.execute(*args, **kwargs)
            for row in cur:
                yield row
        else:
            with self:
                for row in self._select(*args, **kwargs):
                    yield row

    def _execute(self, *args, **kwargs):
        '''Runs an execute that ignores results.

        Used internally to simplify context manager logic.
        '''

        if self.connection_opened():
            self.conn.execute(*args, **kwargs)
        else:
            with self:
                self._execute(*args, **kwargs)

    def _executemany(self, *args, **kwargs):
        '''Runs an executemany that ignores results.

        Used internally to simplify context manager logic.
        '''

        if self.connection_opened():
            self.conn.executemany(*args, **kwargs)
        else:
            with self:
                self._executemany(*args, **kwargs)

    def __bool__(self):
        # No elements is False, otherwise True
        m = self._select_one('SELECT MAX(ROWID) FROM expiringsqlitedict')[0]
        # Explicit better than implicit and bla bla
        return True if m is not None else False

    def iterkeys(self):
        sql = 'SELECT key FROM expiringsqlitedict ORDER BY rowid'
        for (key,) in self._select(sql):
            yield key

    def itervalues(self):
        sql = 'SELECT value FROM expiringsqlitedict ORDER BY rowid'
        for (value,) in self._select(sql):
            yield self.decode(value)

    def iteritems(self):
        sql = 'SELECT key, value FROM expiringsqlitedict ORDER BY rowid'
        for key, value in self._select(sql):
            yield key, self.decode(value)

    def keys(self):
        return self.iterkeys() if major_version > 2 else list(self.iterkeys())

    def values(self):
        return self.itervalues() if major_version > 2 else list(self.itervalues())

    def items(self):
        return self.iteritems() if major_version > 2 else list(self.iteritems())

    def __contains__(self, key):
        return self._select_one('SELECT 1 FROM expiringsqlitedict WHERE key = ?', (key,)) is not None

    def __getitem__(self, key):
        item = self._select_one('SELECT value FROM expiringsqlitedict WHERE key = ?', (key,))
        if item is None:
            raise KeyError(key)
        return self.decode(item[0])

    def __setitem__(self, key, value):
        if self.flag == 'r':
            raise RuntimeError('Refusing to write to read-only SqliteDict')

        expire = int(totimestamp(datetime.utcnow() + self.lifespan))

        self._execute(
            'REPLACE INTO expiringsqlitedict (key, expire, value) VALUES (?, ?, ?)',
            (key, expire, self.encode(value)),
            )

    def __delitem__(self, key):
        if self.flag == 'r':
            raise RuntimeError('Refusing to delete from read-only SqliteDict')

        if key not in self:
            raise KeyError(key)
        self._execute('DELETE FROM expiringsqlitedict WHERE key=?', (key,))

    def update(self, items=(), **kwds):
        if self.flag == 'r':
            raise RuntimeError('Refusing to update read-only SqliteDict')

        try:
            items = items.items()
        except AttributeError:
            pass
        try:
            kwds = kwds.items()
        except AttributeError:
            pass

        items = [(k, self.encode(v)) for k, v in items]
        items += [(k, self.encode(v)) for k, v in kwds]

        if items:
            self._executemany(self.ADD_ITEM, items)

    __iter__ = iterkeys

    def clear(self):
        if self.flag == 'r':
            raise RuntimeError('Refusing to clear read-only SqliteDict')

        self.commit()
        self._execute('DELETE FROM expiringsqlitedict')
        self.commit()

    def commit(self, blocking=True):
        """
        Persist all data to disk.

        When `blocking` is False, the commit command is queued, but the data is
        not guaranteed persisted (default implication when autocommit=True).
        """
        if self.connection_opened():
            self.conn.commit()
    sync = commit

    def connection_opened(self):
        return getattr(self, 'conn', None) is not None

    def close(self, do_log=True, force=False):
        if do_log:
            logger.debug("closing %s" % self)
        if self.connection_opened():
            if not force:
                # typically calls to commit are non-blocking when autocommit is
                # used.  However, we need to block on close() to ensure any
                # awaiting exceptions are handled and that all data is
                # persisted to disk before returning.
                self.conn.commit()
            self.conn.close(force=force)
            self.conn = None
        if self.in_temp:
            try:
                os.remove(self.filename)
            except:
                pass

    def terminate(self):
        """Delete the underlying database file. Use with care."""
        if self.flag == 'r':
            raise RuntimeError('Refusing to terminate read-only SqliteDict')

        self.close()

        if self.filename == ':memory:':
            return

        logger.info("deleting %s" % self.filename)
        try:
            if os.path.isfile(self.filename):
                os.remove(self.filename)
        except (OSError, IOError):
            logger.exception("failed to delete %s" % (self.filename))

    def __del__(self):
        # like close(), but assume globals are gone by now (do not log!)
        try:
            self.close(do_log=False, force=True)
        except Exception:
            # prevent error log flood in case of multiple SqliteDicts
            # closed after connection lost (exceptions are always ignored
            # in __del__ method.
            pass

# Adding extra methods for python 2 compatibility (at import time)
if major_version == 2:
    SqliteDict.__nonzero__ = SqliteDict.__bool__
    del SqliteDict.__bool__  # not needed and confusing
#endclass SqliteDict
