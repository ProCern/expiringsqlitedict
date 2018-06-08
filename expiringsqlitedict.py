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

# some Python 3 vs 2 imports
try:
    from collections import UserDict as DictClass
except ImportError:
    from UserDict import DictMixin as DictClass

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

class SqliteDict(DictClass):
    VALID_FLAGS = ['c', 'r', 'w', 'n']

    def __init__(self, filename=None, flag='c', encode=encode, decode=decode, lifespan=None, vacuuminterval=None):
        """
        Initialize a thread-safe sqlite-backed dictionary. The dictionary will
        be table expiringsqlitedict in database file `filename`. A single file
        (=database) may contain multiple tables.

        If no `filename` is given, a random file in temp will be used (and deleted
        from temp once the dict is closed/deleted).

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

        self.lockfile = _open(self.lockfilename, self.lock_mode)
        lockf(self.lockfile, lock_type)

    def unlock(self):
        '''Release and unlock the lockfile.  Called automatically in a context manager.'''
        lockf(self.lockfile, LOCK_UN)
        self.lockfile.close()

    def clean(self):
        '''Clean up expired columns'''

        self.conn.execute(
            "DELETE FROM expiringsqlitedict WHERE expire<=STRFTIME('%s', 'now', 'utc')"
            )

    def check_vacuum(self):
        '''Tests to see if it's time to vacuum, and vacuums if so'''
        cur = self.conn.cursor()
        cur.execute(
            "SELECT value FROM expiringsqlitedictmeta WHERE key=?",
            ('nextvacuum',),
            )
        nextvacuum = self.decode(cur.fetchone()[0])
        if datetime.utcnow() >= nextvacuum:
            logger.info("vacuuming Sqlite file {}".format(self.filename))
            self.conn.execute('VACUUM')
            cur = self.conn.cursor()
            cur.execute(
                'REPLACE INTO expiringsqlitedictmeta (key, value) VALUES (?, ?)',
                ('nextvacuum', self.encode(datetime.utcnow() + self.vacuuminterval)),
            )
            self.conn.commit()

    def __enter__(self):
        self.lock()
        if self.flag == 'n':
            if os.path.exists(self.filename):
                os.remove(self.filename)
        logger.info("opening Sqlite file {}".format(self.filename))
        self.conn = sqlite3.connect(self.filename).__enter__()
        self.conn.execute('''
        CREATE TABLE IF NOT EXISTS expiringsqlitedict (
            key TEXT UNIQUE NOT NULL,
            expire INTEGER NOT NULL,
            value BLOB NOT NULL)
        ''')
        self.conn.execute('CREATE INDEX IF NOT EXISTS expiringsqlitedict_expire_index ON expiringsqlitedict (expire)')
        self.conn.execute('''
        CREATE TABLE IF NOT EXISTS expiringsqlitedictmeta (
            key TEXT UNIQUE NOT NULL,
            value BLOB NOT NULL)
        ''')
        self.conn.execute(
            'INSERT OR IGNORE INTO expiringsqlitedictmeta (key, value) VALUES (?, ?)',
            ('nextvacuum', self.encode(datetime.utcnow() + self.vacuuminterval))
        )
        self.conn.commit()
        if self.flag == 'w':
            self.clear()
        self.clean()
        self.conn.commit()
        self.check_vacuum()
        self.conn.commit()
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
        rows = self.select_one('SELECT COUNT(*) FROM expiringsqlitedict')[0]
        return rows if rows is not None else 0

    def __bool__(self):
        # No elements is False, otherwise True
        m = self.select_one('SELECT MAX(ROWID) FROM expiringsqlitedict')[0]
        # Explicit better than implicit and bla bla
        return True if m is not None else False

    def iterkeys(self):
        cur = self.conn.cursor()
        cur.execute('SELECT key FROM expiringsqlitedict ORDER BY rowid')
        for key in cur:
            yield key[0]

    def itervalues(self):
        cur = self.conn.cursor()
        cur.execute('SELECT value FROM expiringsqlitedict ORDER BY rowid')
        for value in cur:
            yield self.decode(value[0])

    def iteritems(self):
        cur = self.conn.cursor()
        cur.execute('SELECT key, value FROM expiringsqlitedict ORDER BY rowid')
        for key, value in cur:
            yield key, self.decode(value)

    def keys(self):
        return self.iterkeys() if major_version > 2 else list(self.iterkeys())

    def values(self):
        return self.itervalues() if major_version > 2 else list(self.itervalues())

    def items(self):
        return self.iteritems() if major_version > 2 else list(self.iteritems())

    def select_one(self, *args, **kwargs):
        '''Runs an execute and a select'''
        cur = self.conn.cursor()
        cur.execute(*args, **kwargs)
        return cur.fetchone()

    def __contains__(self, key):
        return self.select_one('SELECT 1 FROM expiringsqlitedict WHERE key = ?', (key,)) is not None

    def __getitem__(self, key):
        item = self.select_one('SELECT value FROM expiringsqlitedict WHERE key = ?', (key,))
        if item is None:
            raise KeyError(key)
        return self.decode(item[0])

    def __setitem__(self, key, value):
        if self.flag == 'r':
            raise RuntimeError('Refusing to write to read-only SqliteDict')

        expire = int((datetime.utcnow() + self.lifespan).timestamp())

        self.conn.execute(
            'REPLACE INTO expiringsqlitedict (key, expire, value) VALUES (?, ?, ?)',
            (key, expire, self.encode(value)),
            )

    def __delitem__(self, key):
        if self.flag == 'r':
            raise RuntimeError('Refusing to delete from read-only SqliteDict')

        if key not in self:
            raise KeyError(key)
        self.conn.execute('DELETE FROM expiringsqlitedict WHERE key = ?', (key,))

    def update(self, items=(), **kwds):
        if self.flag == 'r':
            raise RuntimeError('Refusing to update read-only SqliteDict')

        try:
            items = items.items()
        except AttributeError:
            pass
        items = [(k, self.encode(v)) for k, v in items]

        if items:
            self.conn.executemany(self.ADD_ITEM, items)
        if kwds:
            self.update(kwds)

    __iter__ = iterkeys

    def clear(self):
        if self.flag == 'r':
            raise RuntimeError('Refusing to clear read-only SqliteDict')

        self.conn.commit()
        self.conn.execute('DELETE FROM expiringsqlitedict')
        self.conn.commit()

    def commit(self, blocking=True):
        """
        Persist all data to disk.

        When `blocking` is False, the commit command is queued, but the data is
        not guaranteed persisted (default implication when autocommit=True).
        """
        if self.conn is not None:
            self.conn.commit(blocking)
    sync = commit

    def close(self, do_log=True, force=False):
        if do_log:
            logger.debug("closing %s" % self)
        if hasattr(self, 'conn') and self.conn is not None:
            if not force:
                # typically calls to commit are non-blocking when autocommit is
                # used.  However, we need to block on close() to ensure any
                # awaiting exceptions are handled and that all data is
                # persisted to disk before returning.
                self.conn.commit(blocking=True)
            self.conn.close(force=force)
            self.conn = None
        if self.in_temp:
            try:
                os.remove(self.filename)
            except:
                pass

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
