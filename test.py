#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright © 2021 Taylor C. Richberger
# This code is released under the license described in the LICENSE file

from contextlib import closing
import sqlite3
import unittest
from datetime import timedelta
from tempfile import TemporaryDirectory
from pathlib import Path
from expiringsqlitedict import SqliteDict, SimpleSqliteDict, Order
import json
import marshal
import pickle
import orjson

class TestExpiringDict(unittest.TestCase):
    def test_simple(self):
        with TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / 'test.db'

            with SqliteDict(str(db_path)) as d:
                self.assertFalse(bool(d))
                self.assertEqual(tuple(d), ())
                self.assertEqual(tuple(d.keys()), ())
                self.assertEqual(tuple(d.items()), ())
                self.assertEqual(tuple(d.values()), ())
                self.assertEqual(len(d), 0)
                d['foo'] = 'bar'
                d['baz'] = 1337

            with SqliteDict(str(db_path)) as d:
                self.assertTrue(bool(d))
                self.assertEqual(tuple(d), ('foo', 'baz'))
                self.assertEqual(tuple(d.keys()), ('foo', 'baz'))
                self.assertEqual(tuple(d.items()), (('foo', 'bar'), ('baz', 1337)))
                self.assertEqual(tuple(d.values()), ('bar', 1337))
                self.assertEqual(len(d), 2)

                self.assertEqual(tuple(reversed(d)), ('baz', 'foo'))
                self.assertEqual(tuple(reversed(d.keys())), ('baz', 'foo'))
                self.assertEqual(
                    tuple(reversed(d.items())),
                    (('baz', 1337), ('foo', 'bar')),
                )
                self.assertEqual(tuple(reversed(d.values())), (1337, 'bar'))

                self.assertEqual(tuple(d.keys(Order.KEY)), ('baz', 'foo'))
                self.assertEqual(
                    tuple(d.items(Order.KEY)),
                    (('baz', 1337), ('foo', 'bar')),
                )
                self.assertEqual(tuple(d.values(Order.KEY)), (1337, 'bar'))

                self.assertEqual(tuple(reversed(d.keys(Order.KEY))), ('foo', 'baz'))
                self.assertEqual(
                    tuple(reversed(d.items(Order.KEY))),
                    (('foo', 'bar'), ('baz', 1337)),
                )
                self.assertEqual(tuple(reversed(d.values(Order.KEY))), ('bar', 1337))

            with SqliteDict(str(db_path)) as d:
                d['foo'] = 'barbar'

            with SqliteDict(str(db_path)) as d:
                self.assertTrue(bool(d))
                self.assertEqual(tuple(d), ('foo', 'baz'))
                self.assertEqual(tuple(d.keys()), ('foo', 'baz'))
                self.assertEqual(tuple(d.items()), (('foo', 'barbar'), ('baz', 1337)))
                self.assertEqual(tuple(d.values()), ('barbar', 1337))
                self.assertEqual(len(d), 2)

            with SqliteDict(str(db_path)) as d:
                del d['foo']

            with SqliteDict(str(db_path)) as d:
                self.assertTrue(bool(d))
                self.assertEqual(tuple(d), ('baz',))
                self.assertEqual(tuple(d.keys()), ('baz',))
                self.assertEqual(tuple(d.items()), (('baz', 1337),))
                self.assertEqual(tuple(d.values()), (1337,))
                self.assertEqual(len(d), 1)

            with self.assertRaises(KeyError):
                with SqliteDict(str(db_path)) as d:
                    del d['foo']

            
            with SqliteDict(str(db_path)) as d:
                d['foo'] = 'spam'

            with SqliteDict(str(db_path)) as d:
                self.assertTrue(bool(d))
                self.assertEqual(tuple(d), ('baz', 'foo'))
                self.assertEqual(tuple(d.keys()), ('baz', 'foo'))
                self.assertEqual(tuple(d.items()), (('baz', 1337), ('foo', 'spam')))
                self.assertEqual(tuple(d.values()), (1337, 'spam'))
                self.assertEqual(len(d), 2)

    def test_migration(self):
        with TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / 'test.db'

            with closing(sqlite3.connect(str(db_path))) as connection:
                with closing(connection.cursor()) as cursor:
                    cursor.execute('''
                        CREATE TABLE expiringsqlitedict (
                            key TEXT UNIQUE PRIMARY KEY NOT NULL,
                            expire INTEGER NOT NULL,
                            value BLOB NOT NULL
                        )
                    ''')
                    cursor.execute('''
                        CREATE INDEX expiringsqlitedict_expire_index
                            ON expiringsqlitedict (expire)
                    ''')
                    cursor.execute('''
                        CREATE TRIGGER expiringsqlitedict_insert_trigger
                            AFTER INSERT ON expiringsqlitedict
                        BEGIN
                            DELETE FROM expiringsqlitedict
                                WHERE expire <= strftime('%s', 'now');
                        END
                    ''')
                    cursor.execute('''
                        CREATE TRIGGER expiringsqlitedict_update_trigger
                            AFTER UPDATE ON expiringsqlitedict
                        BEGIN
                            DELETE FROM expiringsqlitedict
                                WHERE expire <= strftime('%s', 'now');
                        END
                    ''')
                    cursor.executemany(
                        '''
                            INSERT INTO expiringsqlitedict (key, expire, value)
                            VALUES (?, strftime('%s', 'now', '+7 days'), ?)
                        ''',
                        (
                            ('foo', '"bar"'),
                            ('baz', '1337'),
                        ),
                    )
                connection.commit()

            with SqliteDict(str(db_path)) as d:
                self.assertTrue(bool(d))
                self.assertEqual(set(d), {'foo', 'baz'})
                self.assertEqual(set(d.keys()), {'foo', 'baz'})
                self.assertEqual(set(d.items()), {('foo', 'bar'), ('baz', 1337)})
                self.assertEqual(set(d.values()), {'bar', 1337})
                self.assertEqual(len(d), 2)

            with SqliteDict(str(db_path)) as d:
                d['foo'] = 'barbar'

            with SqliteDict(str(db_path)) as d:
                self.assertTrue(bool(d))
                self.assertEqual(tuple(d), ('foo', 'baz'))
                self.assertEqual(tuple(d.keys()), ('foo', 'baz'))
                self.assertEqual(tuple(d.items()), (('foo', 'barbar'), ('baz', 1337)))
                self.assertEqual(tuple(d.values()), ('barbar', 1337))
                self.assertEqual(len(d), 2)

            with SqliteDict(str(db_path)) as d:
                del d['foo']

            with SqliteDict(str(db_path)) as d:
                self.assertTrue(bool(d))
                self.assertEqual(tuple(d), ('baz',))
                self.assertEqual(tuple(d.keys()), ('baz',))
                self.assertEqual(tuple(d.items()), (('baz', 1337),))
                self.assertEqual(tuple(d.values()), (1337,))
                self.assertEqual(len(d), 1)

            with self.assertRaises(KeyError):
                with SqliteDict(str(db_path)) as d:
                    del d['foo']

            
            with SqliteDict(str(db_path)) as d:
                d['foo'] = 'spam'

            with SqliteDict(str(db_path)) as d:
                self.assertTrue(bool(d))
                self.assertEqual(tuple(d), ('baz', 'foo'))
                self.assertEqual(tuple(d.keys()), ('baz', 'foo'))
                self.assertEqual(tuple(d.items()), (('baz', 1337), ('foo', 'spam')))
                self.assertEqual(tuple(d.values()), (1337, 'spam'))
                self.assertEqual(len(d), 2)

    def test_quotes(self):
        with TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / 'test.db'

            with SqliteDict(
                str(db_path),
                table = 'Name\\with"special\t-_ char""ac"""ters',
            ) as d:
                self.assertFalse(bool(d))
                self.assertEqual(tuple(d), ())
                self.assertEqual(tuple(d.keys()), ())
                self.assertEqual(tuple(d.items()), ())
                self.assertEqual(tuple(d.values()), ())
                self.assertEqual(len(d), 0)
                d['foo'] = 'bar'
                d['baz'] = 1337

            with SqliteDict(
                str(db_path),
                table = 'Name\\with"special\t-_ char""ac"""ters',
            ) as d:
                self.assertTrue(bool(d))
                self.assertEqual(tuple(d), ('foo', 'baz'))
                self.assertEqual(tuple(d.keys()), ('foo', 'baz'))
                self.assertEqual(tuple(d.items()), (('foo', 'bar'), ('baz', 1337)))
                self.assertEqual(tuple(d.values()), ('bar', 1337))
                self.assertEqual(len(d), 2)

    def test_autocommit(self):
        with TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / 'test.db'

            d = SimpleSqliteDict(str(db_path))
            self.assertFalse(bool(d))
            self.assertEqual(tuple(d), ())
            self.assertEqual(tuple(d.keys()), ())
            self.assertEqual(tuple(d.items()), ())
            self.assertEqual(tuple(d.values()), ())
            self.assertEqual(len(d), 0)
            d['foo'] = 'bar'
            d['baz'] = 1337

            d = SimpleSqliteDict(str(db_path))
            self.assertTrue(bool(d))
            self.assertEqual(tuple(d), ('foo', 'baz'))
            self.assertEqual(tuple(d.keys()), ('foo', 'baz'))
            self.assertEqual(tuple(d.items()), (('foo', 'bar'), ('baz', 1337)))
            self.assertEqual(tuple(d.values()), ('bar', 1337))
            self.assertEqual(len(d), 2)

    def test_isolation_level(self):
        with TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / 'test.db'

            d = SimpleSqliteDict(str(db_path), isolation_level='DEFERRED')
            self.assertFalse(bool(d))
            self.assertEqual(tuple(d), ())
            self.assertEqual(tuple(d.keys()), ())
            self.assertEqual(tuple(d.items()), ())
            self.assertEqual(tuple(d.values()), ())
            self.assertEqual(len(d), 0)
            d['foo'] = 'bar'
            d.connection.commit()
            d['baz'] = 1337
            d.connection.rollback()

            d = SimpleSqliteDict(str(db_path))
            self.assertTrue(bool(d))
            self.assertEqual(tuple(d), ('foo',))
            self.assertEqual(tuple(d.keys()), ('foo',))
            self.assertEqual(tuple(d.items()), (('foo', 'bar'),))
            self.assertEqual(tuple(d.values()), ('bar',))
            self.assertEqual(len(d), 1)

    def test_nested(self):
        with TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / 'test.db'

            with SqliteDict(str(db_path)) as d:
                d['foo'] = {'foo': 'bar', 'baz': [2, 'two']}

            with SqliteDict(str(db_path)) as d:
                self.assertEqual(d['foo'], {'foo': 'bar', 'baz': [2, 'two']})

    def test_json(self):
        with TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / 'test.db'

            with SqliteDict(str(db_path), serializer = json) as d:
                d['foo'] = {'foo': 'bar', 'baz': [2, 'two']}

            with SqliteDict(str(db_path), serializer = json) as d:
                self.assertEqual(d['foo'], {'foo': 'bar', 'baz': [2, 'two']})

    def test_orjson(self):
        with TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / 'test.db'

            with SqliteDict(str(db_path), serializer = orjson) as d:
                d['foo'] = {'foo': 'bar', 'baz': [2, 'two']}

            with SqliteDict(str(db_path), serializer = orjson) as d:
                self.assertEqual(d['foo'], {'foo': 'bar', 'baz': [2, 'two']})

    def test_pickle(self):
        with TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / 'test.db'

            with SqliteDict(str(db_path), serializer = pickle) as d:
                d['foo'] = {'foo': 'bar', 'baz': [2, 'two']}

            with SqliteDict(str(db_path), serializer = pickle) as d:
                self.assertEqual(d['foo'], {'foo': 'bar', 'baz': [2, 'two']})

    def test_marshal(self):
        with TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / 'test.db'

            with SqliteDict(str(db_path), serializer = marshal) as d:
                d['foo'] = {'foo': 'bar', 'baz': [2, 'two']}

            with SqliteDict(str(db_path), serializer = marshal) as d:
                self.assertEqual(d['foo'], {'foo': 'bar', 'baz': [2, 'two']})

    def test_expire(self):
        with TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / 'test.db'

            with SqliteDict(str(db_path)) as d:
                d['foo'] = 'bar'
                d['postponed'] = 'worked'

            with SqliteDict(str(db_path)) as d:
                d.lifespan = timedelta(weeks=-1)
                d.postpone_all()
                d.lifespan = timedelta(weeks=1)
                d.postpone('postponed')
                # This triggers the actual expiry
                d['baz'] = 1337

            with SqliteDict(str(db_path)) as d:
                self.assertTrue(bool(d))
                self.assertEqual(tuple(d), ('postponed', 'baz'))
                self.assertEqual(tuple(d.keys()), ('postponed', 'baz'))
                self.assertEqual(
                    tuple(d.items()),
                    (('postponed', 'worked'), ('baz', 1337)),
                )
                self.assertEqual(tuple(d.values()), ('worked', 1337))
                self.assertEqual(len(d), 2)

if __name__ == '__main__':
    unittest.main()
