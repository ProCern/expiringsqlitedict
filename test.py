#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright © 2021 Taylor C. Richberger
# This code is released under the license described in the LICENSE file

import unittest
from datetime import timedelta
from tempfile import TemporaryDirectory
from pathlib import Path
from expiringsqlitedict import SqliteDict, SimpleSqliteDict
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
                self.assertEqual(set(d), set())
                self.assertEqual(set(d.keys()), set())
                self.assertEqual(set(d.items()), set())
                self.assertEqual(set(d.values()), set())
                self.assertEqual(len(d), 0)
                d['foo'] = 'bar'
                d['baz'] = 1337

            with SqliteDict(str(db_path)) as d:
                self.assertTrue(bool(d))
                self.assertEqual(set(d), {'foo', 'baz'})
                self.assertEqual(set(d.keys()), {'foo', 'baz'})
                self.assertEqual(set(d.items()), {('foo', 'bar'), ('baz', 1337)})
                self.assertEqual(set(d.values()), {'bar', 1337})
                self.assertEqual(len(d), 2)

            with SqliteDict(str(db_path)) as d:
                del d['foo']

            with SqliteDict(str(db_path)) as d:
                self.assertTrue(bool(d))
                self.assertEqual(set(d), {'baz'})
                self.assertEqual(set(d.keys()), {'baz'})
                self.assertEqual(set(d.items()), {('baz', 1337)})
                self.assertEqual(set(d.values()), {1337})
                self.assertEqual(len(d), 1)

            with self.assertRaises(KeyError):
                with SqliteDict(str(db_path)) as d:
                    del d['foo']

    def test_simple_reentrant(self):
        with TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / 'test.db'

            manager = SqliteDict(str(db_path))

            with manager as d:
                self.assertFalse(bool(d))
                self.assertEqual(set(d), set())
                self.assertEqual(set(d.keys()), set())
                self.assertEqual(set(d.items()), set())
                self.assertEqual(set(d.values()), set())
                self.assertEqual(len(d), 0)
                d['foo'] = 'bar'
                d['baz'] = 1337

            with manager as d:
                self.assertTrue(bool(d))
                self.assertEqual(set(d), {'foo', 'baz'})
                self.assertEqual(set(d.keys()), {'foo', 'baz'})
                self.assertEqual(set(d.items()), {('foo', 'bar'), ('baz', 1337)})
                self.assertEqual(set(d.values()), {'bar', 1337})
                self.assertEqual(len(d), 2)

            with manager as d:
                del d['foo']

            with manager as d:
                self.assertTrue(bool(d))
                self.assertEqual(set(d), {'baz'})
                self.assertEqual(set(d.keys()), {'baz'})
                self.assertEqual(set(d.items()), {('baz', 1337)})
                self.assertEqual(set(d.values()), {1337})
                self.assertEqual(len(d), 1)

            with self.assertRaises(KeyError):
                with manager as d:
                    del d['foo']

    def test_quotes(self):
        with TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / 'test.db'

            with SqliteDict(
                str(db_path),
                table = 'Name\\with"special\t-_ char""ac"""ters',
            ) as d:
                self.assertFalse(bool(d))
                self.assertEqual(set(d), set())
                self.assertEqual(set(d.keys()), set())
                self.assertEqual(set(d.items()), set())
                self.assertEqual(set(d.values()), set())
                self.assertEqual(len(d), 0)
                d['foo'] = 'bar'
                d['baz'] = 1337

            with SqliteDict(
                str(db_path),
                table = 'Name\\with"special\t-_ char""ac"""ters',
            ) as d:
                self.assertTrue(bool(d))
                self.assertEqual(set(d), {'foo', 'baz'})
                self.assertEqual(set(d.keys()), {'foo', 'baz'})
                self.assertEqual(set(d.items()), {('foo', 'bar'), ('baz', 1337)})
                self.assertEqual(set(d.values()), {'bar', 1337})
                self.assertEqual(len(d), 2)

    def test_autocommit(self):
        with TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / 'test.db'

            d = SimpleSqliteDict(str(db_path))
            self.assertFalse(bool(d))
            self.assertEqual(set(d), set())
            self.assertEqual(set(d.keys()), set())
            self.assertEqual(set(d.items()), set())
            self.assertEqual(set(d.values()), set())
            self.assertEqual(len(d), 0)
            d['foo'] = 'bar'
            d['baz'] = 1337

            d = SimpleSqliteDict(str(db_path))
            self.assertTrue(bool(d))
            self.assertEqual(set(d), {'foo', 'baz'})
            self.assertEqual(set(d.keys()), {'foo', 'baz'})
            self.assertEqual(set(d.items()), {('foo', 'bar'), ('baz', 1337)})
            self.assertEqual(set(d.values()), {'bar', 1337})
            self.assertEqual(len(d), 2)

    def test_isolation_level(self):
        with TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / 'test.db'

            d = SimpleSqliteDict(str(db_path), isolation_level='DEFERRED')
            self.assertFalse(bool(d))
            self.assertEqual(set(d), set())
            self.assertEqual(set(d.keys()), set())
            self.assertEqual(set(d.items()), set())
            self.assertEqual(set(d.values()), set())
            self.assertEqual(len(d), 0)
            d['foo'] = 'bar'
            d.connection.commit()
            d['baz'] = 1337
            d.connection.rollback()

            d = SimpleSqliteDict(str(db_path))
            self.assertTrue(bool(d))
            self.assertEqual(set(d), {'foo'})
            self.assertEqual(set(d.keys()), {'foo'})
            self.assertEqual(set(d.items()), {('foo', 'bar')})
            self.assertEqual(set(d.values()), {'bar'})
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
                self.assertEqual(set(d), {'baz', 'postponed'})
                self.assertEqual(set(d.keys()), {'baz', 'postponed'})
                self.assertEqual(
                    set(d.items()),
                    {('baz', 1337), ('postponed', 'worked')},
                )
                self.assertEqual(set(d.values()), {1337, 'worked'})
                self.assertEqual(len(d), 2)

if __name__ == '__main__':
    unittest.main()
