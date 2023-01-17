#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright © 2021 Taylor C. Richberger
# This code is released under the license described in the LICENSE file

import unittest
from datetime import timedelta
from tempfile import TemporaryDirectory
import time
from pathlib import Path
from expiringsqlitedict import SqliteDict, AutocommitSqliteDict, ZlibPickleSerializer, OnDemand
from typing import Any
import json
import marshal
import pickle

class TestExpiringDict(unittest.TestCase):
    def test_simple(self):
        with TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / 'test.db'

            with OnDemand(str(db_path), serializer=ZlibPickleSerializer) as d:
                self.assertFalse(bool(d))
                self.assertEqual(set(d), set())
                self.assertEqual(set(d.keys()), set())
                self.assertEqual(set(d.items()), set())
                self.assertEqual(set(d.values()), set())
                self.assertEqual(len(d), 0)
                d['foo'] = 'bar'
                d['baz'] = 1337

            with SqliteDict(str(db_path), serializer=ZlibPickleSerializer) as d:
                self.assertTrue(bool(d))
                self.assertEqual(set(d), {'foo', 'baz'})
                self.assertEqual(set(d.keys()), {'foo', 'baz'})
                self.assertEqual(set(d.items()), {('foo', 'bar'), ('baz', 1337)})
                self.assertEqual(set(d.values()), {'bar', 1337})
                self.assertEqual(len(d), 2)

    def test_autocommit(self):
        with TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / 'test.db'

            d = AutocommitSqliteDict(str(db_path))
            self.assertFalse(bool(d))
            self.assertEqual(set(d), set())
            self.assertEqual(set(d.keys()), set())
            self.assertEqual(set(d.items()), set())
            self.assertEqual(set(d.values()), set())
            self.assertEqual(len(d), 0)
            d['foo'] = 'bar'
            d['baz'] = 1337

            d = AutocommitSqliteDict(str(db_path))
            self.assertTrue(bool(d))
            self.assertEqual(set(d), {'foo', 'baz'})
            self.assertEqual(set(d.keys()), {'foo', 'baz'})
            self.assertEqual(set(d.items()), {('foo', 'bar'), ('baz', 1337)})
            self.assertEqual(set(d.values()), {'bar', 1337})
            self.assertEqual(len(d), 2)

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
                self.assertEqual(set(d.items()), {('baz', 1337), ('postponed', 'worked')})
                self.assertEqual(set(d.values()), {1337, 'worked'})
                self.assertEqual(len(d), 2)

if __name__ == '__main__':
    unittest.main()
