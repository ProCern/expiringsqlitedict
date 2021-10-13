#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright © 2021 Taylor C. Richberger
# This code is released under the license described in the LICENSE file

import unittest
from datetime import timedelta
from tempfile import TemporaryDirectory
import time
from pathlib import Path
from expiringsqlitedict import SqliteDict, AutocommitSqliteDict
from typing import Any
import json

class JsonSerializer:
    @staticmethod
    def loads(data: bytes) -> Any:
        return json.loads(data.decode('utf-8'))

    @staticmethod
    def dumps(value: Any) -> bytes:
        return json.dumps(value).encode('utf-8')

class TestExpiringDict(unittest.TestCase):
    def test_simple(self):
        with TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / 'test.db'

            with SqliteDict(db_path) as d:
                self.assertFalse(bool(d))
                self.assertEqual(set(d), set())
                self.assertEqual(set(d.keys()), set())
                self.assertEqual(set(d.items()), set())
                self.assertEqual(set(d.values()), set())
                self.assertEqual(len(d), 0)
                d['foo'] = 'bar'
                d['baz'] = 1337

            with SqliteDict(db_path) as d:
                self.assertTrue(bool(d))
                self.assertEqual(set(d), {'foo', 'baz'})
                self.assertEqual(set(d.keys()), {'foo', 'baz'})
                self.assertEqual(set(d.items()), {('foo', 'bar'), ('baz', 1337)})
                self.assertEqual(set(d.values()), {'bar', 1337})
                self.assertEqual(len(d), 2)

    def test_autocommit(self):
        with TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / 'test.db'

            d = AutocommitSqliteDict(db_path)
            self.assertFalse(bool(d))
            self.assertEqual(set(d), set())
            self.assertEqual(set(d.keys()), set())
            self.assertEqual(set(d.items()), set())
            self.assertEqual(set(d.values()), set())
            self.assertEqual(len(d), 0)
            d['foo'] = 'bar'
            d['baz'] = 1337

            d = AutocommitSqliteDict(db_path)
            self.assertTrue(bool(d))
            self.assertEqual(set(d), {'foo', 'baz'})
            self.assertEqual(set(d.keys()), {'foo', 'baz'})
            self.assertEqual(set(d.items()), {('foo', 'bar'), ('baz', 1337)})
            self.assertEqual(set(d.values()), {'bar', 1337})
            self.assertEqual(len(d), 2)

    def test_nested(self):
        with TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / 'test.db'

            with SqliteDict(db_path) as d:
                d['foo'] = {'foo': 'bar', 'baz': [2, 'two']}

            with SqliteDict(db_path) as d:
                self.assertEqual(d['foo'], {'foo': 'bar', 'baz': [2, 'two']})

    def test_json(self):
        with TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / 'test.db'

            with SqliteDict(db_path, serializer = JsonSerializer()) as d:
                d['foo'] = {'foo': 'bar', 'baz': [2, 'two']}

            with SqliteDict(db_path, serializer = JsonSerializer()) as d:
                self.assertEqual(d['foo'], {'foo': 'bar', 'baz': [2, 'two']})

    def test_expire(self):
        with TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / 'test.db'

            with SqliteDict(db_path, lifespan=timedelta(seconds=1)) as d:
                d['foo'] = 'bar'

            time.sleep(2.0)

            with SqliteDict(db_path) as d:
                d['baz'] = 1337

            with SqliteDict(db_path) as d:
                self.assertTrue(bool(d))
                self.assertEqual(set(d), {'baz'})
                self.assertEqual(set(d.keys()), {'baz'})
                self.assertEqual(set(d.items()), {('baz', 1337)})
                self.assertEqual(set(d.values()), {1337})
                self.assertEqual(len(d), 1)

if __name__ == '__main__':
    unittest.main()
