# std imports
import json
import unittest
import tempfile
import shutil
import sys
import os

# local
import expiringsqlitedict as sqlitedict
from expiringsqlitedict import SqliteDict
from accessories import norm_file, TestCaseBackport


class SqliteMiscTest(TestCaseBackport):

    def test_with_statement(self):
        """Verify using sqlitedict as a contextmanager . """
        with tempfile.NamedTemporaryFile() as file:
            with SqliteDict(file.name) as d:
                self.assertTrue(isinstance(d, SqliteDict))
                self.assertEqual(dict(d), {})
                self.assertEqual(list(d), [])
                self.assertEqual(len(d), 0)

    def test_reopen_conn(self):
        """Verify using a contextmanager that a connection can be reopened."""
        fname = norm_file('tests/db/sqlitedict-override-test.sqlite')
        db = SqliteDict(filename=fname)
        with db:
            db['key'] = 'value'
        with db:
            self.assertEqual(db['key'], 'value')
            db['key'] = 'othervalue'
        with db:
            self.assertEqual(db['key'], 'othervalue')

    def test_reopen_conn_rollback(self):
        """Verify using a contextmanager that commits are rolled back on exceptions"""
        fname = norm_file('tests/db/sqlitedict-override-test.sqlite')
        db = SqliteDict(filename=fname)
        with db:
            db['key'] = 'value'
        with self.assertRaisesRegex(RuntimeError, 'This rolls back'):
            with db:
                db['key'] = 'othervalue'
                raise RuntimeError('This rolls back')
        with db:
            self.assertEqual(db['key'], 'value')

    def test_as_str(self):
        """Verify SqliteDict.__str__()."""
        # given,
        db = SqliteDict(':memory:')
        # exercise
        db.__str__()
        # test when db closed
        db.close()
        db.__str__()

    def test_as_repr(self):
        """Verify SqliteDict.__repr__()."""
        # given,
        db = SqliteDict(':memory:')
        # exercise
        db.__repr__()

    def test_directory_notfound(self):
        """Verify RuntimeError: directory does not exist."""
        # given: a non-existent directory,
        folder = tempfile.mkdtemp(prefix='sqlitedict-test')
        shutil.rmtree(folder)
        # exercise,
        with self.assertRaises(RuntimeError):
            SqliteDict(filename=os.path.join(folder, 'nonexistent'))


class SqliteDictTerminateTest(unittest.TestCase):

    def test_terminate_instead_close(self):
        ''' make terminate() instead of close()
        '''
        d = sqlitedict.open('tests/db/sqlitedict-terminate.sqlite')
        d['abc'] = 'def'
        d.commit()
        self.assertEqual(d['abc'], 'def')
        d.terminate()
        self.assertFalse(os.path.isfile('tests/db/sqlitedict-terminate.sqlite'))

class SqliteDictTerminateFailTest(unittest.TestCase):
    """Provide Coverage for SqliteDict.terminate()."""

    def setUp(self):
        self.fname = norm_file('tests/db-permdenied/sqlitedict.sqlite')
        self.db = SqliteDict(filename=self.fname)
        with self.db:
            pass
        os.chmod(self.fname, 0o000)
        os.chmod(os.path.dirname(self.fname), 0o000)

    def tearDown(self):
        os.chmod(os.path.dirname(self.fname), 0o700)
        os.chmod(self.fname, 0o600)
        os.unlink(self.fname)
        shutil.rmtree(os.path.dirname(self.fname))

    def test_terminate_cannot_delete(self):
        # exercise,
        self.db.terminate()  # deletion failed, but no exception raised!

        # verify,
        os.chmod(os.path.dirname(self.fname), 0o700)
        os.chmod(self.fname, 0o600)
        self.assertTrue(os.path.exists(self.fname))

class SqliteDictJsonSerializationTest(unittest.TestCase):
    def setUp(self):
        self.fname = norm_file('tests/db-json/sqlitedict.sqlite')
        self.db = SqliteDict(
            filename=self.fname, encode=json.dumps, decode=json.loads
        )

    def tearDown(self):
        self.db.close()
        os.unlink(self.fname)
        shutil.rmtree(os.path.dirname(self.fname))

    def get_json(self, key):
        return self.db.select_one('SELECT value FROM expiringsqlitedict WHERE key = ?', (key,))[0]

    def test_int(self):
        self.db['test'] = -42
        assert self.db['test'] == -42
        assert self.get_json('test') == '-42'

    def test_str(self):
        test_str = u'Test \u30c6\u30b9\u30c8'
        self.db['test'] = test_str
        assert self.db['test'] == test_str
        assert self.get_json('test') == r'"Test \u30c6\u30b9\u30c8"'

    def test_bool(self):
        self.db['test'] = False
        assert self.db['test'] is False
        assert self.get_json('test') == 'false'

    def test_none(self):
        self.db['test'] = None
        assert self.db['test'] is None
        assert self.get_json('test') == 'null'

    def test_complex_struct(self):
        test_value = {
            'version': 2.5,
            'items': ['one', 'two'],
        }
        self.db['test'] = test_value
        assert self.db['test'] == test_value
        assert self.get_json('test') == json.dumps(test_value)
