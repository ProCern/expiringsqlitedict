=========================================================================
expiringsqlitedict -- persistent ``dict``, backed-up by SQLite and pickle
=========================================================================

|License|_

.. _Downloads: https://pypi.python.org/pypi/expiringsqlitedict
.. _License: https://pypi.python.org/pypi/expiringsqlitedict

A lightweight wrapper around Python's sqlite3 database with a simple, Pythonic
dict-like interface.  This fork is modified to implement a metatable and
automatic expiring and vacuuming semantics, as well as some appropriate locking.
This also compresses values automatically.

.. code-block:: python

  >>> from expiringsqlitedict import SqliteDict
  >>> with SqliteDict('./my_db.sqlite', autocommit=True) as mydict:
  >>>     mydict['some_key'] = any_picklable_object
  >>>     print mydict['some_key']  # prints the new value
  >>>     for key, value in mydict.iteritems():
  >>>         print key, value
  >>>     print len(mydict) # etc... all dict functions work

Pickle is used internally to (de)serialize the values. Keys are arbitrary strings,
values arbitrary pickle-able objects.  This must be used within a context
manager.

Features
--------

* Values can be **any picklable objects** (uses ``cPickle`` with the highest protocol).
* Support for **access from multiple programs or threads**, using a lockfile.
* Support for **custom serialization or compression**:

  .. code-block:: python

      # use JSON instead of pickle
      >>> import json
      >>> mydict = SqliteDict('./my_db.sqlite', encode=json.dumps, decode=json.loads)

      # apply zlib compression after pickling
      >>> import zlib, pickle, sqlite3
      >>> def my_encode(obj):
      ...     return sqlite3.Binary(zlib.compress(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)))
      >>> def my_decode(obj):
      ...     return pickle.loads(zlib.decompress(bytes(obj)))
      >>> mydict = SqliteDict('./my_db.sqlite', encode=my_encode, decode=my_decode)


Installation
------------

The module has no dependencies beyond Python itself.

Install or upgrade with::

    pip install expiringsqlitedict

or from the `source tar.gz <http://pypi.python.org/pypi/expiringsqlitedict>`_::

    python setup.py install

Documentation
-------------

Standard Python document strings are inside the module:

.. code-block:: python

  >>> import expiringsqlitedict
  >>> help(expiringsqlitedict)

(but it's just ``dict`` with a commit, really).

**Beware**: because of Python semantics, ``expiringsqlitedict`` cannot know when
a mutable SqliteDict-backed entry was modified in RAM. For example,
``mydict.setdefault('new_key', []).append(1)`` will leave ``mydict['new_key']``
equal to empty list, not ``[1]``. You'll need to explicitly assign the mutated
object back to SqliteDict to achieve the same effect:

.. code-block:: python

  >>> val = mydict.get('new_key', [])
  >>> val.append(1)  # sqlite DB not updated here!
  >>> mydict['new_key'] = val  # now updated


For developers
--------------

Install::

    # pip install nose
    # pip install coverage

To perform all tests::

   # make test-all

To perform all tests with coverage::

   # make test-all-with-coverage


Comments, bug reports
---------------------

``expiringsqlitedict`` resides on `github
<https://github.com/absperf/expiringsqlitedict>`_. You can file issues or pull
requests there.


----

``expiringsqlitedict`` is open source software released under the `Apache 2.0 license <http://opensource.org/licenses/apache2.0.php>`_.
Copyright (c) 2011-2018 `Radim Řehůřek <http://radimrehurek.com>`_ and contributors.  The changes in this fork copyright (c) 2018 Absolute Performance, Inc.
