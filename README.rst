expiringsqlitedict -- expiring file-backed ``dict``
===================================================

.. _Downloads: https://pypi.python.org/pypi/expiringsqlitedict
.. _License: https://pypi.python.org/pypi/expiringsqlitedict

A lightweight wrapper around Python's sqlite3 database with a MutableMapping
interface::

  from expiringsqlitedict import SqliteDict
  with SqliteDict('./my_db.sqlite') as mydict:
      mydict['some_key'] = any_picklable_object
      print(mydict['some_key'])  # prints the new value
      for key, value in mydict.items():
          print((key, value))
      print(len(mydict)) # etc... all dict functions work

json is used internally by default to serialize the values. Keys are
arbitrary strings, values arbitrary json-able objects.  This must be used
within a context manager, and serialization can be overridden with your own.
The database is wrapped with a transaction, and any exception thrown out of the
context manager rolls back all changes.

This was forked off of `sqlitedict <https://github.com/RaRe-Technologies/sqlitedict>`_
in order to add auto-expiring functionality, and initially was quite similar to
it.  Version 2.0 split of completely and takes the module into a complete
rewrite, mostly to remove unnecessary Python 2 compatibility, simplify the API,
completely enforce a context manager for typical cases, add full typing
throughout, and use sqlite triggers for expiration cleanup.

Version 3 set the default encoding to json, and made many other API refinements.

This version also does not vacuum at all automatically.  It did in previous
versions, but this was kind of a silly behavior to put into the library itself.
If you want your database file intermittently vacuumed, you should put such
behavior into a crontab or use the ``sqlite3`` module to do it yourself
intermittently.

Features
--------

* Values can be any json-capable objects (this can be customized to be as
  flexible as you need, through custom serializers)
* Support for access from multiple programs or threads, with locking fully
  managed by sqlite itself.
* A very simple codebase that is easy to read, relying on sqlite for as much
  behavior as possible.
* A simple autocommit wrapper (``SimpleSqliteDict``), if you really can't
  handle a context manager and need something that fully handles like a dict.
  You can specify a ``isolation_level`` on this to have to commit and roll back
  yourself.
* An on-demand wrapper (``OnDemand``), for situations where you want to open and
  close the database in as narrow a window as possible.
* Support for custom serialization or compression:

.. code-block:: python

  import json
    
  with SqliteDict('some.db', serializer=json) as mydict:
      mydict['some_key'] = some_json_encodable_object
      print(mydict['some_key'])

Installation
------------

The module has no dependencies beyond Python itself.

Install or upgrade with::

    pip install expiringsqlitedict

or from the `source tar.gz <http://pypi.python.org/pypi/expiringsqlitedict>`_::

    python setup.py install

This module is a single file, so you could also easily import the module in your
own tree, if your workflow needs that.

Testing
-------

You may test this by running ``test.py`` with ``PYTHONPATH`` set to the current
working directory.  There is a convenience makefile to do this for you when you
run:

.. code-block:: sh

  make test

Documentation
-------------

Standard Python document strings are inside the module::

  >>> import expiringsqlitedict
  >>> help(expiringsqlitedict)

Comments, bug reports
---------------------

``expiringsqlitedict`` resides on `github <https://github.com/absperf/expiringsqlitedict>`_. You can file issues or pull
requests there.


----

``expiringsqlitedict`` is open source software released under the
`Apache 2.0 license <http://opensource.org/licenses/apache2.0.php>`_.
Version <2 Copyright (c) 2011-2018 `Radim Řehůřek <http://radimrehurek.com>`_ and contributors.
All versions copyright (c) 2018-2022 Absolute Performance, Inc.
