Development
===========

This project uses ``uv`` to manage a ``virtualenv``.

All the commands will only install things locally to this repository.

To run mypy against this plugin::

  > ./types

To clear the cache first::

  > CLEAR_MYPY_CACHE=1 ./types 

To run tests::

  > ./test.sh

To activate the ``virtualenv`` in your current shell::

  > source dev activate

To build the docs locally::

  > ./dev docs view
