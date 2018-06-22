# Announcing datasette-pytables 1.0.3

## What's new

This new release of datasette-pytables fix a compatibility problem with the last versions of Mozilla SQL Parser, which is used by datasette-pytables for translating queries from SQL to NumExpr syntax.

## What it is

Datasette-PyTables provides a web interface and a JSON API for [PyTables](https://github.com/PyTables/PyTables) files, allowing them to be accessible for e.g. Javascript programs. It works in conjunction with [Datasette-Core](https://github.com/PyTables/datasette-core), a trivial fork of the original [Datasette](https://github.com/simonw/datasette), which provides a web interface for SQLite files.  This fork is able to work with SQLite files, like the original project, but can accept external connectors for any kind of database files, so you can develop your own connector for your favourite data container if you want (read [developers doc](https://github.com/PyTables/datasette-pytables/blob/master/DEVELOPERS.md)).

## Resources

Visit the main datasette-pytables site repository at:
https://github.com/PyTables/datasette-pytables

----

  **Enjoy data!**
