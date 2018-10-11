# Announcing datasette-pytables 1.1.0

## What's new

This new release of datasette-pytables stops using datasette-core (a Datasette fork for supporting external connectors like this one) and starts using datasette-connectors.

## What it is

Datasette-PyTables provides a web interface and a JSON API for [PyTables](https://github.com/PyTables/PyTables) files, allowing them to be accessible for e.g. Javascript programs. It works in conjunction with [Datasette-Connectors](https://github.com/PyTables/datasette-connectors), a module that patches [Datasette](https://github.com/simonw/datasette), which provides a web interface for SQLite files. With this patch, it's possible to work with SQLite files in the usual way but external connectors are accepted for any kind of database files, so you can develop your own connector for your favourite data container if you want (read [developers doc](https://github.com/PyTables/datasette-connectors/blob/master/DEVELOPERS.md)).

## Resources

Visit the main datasette-pytables site repository at:
https://github.com/PyTables/datasette-pytables

----

  **Enjoy data!**
