# Announcing datasette-pytables 2.0.0

## What's new

This new release of datasette-pytables uses datasette-connectors 2.0.0, which provides the last Datasette version with design and security improvings and many changes in its API. The main change for datasette-pytables users is that percent character has to be used instead slash for table names when writing queries in the Datasette query editor.

## What it is

Datasette-PyTables provides a web interface and a JSON API for [PyTables](https://github.com/PyTables/PyTables) files, allowing them to be accessible for e.g. Javascript programs. It works in conjunction with [Datasette-Connectors](https://github.com/PyTables/datasette-connectors), a module that patches [Datasette](https://github.com/simonw/datasette), which provides a web interface for SQLite files. With this patch, it's possible to work with SQLite files in the usual way but external connectors are accepted for any kind of database files, so you can develop your own connector for your favourite data container if you want (read [developers doc](https://github.com/PyTables/datasette-connectors/blob/master/DEVELOPERS.md)).

## Resources

Visit the main datasette-pytables site repository at:
https://github.com/PyTables/datasette-pytables

----

  **Enjoy data!**
