# Announcing datasette-pytables 1.0.2

## What's new

This is the first public release.  Please, some feedback would be very appreciate.

Many things are working, most specially:

* Filters (e.g. `temp >= 3`) are working.

* Sorted by is working for columns with CSI indexes.

* Pagination is implemented for the first dimension of tables or arrays.

* Arrays can be visualized (at least when they are small).

## What it is

Datasette-PyTables provides a web interface and a JSON API for [PyTables](https://github.com/PyTables/PyTables) files, allowing them to be accessible for e.g. Javascript programs. It works in conjunction with [Datasette-Core](https://github.com/PyTables/datasette-core), a trivial fork of the original [Datasette](https://github.com/simonw/datasette), which provides a web interface for SQLite files.  This fork is able to work with SQLite files, like the original project, but can accept external connectors for any kind of database files, so you can develop your own connector for your favourite data container if you want (read [developers doc](https://github.com/PyTables/datasette-pytables/blob/master/DEVELOPERS.md)).

## Resources

Visit the main datasette-pytables site repository at:
https://github.com/PyTables/datasette-pytables

----

  **Enjoy data!**
