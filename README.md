# datasette-pytables

Datasette-PyTables provides a web interface and a JSON API for [PyTables](https://github.com/PyTables/PyTables) files, allowing them to be accessible for Javascript programs, for example. It works in conjunction with [Datasette-Core](https://github.com/PyTables/datasette-core), a modified version of the original [Datasette](https://github.com/simonw/datasette), which provides a web interface for SQLite files.

The modified version is able to work with SQLite files, like the original project, but can accept external connectors for any kind of database files, so you can develop your own connector for your favourite data container if you want (read [developers doc](https://github.com/PyTables/datasette-pytables/blob/master/DEVELOPERS.md))

## Installation

Run `pip install datasette-pytables` to add the modified version of Datasette and the PyTables connector to your environment. Easy!

## How to serve PyTables files

    datasette serve path/to/data.h5

This will start a web server on port 8001, so you can access to your data visiting [http://localhost:8001/](http://localhost:8001/)

Read the [Datasette documentation](http://datasette.readthedocs.io/en/latest/) for more advanced options.
