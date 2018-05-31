# How is datasette-pytables made?

Datasette-PyTables is an external connector for [Datasette](https://github.com/simonw/datasette). Datasette publish data in SQLite files to the Internet with a JSON API, and this connector provides a way to do the same with PyTables files.

Using a modified version of Datasette, [Datasette-Core](https://github.com/PyTables/datasette-core), we can load external connectors that allow us to access to any data container. For this, the connectors need a certain structure.

Reviewing datasette-pytables code, you will see how to make other connectors for your needs.

## Tables inspection

First of all, we need to export a special method called `inspect` that receives the path of the file as an argument and returns a tuple formed by a dictionary with tables info, a list with views name and a string identifying the connector.

Each entry in the dictionary for tables info has the next structure:

    tables['table_name'] = {
        'name': 'table_name',
        'columns': ['c1', 'c2'],
        'primary_keys': [],
        'count': 100,
        'label_column': None,
        'hidden': False,
        'fts_table': None,
        'foreign_keys': {'incoming': [], 'outgoing': []}

This structure is used for PyTables. Maybe, in your case, you will need things like primary keys or foreign keys.

## Returning results

Datasette runs through SQL queries, so your connector has to accept these queries and execute them. The next class and methods are needed:

    class Connection:
        def __init__(self, path):
            ...

        def execute(self, sql, params=None, truncate=False, page_size=None, max_returned_rows=None):
            ...

The `execute` method receives:

* **sql**: the query
* **params**: a dictionary with the params used in the query
* **truncate**: a boolean saying if the returned data can be separated in pages or not
* **page_size**: the number of rows a page can contain
* **max_returned_rows**: the maximum number of rows Datasette expects

We need to parse the query because PyTables has its own style for queries, but other databases could work with the SQL queries without requiring any parsing.

Sometimes, Datasette make queries to `sqlite_master`; you need to keep it in mind.

The `execute` method has to return a tuple with:

* a list of rows (Datasette expects something like SQLite rows)
* a boolean saying if the data is truncated, i.e., if we return all the rows or there are more rows than the maximum indicated in max_returned_rows
* a tuple with the description of the columns in the form `(('c1',), ('c2',), ...)`

## Rows format

Datasette receives the results from the queries with SQLite row instances, so we need to return our rows in a similar way.

For example, if we have the next query:

    SELECT name FROM persons

we need to return an object that allows to do things that:

    row[0] == 'Susan'
    row['name'] == 'Susan'
    [c for c in row] == ['Susan']
    json.dumps(row)

We extend `list` class to get it, but if you respect the requirements for rows, you can develop your own implementation.
