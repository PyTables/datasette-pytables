# Release notes for datasette-pytables


## Changes from 2.0.1 to 2.0.2-dev

  #XXX version-specific blurb XXX#


## Changes from 1.1.0 to 2.0.1

* Communication with Datasette using datasette-connectors 2.0.1


## Changes from 1.0.3 to 1.1.0

* Communication with Datasette using datasette-connectors.

* Fix compatibility problems with sqlite standard queries.

* Fix some problems with fixtures when testing.


## Changes from 1.0.2 to 1.0.3

* Freeze specific versions for moz-sql-parser and mo-future


## Initial version 1.0.2

* Filters (e.g. `temp >= 3`) are working.

* Sorted by is working for columns with CSI indexes.

* Pagination is implemented for the first dimension of tables or arrays.

* Arrays can be visualized (at least when they are small).
