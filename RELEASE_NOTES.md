# Release notes for datasette-pytables


## Changes from 1.0.2 to 1.0.3

* Freeze specific versions for moz-sql-parser and mo-future


## Initial version 1.0.2

* Filters (e.g. `temp >= 3`) are working.

* Sorted by is working for columns with CSI indexes.

* Pagination is implemented for the first dimension of tables or arrays.

* Arrays can be visualized (at least when they are small).
