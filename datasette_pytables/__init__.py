import tables
import sqlparse
from collections import OrderedDict

_connector_type = 'pytables'

def inspect(path):
    "Open file and return tables info"
    h5tables = {}
    views = []
    h5file = tables.open_file(path)

    for table in filter(lambda node: not(isinstance(node, tables.group.Group)), h5file):
        colnames = []
        if isinstance(table, tables.table.Table):
            colnames = table.colnames

        h5tables[table._v_pathname] = {
            'name': table._v_pathname,
            'columns': colnames,
            'primary_keys': [],
            'count': table.nrows,
            'label_column': None,
            'hidden': False,
            'fts_table': None,
            'foreign_keys': {'incoming': [], 'outgoing': []},
        }

    h5file.close()
    return h5tables, views, _connector_type

def _parse_sql(sql):
    parsed = sqlparse.parse(sql)
    stmt = parsed[0]
    parsed_sql = {}
    current_keyword = ""
    for token in stmt.tokens:
        if token.is_keyword:
            if current_keyword in parsed_sql and parsed_sql[current_keyword] == '':
                # Check composed keywords like 'order by'
                del parsed_sql[current_keyword]
                current_keyword += " " + str(token)
            else:
                current_keyword = str(token)
            parsed_sql[current_keyword] = ""
        else:
            if not token.is_whitespace:
                parsed_sql[current_keyword] += str(token)
    return parsed_sql

class Connection:
    def __init__(self, path):
        self.path = path
        self.h5file = tables.open_file(path)

    def execute(self, sql, params=None, truncate=False):
        rows = []
        truncated = False
        description = []

        parsed_sql = _parse_sql(sql)
        table = self.h5file.get_node(parsed_sql['from'][1:-1])
        table_rows = []
        fields = parsed_sql['select'].split(',')

        # Use 'where' statement or get all the rows
        if 'where' in parsed_sql:
            pass
        else:
            table_rows = table.iterrows()

        if len(fields) == 1 and fields[0] == 'count(*)':
            rows.append(Row({'count(*)': table.nrows}))
        else:
            for table_row in table_rows:
                row = Row()
                for field in fields:
                    if field == 'rowid':
                        row[field] = table_row.nrow
                    elif field == '*':
                        for col in table.colnames:
                            row[col] = table_row[col]
                    else:
                        row[field] = table_row[field]
                rows.append(row)

        description = ((col,) for col in table.colnames)

        if truncate:
            return rows, truncated, description
        else:
            return rows

class Row(OrderedDict):
    def __getitem__(self, label):
        if type(label) is int:
            return super(OrderedDict, self).__getitem__(list(self.keys())[label])
