from collections import OrderedDict
import sqlparse
import tables

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
            'count': int(table.nrows),
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
        elif type(token) is sqlparse.sql.Where:
            parsed_sql['where'] = token
        else:
            if not token.is_whitespace:
                parsed_sql[current_keyword] += str(token)
    return parsed_sql

_operators = {
    '=': '==',
}

def _translate_condition(table, condition, params):
    field = condition.left.get_real_name()

    operator = list(filter(lambda t: t.ttype == sqlparse.tokens.Comparison, condition.tokens))[0]
    if operator.value in _operators:
        operator = _operators[operator.value]
    else:
        operator = operator.value

    value = condition.right.value
    if value.startswith(':'):
        # Value is a parameters
        value = value[1:]
        if value in params:
            # Cast value to the column type
            coltype = table.coltypes[field]
            if coltype == 'string':
                params[value] = str(params[value])
            elif coltype.startswith('int'):
                params[value] = int(params[value])
            elif coltype.startswith('float'):
                params[value] = float(params[value])

    translated = "{left} {operator} {right}".format(left=field, operator=operator, right=value)
    return translated, params

class Connection:
    def __init__(self, path):
        self.path = path
        self.h5file = tables.open_file(path)

    def execute(self, sql, params=None, truncate=False):
        if params is None:
            params = {}
        rows = []
        truncated = False
        description = []

        parsed_sql = _parse_sql(sql)
        table = self.h5file.get_node(parsed_sql['from'][1:-1])
        table_rows = []
        fields = parsed_sql['select'].split(',')

        # Use 'where' statement or get all the rows
        if 'where' in parsed_sql:
            query = ''
            start = 0
            end = table.nrows
            try:
                conditions = []
                for condition in parsed_sql['where'].get_sublists():
                    if str(condition) == '"rowid"=:p0':
                        start = int(params['p0'])
                        end = start + 1
                    else:
                        translated, params = _translate_condition(table, condition, params)
                        conditions.append(translated)
                if conditions:
                    query = ') & ('.join(conditions)
                    query = '(' + query + ')'
            except:
                # Probably it's a PyTables query
                query = str(parsed_sql['where'])[6:]    # without where keyword

            if query:
                table_rows = table.where(query, params, start, end)
            else:
                table_rows = table.iterrows(start, end)
        else:
            table_rows = table.iterrows()

        # Prepare rows
        if len(fields) == 1 and fields[0] == 'count(*)':
            rows.append(Row({fields[0]: int(table.nrows)}))
        else:
            for table_row in table_rows:
                row = Row()
                for field in fields:
                    if field == 'rowid':
                        row[field] = int(table_row.nrow)
                    elif field == '*':
                        for col in table.colnames:
                            value = table_row[col]
                            if type(value) is bytes:
                                value = value.decode('utf-8')
                            row[col] = value
                    else:
                        row[field] = table_row[field]
                rows.append(row)

        # Prepare query description
        for field in fields:
            if field == '*':
                for col in table.colnames:
                    description.append((col,))
            else:
                description.append((field,))

        # Return the rows
        if truncate:
            return rows, truncated, tuple(description)
        else:
            return rows

class Row(OrderedDict):
    def __getitem__(self, label):
        if type(label) is int:
            return super(OrderedDict, self).__getitem__(list(self.keys())[label])
        else:
            return super(OrderedDict, self).__getitem__(label)

    def __iter__(self):
        return self.values().__iter__()
