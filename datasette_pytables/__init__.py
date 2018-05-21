from collections import OrderedDict
from moz_sql_parser import parse
import re
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

def _parse_sql(sql, params):
    # Table name
    sql = re.sub('(?i)from \[(.*)]', 'from "\g<1>"', sql)
    # Params
    for param in params:
        sql = sql.replace(":" + param, param)

    try:
        parsed = parse(sql)
    except:
        # Propably it's a PyTables expression
        for token in ['group by', 'order by', 'limit']:
            res = re.search('(?i)where (.*)' + token, sql)
            if res:
                modified_sql = re.sub('(?i)where (.*)(' + token + ')', '\g<2>', sql)
                parsed = parse(modified_sql)
                parsed['where'] = res.group(1)
                break

    # Always a list of fields
    if type(parsed['select']) is not list:
        parsed['select'] = [parsed['select']]

    return parsed

_operators = {
    'eq': '==',
    'neq': '!=',
    'gt': '>',
    'gte': '>=',
    'lt': '<',
    'lte': '<=',
    'and': '&',
    'or': '|',
}

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

        parsed_sql = _parse_sql(sql, params)
        table = self.h5file.get_node(parsed_sql['from'])
        table_rows = []
        fields = parsed_sql['select']

        query = ''
        start = 0
        end = table.nrows

        # Use 'where' statement or get all the rows
        def _cast_param(field, pname):
            # Cast value to the column type
            coltype = table.coltypes[field]
            fcast = None
            if coltype == 'string':
                fcast = str
            elif coltype.startswith('int'):
                fcast = int
            elif coltype.startswith('float'):
                fcast = float
            if fcast:
                params[pname] = fcast(params[pname])

        def _translate_where(where):
            # Translate SQL to PyTables expression
            expr = ''
            operator = list(where)[0]

            if operator in ['and', 'or']:
                subexpr = [_translate_where(e) for e in where[operator]]
                subexpr = filter(lambda e: e, subexpr)
                subexpr = ["({})".format(e) for e in subexpr]
                expr = " {} ".format(_operators[operator]).join(subexpr)
            elif operator == 'exists':
                pass
            elif where == {'eq': ['rowid', 'p0']}:
                nonlocal start, end
                start = int(params['p0'])
                end = start + 1
            else:
                left, right = where[operator]
                if left in params:
                    _cast_param(right, left)
                elif right in params:
                    _cast_param(left, right)

                expr = "{left} {operator} {right}".format(left=left, operator=_operators.get(operator, operator), right=right)

            return expr

        if 'where' in parsed_sql:
            if type(parsed_sql['where']) is dict:
                query = _translate_where(parsed_sql['where'])
            else:
                query = parsed_sql['where']

        # Limit number of rows
        if 'limit' in parsed_sql:
            max_rows = int(parsed_sql['limit'])
            if end - start > max_rows:
                end = start + max_rows

        # Execute query
        if query:
            table_rows = table.where(query, params, start, end)
        else:
            table_rows = table.iterrows(start, end)

        # Prepare rows
        if len(fields) == 1 and type(fields[0]['value']) is dict and \
           fields[0]['value'].get('count') == '*':
            rows.append(Row({'count(*)': int(table.nrows)}))
        else:
            for table_row in table_rows:
                row = Row()
                for field in fields:
                    field_name = field['value']
                    if type(field_name) is dict and 'distinct' in field_name:
                        field_name = field_name['distinct']
                    if field_name == 'rowid':
                        row['rowid'] = int(table_row.nrow)
                    elif field_name == '*':
                        for col in table.colnames:
                            value = table_row[col]
                            if type(value) is bytes:
                                value = value.decode('utf-8')
                            row[col] = value
                    else:
                        row[field_name] = table_row[field_name]
                rows.append(row)

        # Prepare query description
        for field in [f['value'] for f in fields]:
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
