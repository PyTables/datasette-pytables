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
        for token in ['group by', 'order by', 'limit', '']:
            res = re.search('(?i)where (.*)' + token, sql)
            if res:
                modified_sql = re.sub('(?i)where (.*)(' + token + ')', '\g<2>', sql)
                parsed = parse(modified_sql)
                parsed['where'] = res.group(1).strip()
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

    def execute(self, sql, params=None, truncate=False, page_size=None, max_returned_rows=None):
        if params is None:
            params = {}
        rows = []
        truncated = False
        description = []

        parsed_sql = _parse_sql(sql, params)

        if parsed_sql['from'] == 'sqlite_master':
            rows = self._execute_datasette_query(sql, params)
            description = (('value',))
            return rows, truncated, description

        table = self.h5file.get_node(parsed_sql['from'])
        table_rows = []
        fields = parsed_sql['select']

        query = ''
        start = 0
        end = table.nrows

        # Use 'where' statement or get all the rows
        def _cast_param(field, pname):
            # Cast value to the column type
            if type(table) is tables.table.Table:
                coltype = table.coltypes[field]
            else:
                coltype = table.dtype.name
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
            nonlocal start, end
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
                start = int(params['p0'])
                end = start + 1
            elif where == {'gt': ['rowid', 'p0']}:
                start = int(params['p0']) + 1
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

        # Truncate if needed
        if page_size and max_returned_rows and truncate:
            if max_returned_rows == page_size:
                max_returned_rows += 1
            if end - start > max_returned_rows:
                end = start + max_returned_rows
                truncated = True

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
            if type(table) is tables.table.Table:
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
            else:
                # Any kind of array
                rowid = start - 1
                for table_row in table_rows:
                    row = Row()
                    rowid += 1
                    for field in fields:
                        field_name = field['value']
                        if type(field_name) is dict and 'distinct' in field_name:
                            field_name = field_name['distinct']
                        if field_name == 'rowid':
                            row['rowid'] = rowid
                        else:
                            value = table_row
                            if type(value) is bytes:
                                value = value.decode('utf-8')
                            row['value'] = value
                    rows.append(row)

        # Prepare query description
        for field in [f['value'] for f in fields]:
            if field == '*':
                if type(table) is tables.table.Table:
                    for col in table.colnames:
                        description.append((col,))
                else:
                    description.append(('value',))
            else:
                description.append((field,))

        # Return the rows
        return rows, truncated, tuple(description)

    def _execute_datasette_query(self, sql, params):
        "Datasette special queries for getting tables info"
        if sql == "SELECT count(*) from sqlite_master WHERE type = 'view' and name=:n":
            row = Row()
            row['count(*)'] = 0
            return [row]
        elif sql == 'select sql from sqlite_master where name = :n and type="table"':
            try:
                table = self.h5file.get_node(params['n'])
                row = Row()
                row['sql'] = 'CREATE TABLE {} ()'.format(params['n'])
                return [row]
            except:
                return []
        else:
            raise Exception("SQLite queries cannot be executed with this connector")

class Row(list):
    def __init__(self, values=None):
        self.labels = []
        self.values = []
        if values:
            for idx in values:
                self.__setitem__(idx, values[idx])

    def __setitem__(self, idx, value):
        if type(idx) is str:
            if idx in self.labels:
                self.values[self.labels.index(idx)] = value
            else:
                self.labels.append(idx)
                self.values.append(value)
        else:
            self.values[idx] = value

    def __getitem__(self, idx):
        if type(idx) is str:
            return self.values[self.labels.index(idx)]
        else:
            return self.values[idx]

    def __iter__(self):
        return self.values.__iter__()
