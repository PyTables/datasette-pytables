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
        colnames = ['value']
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
        colnames = ['value']
        if type(table) is tables.table.Table:
            colnames = table.colnames

        query = ''
        start = 0
        end = table.nrows

        # Use 'where' statement or get all the rows
        def _cast_param(field, pname):
            # Cast value to the column type
            coltype = table.dtype.name
            if type(table) is tables.table.Table:
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

        # Sort by column
        orderby = ''
        if 'orderby' in parsed_sql:
            orderby = parsed_sql['orderby']
            if type(orderby) is list:
                orderby = orderby[0]
            orderby = orderby['value']
            if orderby == 'rowid':
                orderby = ''

        # Limit number of rows
        limit = None
        if 'limit' in parsed_sql:
            limit = int(parsed_sql['limit'])

        # Truncate if needed
        if page_size and max_returned_rows and truncate:
            if max_returned_rows == page_size:
                max_returned_rows += 1

        # Execute query
        if query:
            table_rows = table.where(query, params, start, end)
        elif orderby:
            table_rows = table.itersorted(orderby, start=start, stop=end)
        else:
            table_rows = table.iterrows(start, end)

        # Prepare rows
        def normalize_field_value(value):
            if type(value) is bytes:
                return value.decode('utf-8')
            elif not type(value) in (int, float, complex):
                return str(value)
            else:
                return value

        def make_get_rowid():
            if type(table) is tables.table.Table:
                def get_rowid(row):
                    return int(row.nrow)
            else:
                rowid = start - 1
                def get_rowid(row):
                    nonlocal rowid
                    rowid += 1
                    return rowid
            return get_rowid

        def make_get_row_value():
            if type(table) is tables.table.Table:
                def get_row_value(row, field):
                    return row[field]
            else:
                def get_row_value(row, field):
                    return row
            return get_row_value

        if len(fields) == 1 and type(fields[0]['value']) is dict and \
           fields[0]['value'].get('count') == '*':
            rows.append(Row({'count(*)': int(table.nrows)}))
        else:
            get_rowid = make_get_rowid()
            get_row_value = make_get_row_value()
            count = 0
            for table_row in table_rows:
                count += 1
                if limit and count > limit:
                    break
                if truncate and max_returned_rows and count > max_returned_rows:
                    truncated = True
                    break
                row = Row()
                for field in fields:
                    field_name = field['value']
                    if type(field_name) is dict and 'distinct' in field_name:
                        field_name = field_name['distinct']
                    if field_name == 'rowid':
                        row['rowid'] = get_rowid(table_row)
                    elif field_name == '*':
                        for col in colnames:
                            row[col] = normalize_field_value(get_row_value(table_row, col))
                    else:
                        row[field_name] = normalize_field_value(get_row_value(table_row, field_name))
                rows.append(row)

        # Prepare query description
        for field in [f['value'] for f in fields]:
            if field == '*':
                for col in colnames:
                    description.append((col,))
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
                colnames = ['value']
                if type(table) is tables.table.Table:
                    colnames = table.colnames
                row = Row()
                row['sql'] = 'CREATE TABLE {} ({})'.format(params['n'], ", ".join(colnames))
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
