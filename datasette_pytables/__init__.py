import tables
import datasette_connectors as dc
from .utils import parse_sql


class PyTablesConnection(dc.Connection):
    def __init__(self, path, connector):
        super().__init__(path, connector)
        self.h5file = tables.open_file(path)


class PyTablesConnector(dc.Connector):
    connector_type = 'pytables'
    connection_class = PyTablesConnection

    operators = {
        'eq': '==',
        'neq': '!=',
        'gt': '>',
        'gte': '>=',
        'lt': '<',
        'lte': '<=',
        'and': '&',
        'or': '|',
        'binary_and': '&',
        'binary_or': '|',
    }

    def _serialize_table_name(self, table_name):
        return table_name.replace('/', '%')

    def _deserialize_table_name(self, table_name):
        return table_name.replace('%', '/')

    def table_names(self):
        return [
            self._serialize_table_name(node._v_pathname)
            for node in self.conn.h5file
            if not(isinstance(node, tables.group.Group))
        ]

    def table_count(self, table_name):
        table = self.conn.h5file.get_node(self._deserialize_table_name(table_name))
        return int(table.nrows)

    def table_info(self, table_name):
        table = self.conn.h5file.get_node(self._deserialize_table_name(table_name))
        columns = [
            {
                'name': 'value',
                'type': table.dtype.name,
            }
        ]
        if isinstance(table, tables.table.Table):
            columns = [
                {
                    'name': colname,
                    'type': table.coltypes[colname],
                }
                for colname in table.colnames
            ]

        return [
            {
                'cid': cid,
                'name': column['name'],
                'type': column['type'],
                'notnull': True,
                'default_value': None,
                'is_pk': False,
            }
            for cid, column in enumerate(columns)
        ]

    def hidden_table_names(self):
        return []

    def detect_spatialite(self):
        return False

    def view_names(self):
        return []

    def detect_fts(self, table_name):
        return False

    def foreign_keys(self, table_name):
        return []

    def table_exists(self, table_name):
        try:
            self.conn.h5file.get_node(self._deserialize_table_name(table_name))
            return True
        except:
            return False

    def table_definition(self, table_type, table_name):
        table_name = self._deserialize_table_name(table_name)
        table = self.conn.h5file.get_node(table_name)
        colnames = ['value']
        if isinstance(table, tables.table.Table):
            colnames = table.colnames

        return 'CREATE TABLE {} ({})'.format(
            table_name,
            ', '.join(colnames),
        )

    def indices_definition(self, table_name):
        return []

    def execute(
        self,
        sql,
        params=None,
        truncate=False,
        custom_time_limit=None,
        page_size=None,
        log_sql_errors=True,
    ):
        results = []
        truncated = False
        description = ()

        # Some Datasette queries uses glob operand, not supported by Pytables
        if ' glob ' in sql:
            return results, truncated, description

        parsed_sql = parse_sql(sql, params)

        while isinstance(parsed_sql['from'], dict):
            # Pytables does not support subqueries
            parsed_sql['from'] = parsed_sql['from']['value']['from']

        table = self.conn.h5file.get_node(self._deserialize_table_name(parsed_sql['from']))
        table_rows = []
        fields = parsed_sql['select']
        colnames = ['value']
        if type(table) is tables.table.Table:
            colnames = table.colnames

        query = ''
        start = 0
        end = table.nrows

        def _get_field_type(field):
            coltype = table.dtype.name
            if type(table) is tables.table.Table:
                coltype = table.coltypes[field]
            return coltype

        # Use 'where' statement or get all the rows
        def _cast_param(field, pname):
            # Cast value to the column type
            coltype = _get_field_type(field)
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
                expr = " {} ".format(self.operators[operator]).join(subexpr)
            elif operator == 'exists':
                pass
            elif where == {'eq': ['rowid', 'p0']}:
                start = int(params['p0'])
                end = start + 1
            elif where == {'gt': ['rowid', 'p0']}:
                start = int(params['p0']) + 1
            else:
                left, right = where[operator]

                if isinstance(left, dict):
                    left = "(" + _translate_where(left) + ")"
                elif left in params:
                    _cast_param(right, left)

                if isinstance(right, dict):
                    right = "(" + _translate_where(right) + ")"
                elif right in params:
                    _cast_param(left, right)

                expr = "{left} {operator} {right}".format(
                    left=left,
                    operator=self.operators.get(operator, operator),
                    right=right,
                )

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

        # Offset
        offset = None
        if 'offset' in parsed_sql:
            offset = int(parsed_sql['offset'])

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

        # Get results
        get_rowid = make_get_rowid()
        get_row_value = make_get_row_value()
        if offset:
            table_rows = table_rows[offset:]
        count = 0
        for table_row in table_rows:
            count += 1
            if limit is not None and count > limit:
                break
            if truncate and max_returned_rows and count > max_returned_rows:
                truncated = True
                break
            row = {}
            for field in fields:
                field_name = field
                if isinstance(field, dict):
                    field_name = field['value']
                if isinstance(field_name, dict) and 'distinct' in field_name:
                    field_name = field_name['distinct']
                if field_name == 'rowid':
                    row['rowid'] = get_rowid(table_row)
                elif field_name == '*':
                    for col in colnames:
                        row[col] = normalize_field_value(get_row_value(table_row, col))
                elif isinstance(field_name, dict):
                    if field_name.get('count') == '*':
                        row['count(*)'] = int(table.nrows)
                    elif field_name.get('json_type'):
                        field_name = field_name.get('json_type')
                        row['json_type(' + field_name + ')'] = _get_field_type(field_name)
                    else:
                        raise Exception("Function not recognized")
                else:
                    row[field_name] = normalize_field_value(get_row_value(table_row, field_name))
            results.append(row)

        # Prepare query description
        for field in [f['value'] if isinstance(f, dict) else f for f in fields]:
            if field == '*':
                for col in colnames:
                    description += ((col,),)
            else:
                description += ((field,),)

        return results, truncated, description
