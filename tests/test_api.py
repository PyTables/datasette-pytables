from .fixtures import app_client
import pytest

pytest.fixture(scope='module')(app_client)

def test_homepage(app_client):
    _, response = app_client.get('/.json')
    assert response.status == 200
    assert response.json.keys() == {'test_tables': 0}.keys()
    d = response.json['test_tables']
    assert d['name'] == 'test_tables'
    assert d['tables_count'] == 4

def test_database_page(app_client):
    response = app_client.get('/test_tables.json', gather_request=False)
    data = response.json
    assert 'test_tables' == data['database']
    assert [{
        'name': '/array1',
        'columns': [],
        'primary_keys': [],
        'count': 2,
        'label_column': None,
        'hidden': False,
        'fts_table': None,
        'foreign_keys': {'incoming': [], 'outgoing': []}
    }, {
        'name': '/group1/array2',
        'columns': [],
        'primary_keys': [],
        'count': 10000,
        'label_column': None,
        'hidden': False,
        'fts_table': None,
        'foreign_keys': {'incoming': [], 'outgoing': []}
    }, {
        'name': '/group1/table1',
        'columns': ['identity', 'idnumber', 'speed'],
        'primary_keys': [],
        'count': 10000,
        'label_column': None,
        'hidden': False,
        'fts_table': None,
        'foreign_keys': {'incoming': [], 'outgoing': []}
    }, {
        'name': '/group2/table2',
        'columns': ['identity', 'idnumber', 'speed'],
        'primary_keys': [],
        'count': 10000,
        'label_column': None,
        'hidden': False,
        'fts_table': None,
        'foreign_keys': {'incoming': [], 'outgoing': []}
    }] == data['tables']

def test_custom_sql(app_client):
    response = app_client.get(
        '/test_tables.json?sql=select+identity+from+[/group1/table1]&_shape=objects',
        gather_request=False
    )
    data = response.json
    assert {
        'sql': 'select identity from [/group1/table1]',
        'params': {}
    } == data['query']
    assert 50 == len(data['rows'])
    assert [
        {'identity': 'This is particle:  0'},
        {'identity': 'This is particle:  1'},
        {'identity': 'This is particle:  2'}
    ] == data['rows'][:3]
    assert ['identity'] == data['columns']
    assert 'test_tables' == data['database']
    assert data['truncated']

def test_invalid_custom_sql(app_client):
    response = app_client.get(
        '/test_tables.json?sql=.schema',
        gather_request=False
    )
    assert response.status == 400
    assert response.json['ok'] is False
    assert 'Statement must be a SELECT' == response.json['error']

def test_table_json(app_client):
    response = app_client.get(
        '/test_tables/%2Fgroup2%2Ftable2.json?_shape=objects',
        gather_request=False
    )
    assert response.status == 200
    data = response.json
    assert data['query']['sql'] == 'select rowid, * from [/group2/table2] order by rowid limit 51'
    assert data['rows'][3:6] == [{
        'rowid': 3,
        'identity': 'This is particle:  3',
        'idnumber': 3,
        'speed': 6.0
    }, {
        'rowid': 4,
        'identity': 'This is particle:  4',
        'idnumber': 4,
        'speed': 8.0
    }, {
        'rowid': 5,
        'identity': 'This is particle:  5',
        'idnumber': 5,
        'speed': 10.0
    }]

def test_table_not_exists_json(app_client):
    assert {
        'ok': False,
        'error': 'Table not found: blah',
        'status': 404,
        'title': None,
    } == app_client.get(
        '/test_tables/blah.json', gather_request=False
    ).json

def test_table_shape_arrays(app_client):
    response = app_client.get(
        '/test_tables/%2Fgroup2%2Ftable2.json?_shape=arrays',
        gather_request=False
    )
    assert [
        [6, 'This is particle:  6', 6, 12.0],
        [7, 'This is particle:  7', 7, 14.0],
    ] == response.json['rows'][6:8]

def test_table_shape_objects(app_client):
    response = app_client.get(
        '/test_tables/%2Fgroup2%2Ftable2.json?_shape=objects',
        gather_request=False
    )
    assert [{
        'rowid': 6,
        'identity': 'This is particle:  6',
        'idnumber': 6,
        'speed': 12.0,
    }, {
        'rowid': 7,
        'identity': 'This is particle:  7',
        'idnumber': 7,
        'speed': 14.0,
    }] == response.json['rows'][6:8]

def test_table_shape_array(app_client):
    response = app_client.get(
        '/test_tables/%2Fgroup2%2Ftable2.json?_shape=array',
        gather_request=False
    )
    assert [{
        'rowid': 6,
        'identity': 'This is particle:  6',
        'idnumber': 6,
        'speed': 12.0,
    }, {
        'rowid': 7,
        'identity': 'This is particle:  7',
        'idnumber': 7,
        'speed': 14.0,
    }] == response.json[6:8]

def test_table_shape_invalid(app_client):
    response = app_client.get(
        '/test_tables/%2Fgroup2%2Ftable2.json?_shape=invalid',
        gather_request=False
    )
    assert {
        'ok': False,
        'error': 'Invalid _shape: invalid',
        'status': 400,
        'title': None,
    } == response.json
