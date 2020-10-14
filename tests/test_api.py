from .fixtures import app_client
import pytest
from urllib.parse import urlencode

def test_homepage(app_client):
    response = app_client.get('/.json')
    assert response.status == 200
    assert response.json.keys() == {'test_tables': 0}.keys()
    d = response.json['test_tables']
    assert d['name'] == 'test_tables'
    assert d['tables_count'] == 5

def test_database_page(app_client):
    response = app_client.get('/test_tables.json')
    data = response.json
    assert 'test_tables' == data['database']
    assert [{
        'name': '/array1',
        'columns': ['value'],
        'primary_keys': [],
        'count': 2,
        'hidden': False,
        'fts_table': None,
        'foreign_keys': {'incoming': [], 'outgoing': []},
        'private': False,
    }, {
        'name': '/group1/array2',
        'columns': ['value'],
        'primary_keys': [],
        'count': 10000,
        'hidden': False,
        'fts_table': None,
        'foreign_keys': {'incoming': [], 'outgoing': []},
        'private': False,
    }, {
        'name': '/group1/table1',
        'columns': ['identity', 'idnumber', 'speed'],
        'primary_keys': [],
        'count': 10000,
        'hidden': False,
        'fts_table': None,
        'foreign_keys': {'incoming': [], 'outgoing': []},
        'private': False,
    }, {
        'name': '/group2/multi',
        'columns': ['value'],
        'primary_keys': [],
        'count': 10,
        'hidden': False,
        'fts_table': None,
        'foreign_keys': {'incoming': [], 'outgoing': []},
        'private': False,
    }, {
        'name': '/group2/table2',
        'columns': ['identity', 'idnumber', 'speed'],
        'primary_keys': [],
        'count': 10000,
        'hidden': False,
        'fts_table': None,
        'foreign_keys': {'incoming': [], 'outgoing': []},
        'private': False,
    }] == data['tables']

def test_custom_sql(app_client):
    response = app_client.get(
        '/test_tables.json?' + urlencode({
            'sql': 'select identity from [/group1/table1]',
            '_shape': 'objects'
        }),
        gather_request=False
    )
    data = response.json
    assert {
        'sql': 'select identity from [/group1/table1]',
        'params': {}
    } == data['query']
    assert 1000 == len(data['rows'])
    assert [
        {'identity': 'This is particle:  0'},
        {'identity': 'This is particle:  1'},
        {'identity': 'This is particle:  2'},
        {'identity': 'This is particle:  3'}
    ] == data['rows'][:4]
    assert ['identity'] == data['columns']
    assert 'test_tables' == data['database']
    assert data['truncated']

def test_custom_complex_sql(app_client):
    response = app_client.get(
        '/test_tables.json?' + urlencode({
            'sql': 'select identity from [/group1/table1] where speed > 100 and idnumber < 55',
            '_shape': 'objects'
        }),
        gather_request=False
    )
    data = response.json
    assert {
        'sql': 'select identity from [/group1/table1] where speed > 100 and idnumber < 55',
        'params': {}
    } == data['query']
    assert 4 == len(data['rows'])
    assert [
        {'identity': 'This is particle: 51'},
        {'identity': 'This is particle: 52'},
        {'identity': 'This is particle: 53'},
        {'identity': 'This is particle: 54'}
    ] == data['rows']
    assert ['identity'] == data['columns']
    assert 'test_tables' == data['database']
    assert not data['truncated']

def test_custom_pytables_sql(app_client):
    response = app_client.get(
        '/test_tables.json?' + urlencode({
            'sql': 'select identity from [/group1/table1] where (speed > 100) & (speed < 500)',
            '_shape': 'objects'
            }),
        gather_request=False
    )
    data = response.json
    assert {
        'sql': 'select identity from [/group1/table1] where (speed > 100) & (speed < 500)',
        'params': {}
    } == data['query']
    assert 199 == len(data['rows'])
    assert [
        {'identity': 'This is particle: 51'},
        {'identity': 'This is particle: 52'},
        {'identity': 'This is particle: 53'}
    ] == data['rows'][:3]
    assert ['identity'] == data['columns']
    assert 'test_tables' == data['database']
    assert not data['truncated']

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

@pytest.mark.parametrize('path, expected_rows, expected_pages', [
    ('/test_tables/%2Farray1.json', 2, 1),
    ('/test_tables/%2Farray1.json?_size=1', 2, 2),
    ('/test_tables/%2Fgroup1%2Farray2.json?_size=1000', 10000, 10),
    ('/test_tables/%2Fgroup2%2Fmulti.json?_size=5', 10, 2),
])
def test_paginate_tables_and_arrays(app_client, path, expected_rows, expected_pages):
    fetched = []
    count = 0
    while path:
        response = app_client.get(path, gather_request=False)
        print("*****", response.json)
        assert 200 == response.status
        count += 1
        fetched.extend(response.json['rows'])
        path = response.json['next_url']
        if path:
            assert response.json['next']
            assert '_next={}'.format(response.json['next']) in path

    assert expected_rows == len(fetched)
    assert expected_pages == count
