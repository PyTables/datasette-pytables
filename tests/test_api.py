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
    assert [
        {'identity': 'This is particle:  0'},
        {'identity': 'This is particle:  1'},
        {'identity': 'This is particle:  2'}
    ] == data['rows'][:3]
    assert ['identity'] == data['columns']
    assert 'test_tables' == data['database']
    assert data['truncated']
