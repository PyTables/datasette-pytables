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
