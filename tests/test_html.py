from .fixtures import app_client, app_client_with_hash

def test_homepage(app_client):
    response = app_client.get('/')
    assert response.status == 200
    assert 'test_tables' in response.text

def test_database_page(app_client_with_hash):
    response = app_client_with_hash.get('/test_tables', allow_redirects=False)
    assert response.status == 302
    response = app_client_with_hash.get('/test_tables')
    assert 'test_tables' in response.text

def test_table(app_client):
    response = app_client.get('/test_tables/%2Fgroup1%2Ftable1')
    assert response.status == 200

def test_table_row(app_client):
    response = app_client.get('/test_tables/%2Fgroup1%2Ftable1/50')
    assert response.status == 200

def test_array(app_client):
    response = app_client.get('/test_tables/%2Fgroup1%2Farray2')
    assert response.status == 200

def test_array_row(app_client):
    response = app_client.get('/test_tables/%2Fgroup1%2Farray2/1050')
    assert response.status == 200
