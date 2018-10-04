from .fixtures import app_client

def test_homepage(app_client):
    response = app_client.get('/', gather_request=False)
    assert response.status == 200
    assert 'test_tables' in response.text

def test_database_page(app_client):
    response = app_client.get('/test_tables', allow_redirects=False, gather_request=False)
    assert response.status == 302
    response = app_client.get('/test_tables', gather_request=False)
    assert 'test_tables' in response.text

def test_table(app_client):
    response = app_client.get('/test_tables/%2Fgroup1%2Ftable1', gather_request=False)
    assert response.status == 200

def test_table_row(app_client):
    response = app_client.get('/test_tables/%2Fgroup1%2Ftable1/50', gather_request=False)
    assert response.status == 200

def test_array(app_client):
    response = app_client.get('/test_tables/%2Fgroup1%2Farray2', gather_request=False)
    assert response.status == 200

def test_array_row(app_client):
    response = app_client.get('/test_tables/%2Fgroup1%2Farray2/1050', gather_request=False)
    assert response.status == 200
