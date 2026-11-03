from fastapi.testclient import TestClient

from main import UTF8JSONResponse, app


def test_json_response_declares_utf8_charset() -> None:
    assert UTF8JSONResponse.media_type == 'application/json; charset=utf-8'


def test_openapi_json_content_type_includes_utf8_charset() -> None:
    with TestClient(app) as client:
        response = client.get('/movies/openapi.json')

    assert response.status_code == 200
    assert 'charset=utf-8' in response.headers['content-type'].lower()
    assert 'Информация о фильмах' in response.json()['info']['description']


def test_swagger_ui_is_available() -> None:
    with TestClient(app) as client:
        docs = client.get('/movies/docs')
        schema = client.get('/movies/openapi.json')

    assert docs.status_code == 200
    assert 'swagger-ui' in docs.text
    assert schema.status_code == 200
    paths = schema.json()['paths']
    assert '/api/v1/films' in paths
    assert '/api/v1/films/search/' in paths
    assert '/api/v1/films/{film_id}/' in paths
