import os
import pytest
import random
import string
import json
from io import BytesIO
from unittest import mock
from pandas import DataFrame
from urllib.parse import urljoin
from requests.models import Response

from bulk_api_client import Client, AppAPI, ModelAPI
from bulk_api_client import requests, CERT_PATH, BulkAPIError, is_kv


def random_string(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


@pytest.mark.parametrize("test_input,expected",
                         [("key=value", True), ("test=test", True),
                          ("invalid", False), ("!@#$", False)])
def test_is_kv(test_input, expected):
    """Test is_kv func returns correct bool for if pair is a key:val pair"""
    assert is_kv(test_input) == expected


def test_cert_path():
    assert os.path.isfile(CERT_PATH)


def test_client():
    """Test Client class works as intented"""
    token = random_string()
    url = random_string()
    test_client = Client(token, api_url=url)
    assert test_client.token == token
    assert test_client.api_url == url


def test_client_request(client):
    """Test Client method uses request library"""
    method = 'GET'
    path = random_string()
    full_path = urljoin(client.api_url, path)
    params = {'teset_param': 1}
    headers = {'Authorization': 'Token {}'.format(client.token)}
    response = Response()
    response._content = b''
    response.status_code = 200
    with mock.patch.object(requests, 'request', return_value=response) as fn:
        client.request(method, full_path, params)
        fn.assert_called_with(
            method,
            full_path,
            params=params,
            headers=headers,
            verify=CERT_PATH,
            stream=True
        )


@pytest.mark.django_db
@pytest.mark.parametrize("status_code,err_msg", [
    (401, {"detail":
           "You do not have permission to perform this action."}),
    (403, {"detail":
           "You do not have permission to perform this action."}),
    (404, {"detail":
           "Not found."}), ])
def test_client_request_errors(client, status_code, err_msg):
    """Test Client request method handles errors as intended"""
    method = 'GET'
    path = random_string()
    full_path = urljoin(client.api_url, path)
    params = {'teset_param': 1}
    headers = {'Authorization': 'Token {}'.format(client.token)}
    response = Response()
    response._content = json.dumps(err_msg)
    response.status_code = status_code
    with mock.patch.object(requests, 'request', return_value=response) as fn:
        with pytest.raises(BulkAPIError) as err:
            client.request(method, full_path, params)
        fn.assert_called_with(
            method,
            full_path,
            params=params,
            headers=headers,
            verify=CERT_PATH,
            stream=True
        )
    assert str(err.value) == str(err_msg)


def test_client_app_method(client):
    """Test Client class works as intented"""
    test_app_name = random_string()
    with mock.patch.object(AppAPI, '__init__', return_value=None) as fn:
        test_app_obj = client.app(test_app_name)
        fn.assert_called_with(client, test_app_name)
    assert isinstance(test_app_obj, AppAPI)


def test_client_clear_cache(client):
    """Test cache clearing method empties the temp dir"""
    with open(os.path.join(client.temp_dir, "test.txt"), 'wb') as f:
        f.write(b'test')
    client.clear_cache()

    assert not os.listdir(client.temp_dir)


def test_app_api(client):
    """Test AppAPI class works as intented"""
    test_app_label = random_string()
    url = client.api_url
    params = {}
    data = {
        'app_label_1': "rgrg",
        'app_label_2': "rgrg",
        'app_label_3': "rgrg",
        test_app_label: url
    }

    response = Response()
    response._content = json.dumps(data)
    response.status_code = 200

    with mock.patch.object(Client, 'request', return_value=response) as fn:
        test_app_obj = AppAPI(client, test_app_label)
    fn.assert_called_with('GET', url, params)
    assert test_app_obj.app_label == test_app_label


def test_app_api_invalid_app(client):
    data = {
        'app_label_1': "rgrg",
        'app_label_2': "rgrg",
        'app_label_3': "rgrg"
    }
    response = Response()
    response._content = json.dumps(data)
    response.status_code = 200
    with mock.patch.object(Client, 'request', return_value=response):
        with pytest.raises(BulkAPIError):
            AppAPI(client, 'invalid_label')


def test_app_api_model(client):
    """Test AppAPI class works as intented"""
    app_label = random_string()
    data = {
        'app_label_1': "rgrg",
        'app_label_2': "rgrg",
        'app_label_3': "rgrg",
        app_label: "test"
    }
    response = Response()
    response._content = json.dumps(data)
    response.status_code = 200
    with mock.patch.object(Client, 'request', return_value=response):
        test_app = AppAPI(client, app_label)

    test_model_name = 'test_model_name'
    with mock.patch.object(ModelAPI, '__init__', return_value=None) as fn:
        test_model_obj = test_app.model(test_model_name)
        fn.assert_called_with(test_app, test_model_name)
    assert test_app.app_label == app_label
    assert isinstance(test_model_obj, ModelAPI)


def test_model_api_query(app_api):
    """Test ModelAPI class works as intented"""
    test_model_name = random_string()
    path = 'bulk/pandas_views/{}/{}'.format(
        app_api.app_label, test_model_name)
    url = urljoin(app_api.client.api_url, path)

    test_fields = ['id', 'text']
    test_filter = 'key=value|key=value&key=value'
    test_order = 'text'
    test_page = 1
    test_page_size = 1

    data = {
        'model_name_1': "rgrg",
        'model_name_2': "rgrg",
        'model_name_3': "rgrg",
        test_model_name: url,
    }

    response = Response()
    response._content = json.dumps(data)
    response.status_code = 200
    params = {
        'fields': ','.join(test_fields),
        'filter': test_filter,
        'ordering': test_order,
        'page': test_page,
        'page_size': test_page_size
    }
    with mock.patch.object(Client, 'request', return_value=response):
        test_model = ModelAPI(app_api, test_model_name)

    response = Response()
    response._content = b'col1,col2\n1,2'
    response.status_code = 200
    response.raw = BytesIO(b'col1,col2\n1,2')
    with mock.patch.object(Client, 'request', return_value=response) as fn:
        test_model_data_frame = test_model.query(
            fields=test_fields, filter=test_filter, order=test_order,
            page=test_page, page_size=test_page_size)
        fn.assert_called_with('GET', url, params=params)
    assert test_model.model_name == test_model_name
    assert isinstance(test_model_data_frame, DataFrame)


def test_model_api_query_null_params(app_api):
    """Test ModelAPI class works as intented"""
    test_model_name = random_string()
    path = 'bulk/pandas_views/{}/{}'.format(
        app_api.app_label, test_model_name)
    url = urljoin(app_api.client.api_url, path)

    data = {
        'model_name_1': "rgrg",
        'model_name_2': "rgrg",
        'model_name_3': "rgrg",
        test_model_name: url,
    }

    model_response = Response()
    model_response._content = json.dumps(data)
    model_response.status_code = 200

    params = {
        'fields': None,
        'filter': None,
        'ordering': None,
        'page': None,
        'page_size': None,
    }
    with mock.patch.object(Client, 'request', return_value=model_response):
        test_model = ModelAPI(app_api, test_model_name)

    query_response = Response()
    query_response.status_code = 200
    query_response._content = b'col1,col2\n1,2'
    query_response.raw = BytesIO(b'col1,col2\n1,2')
    with mock.patch.object(Client, 'request',
                           return_value=query_response) as fn:
        test_model_data_frame = test_model.query()
        fn.assert_called_with('GET', url, params=params)
    assert test_model.model_name == test_model_name
    assert isinstance(test_model_data_frame, DataFrame)


@pytest.mark.parametrize("kwarg,val,msg", [
    ("fields", "invalid_field", {
        'detail': "fields arguement must be list"}),
    ("filter", 1, {
        'detail': "filter must be a string of form field_name=value"}),
    ("filter", "invalid", {
        'detail': "filter must be a string of form field_name=value"}),
    ("order", 1, {
        'detail': "order must be a string"}),
    ("page", "invalid_page", {
        'detail': "page must be a positive integer"}),
    ("page", 0, {
        'detail': "page must be a positive integer"}),
    ("page", -1, {
        'detail': "page must be a positive integer"}),
    ("page_size", "invalid_page_size", {
     'detail': "page size must be a positive integer"}),
    ("page_size", 0, {
        'detail': "page size must be a positive integer"}),
    ("page_size", -1, {
        'detail': "page size must be a positive integer"})])
def test_model_api_query_invalid_params(app_api, kwarg, val, msg):
    """Test ModelAPI class works as intented"""

    test_model_name = random_string()

    data = {
        'model_name_1': "rgrg",
        'model_name_2': "rgrg",
        'model_name_3': "rgrg",
        test_model_name: "test",
    }

    response = Response()
    response._content = json.dumps(data)
    response.status_code = 200
    path = 'bulk/pandas_views/{}/{}'.format(
        app_api.app_label, test_model_name)
    params = {
        kwarg: val,
    }
    with mock.patch.object(Client, 'request', return_value=response):
        test_model = ModelAPI(app_api, test_model_name)

    response._content = b'col1,col2\n1,2'
    with mock.patch.object(Client, 'request', return_value=response):
        with pytest.raises(TypeError) as err:
            test_model_data_frame = test_model.query(**params)
    assert str(err.value) == str(msg)


def test_model_api_invalid_model(app_api):
    data = {
        'model_name_1': "rgrg",
        'model_name_2': "rgrg",
        'model_name_3': "rgrg",
    }
    response = Response()
    response._content = json.dumps(data)
    response.status_code = 200
    with mock.patch.object(Client, 'request', return_value=response):
        with pytest.raises(BulkAPIError):
            ModelAPI(app_api, 'invalid_model')
