import os
import pytest
import random
import string
import json
from unittest import mock
from pandas import DataFrame
from urllib.parse import urljoin
from requests.models import Response

from bulk_api_client import Client, AppAPI, ModelAPI
from bulk_api_client import requests, CERT_PATH, BulkAPIError


def random_string(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


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
    with mock.patch.object(requests, 'request') as fn:
        fn.return_value = ''
        client.request(method, path, params)
        fn.assert_called_with(
            method, full_path, params=params, headers=headers, verify=CERT_PATH)


def test_client_app_method(client):
    """Test Client class works as intented"""
    test_app_name = random_string()
    with mock.patch.object(AppAPI, '__init__', return_value=None) as fn:
        test_app_obj = client.app(test_app_name)
        fn.assert_called_with(client, test_app_name)
    assert isinstance(test_app_obj, AppAPI)


def test_app_api_model(client):
    """Test AppAPI class works as intented"""
    app_label = random_string()
    data = {
        'app_label_1': "rgrg",
        'app_label_2': "rgrg",
        'app_label_3': "rgrg"
    }
    response = Response()
    response._content = json.dumps(data)
    response.status_code = 200
    with mock.patch.object(Client, 'request') as fn:
        fn.return_value = response
        test_app = AppAPI(client, 'app_label_1')
    test_model_name = 'test_model_name'
    response = Response()
    response._content = b'content'
    with mock.patch.object(ModelAPI, '__init__', return_value=None) as fn:
        test_model_obj = test_app.model(test_model_name)
        fn.assert_called_with(test_app, test_model_name)
    assert test_app.app_label == 'app_label_1'
    assert isinstance(test_model_obj, ModelAPI)


def test_app_api_invalid_app(client):
    data = {
        'app_label_1': "rgrg",
        'app_label_2': "rgrg",
        'app_label_3': "rgrg"
    }
    response = Response()
    response._content = json.dumps(data)
    response.status_code = 200
    with mock.patch.object(Client, 'request') as fn:
        with pytest.raises(BulkAPIError) as e:
            fn.return_value = response
            AppAPI(client, 'invalid_label')


def test_model_api_query(app_api):
    """Test ModelAPI class works as intented"""
    test_model_name = 'test_app_model'
    test_model = ModelAPI(app_api, test_model_name)
    assert test_model.model_name == test_model_name

    test_fields = ['id', 'text']
    test_order = 'text'
    test_filter = 'filter'
    test_page = 'page'
    test_page_size = 'page_size'

    response = Response()
    response._content = b'col1,col2\n1,2'
    path = 'bulk/pandas_views/{}/{}'.format(
        app_api.app_label, test_model_name)
    params = {'fields': ','.join(test_fields), 'filter': test_filter,
              'ordering': test_order, 'page': test_page,
              'page_size': test_page_size}

    with mock.patch.object(Client, 'request') as fn:
        fn.return_value = response
        test_model_data_frame = test_model.query(
            fields=test_fields, filter=test_filter, order=test_order,
            page=test_page, page_size=test_page_size)
        fn.assert_called_with('GET', path, params=params)
    assert isinstance(test_model_data_frame, DataFrame)


def test_model_api_query_null_params(app_api):
    """Test ModelAPI class works as intented"""
    token = random_string()
    url = "http://{}".format(random_string())
    test_client = Client(token, api_url=url)
    data = {
        'app_label_1': "test",
        'app_label_2': "test",
        'app_label_3': "test",
        'test_app_name': "test"
    }
    response = Response()
    response._content = json.dumps(data)
    response.status_code = 200
    with mock.patch.object(Client, 'request') as fn:
        fn.return_value = response
        test_app = AppAPI(test_client, 'test_app_name')
    test_model_name = 'test_app_model'
    test_model = ModelAPI(test_app, test_model_name)
    assert test_model.model_name == test_model_name

    response = Response()
    response._content = b'col1,col2\n1,2'
    path = 'bulk/pandas_views/test_app_name/test_app_model'
    params = {'fields': None, 'filter': None,
              'ordering': None, 'page': None,
              'page_size': None}
    with mock.patch.object(Client, 'request') as fn:
        fn.return_value = response
        test_model_data_frame = test_model.query()
        fn.assert_called_with('GET', path, params=params)
    assert isinstance(test_model_data_frame, DataFrame)
