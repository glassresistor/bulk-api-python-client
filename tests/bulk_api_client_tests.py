import os
import pytest
import random
import string
import json
from unittest import mock
from pandas import DataFrame
from urllib.parse import urljoin
from requests.models import Response

from bulk_api_client import Client, AppAPI, ModelAPI, requests, CERT_PATH


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


def test_client_request():
    """Test Client method uses request library"""
    token = random_string()
    url = random_string()
    test_client = Client(token, api_url=url)
    method = 'GET'
    path = random_string()
    full_path = urljoin(url, path)
    params = {'teset_param': 1}
    headers = {'Authorization': 'Token {}'.format(token)}
    with mock.patch.object(requests, 'request') as fn:
        fn.return_value = ''
        test_client.request(method, path, params)
        fn.assert_called_with(
            method, full_path, params=params, headers=headers, verify=CERT_PATH)


def test_client_app_method():
    """Test Client class works as intented"""
    token = random_string()
    url = random_string()
    test_client = Client(token, api_url=url)
    test_app_name = 'test_model_name'
    with mock.patch.object(AppAPI, '__init__', return_value=None) as fn:
        test_app_obj = test_client.app(test_app_name)
        fn.assert_called_with(test_client, test_app_name)
    assert isinstance(test_app_obj, AppAPI)


def test_app_api_model():
    """Test AppAPI class works as intented"""
    token = random_string()
    url = random_string()
    test_client = Client(token, api_url=url)
    test_app = AppAPI(test_client, 'test_app_label')
    test_model_name = 'test_model_name'
    with mock.patch.object(ModelAPI, '__init__', return_value=None) as fn:
        test_model_obj = test_app.model(test_model_name)
        fn.assert_called_with(test_app, test_model_name)
    assert test_app.app_label == 'test_app_label'
    assert isinstance(test_model_obj, ModelAPI)


def test_model_api_query():
    """Test ModelAPI class works as intented"""
    token = random_string()
    url = random_string()
    test_client = Client(token, api_url=url)
    test_app = AppAPI(test_client, 'test_app_name')
    test_model_name = 'test_app_model'
    test_model = ModelAPI(test_app, test_model_name)
    assert test_model.model_name == test_model_name

    test_fields = ['id', 'text']
    test_order = 'text'
    test_filter = 'filter'
    test_page = 'page'
    test_page_size = 'page_size'
    response = Response()
    response._content = b'col1,col2\n1,2'
    path = 'bulk/pandas_views/test_app_name/test_app_model'
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


def test_model_api_query_null_params():
    """Test ModelAPI class works as intented"""
    token = random_string()
    url = random_string()
    test_client = Client(token, api_url=url)
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
