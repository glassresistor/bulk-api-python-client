import pytest
import random
import string
from unittest import mock
from urllib.parse import urljoin

from bulk_api_client import Client, AppAPI, requests


def random_string(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


def test_client():
    """Test Client class works as intented"""
    token = random_string()
    url = random_string()
    test_client = Client(token=token, api_url=url)
    assert test_client.token == token
    assert test_client.api_url == url


def test_client_request():
    """Test Client method uses request library"""
    token = random_string()
    url = random_string()
    test_client = Client(token=token, api_url=url)
    method = 'GET'
    path = random_string()
    full_path = urljoin(url, path)
    params = {'teset_param': 1}
    headers = {'Authorization': 'Token {}'.format(token)}
    with mock.patch.object(requests, 'request') as fn:
        test_client.request(method, path, params)
        fn.assert_called_with(
            method, full_path, params=params, headers=headers)


def test_client_app_method():
    """Test Client class works as intented"""
    token = random_string()
    url = random_string()
    test_client = Client(token=token, api_url=url)
    test_app_label_class = test_client.app('test_app_label')
    test_app_label = test_app_label_class('test_app_label')
    assert test_app_label.app_label == 'test_app_label'


def test_app_api():
    """Test AppAPI class works as intented"""
    token = random_string()
    url = random_string()
    test_app = AppAPI(token=token, api_url=url, app_label='test_app_label')
    test_model_name = 'test_model_name'
    test_model_obj = test_app.model(model_name=test_model_name)
    assert test_app.app_label == 'test_app_label'
    assert test_model_obj.model_name == test_model_name
