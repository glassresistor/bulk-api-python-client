import os
import pytest
import string
import random
import json
import yaml
from io import BytesIO
from unittest import mock
from urllib.parse import urljoin
from requests.models import Response

from bulk_api_client.client import Client, requests, CERT_PATH, requests_cache
from bulk_api_client.app import AppAPI
from bulk_api_client.model import is_kv
from bulk_api_client.exceptions import BulkAPIError

BASE_URL = "http://test.org/api/"


def random_string(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(stringLength))


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ("key=value", True),
        ("test=test", True),
        ("invalid", False),
        ("!@#$", False),
    ],
)
def test_is_kv(test_input, expected):
    """Test is_kv func returns correct bool for if pair is a key:val pair"""
    assert is_kv(test_input) == expected


def test_cert_path():
    assert os.path.isfile(CERT_PATH)


def test_client():
    """Test Client class works as intented"""
    token = random_string()
    url = random_string()
    yaml_data = {"definitions": ["some_definitions"], "paths": ["some_paths"]}
    data = BytesIO(yaml.dump(yaml_data).encode())
    response = Response()
    response._content = b""
    response.status_code = 200
    response.raw = data
    with mock.patch.object(requests, "request", return_value=response):
        test_client = Client(token, api_url=url)
    assert test_client.token == token
    assert test_client.api_url == url


def test_client_request(client):
    """Test Client method uses request library"""
    method = "GET"
    path = random_string()
    full_path = urljoin(client.api_url, path)
    params = {"teset_param": 1}
    kwargs = {"headers": {"Authorization": "Token {}".format(client.token)}}
    response = Response()
    response._content = b""
    response.status_code = 200
    with mock.patch.object(requests, "request", return_value=response) as fn:
        client.request(method, full_path, params)
        fn.assert_called_with(
            method=method,
            url=full_path,
            params=params,
            verify=CERT_PATH,
            stream=True,
            **kwargs,
        )


@pytest.mark.parametrize(
    "status_code,err_msg",
    [
        (401, {"detail": "You do not have permission to perform this action."}),
        (403, {"detail": "You do not have permission to perform this action."}),
        (404, {"detail": "Not found."}),
    ],
)
def test_client_request_errors(client, status_code, err_msg):
    """Test Client request method handles errors as intended"""
    method = "GET"
    path = random_string()
    full_path = urljoin(client.api_url, path)
    params = {"teset_param": 1}
    kwargs = {"headers": {"Authorization": "Token {}".format(client.token)}}
    response = Response()
    response._content = json.dumps(err_msg)
    response.status_code = status_code
    with mock.patch.object(requests, "request", return_value=response) as fn:
        with pytest.raises(BulkAPIError) as err:
            client.request(method, full_path, params)
        fn.assert_called_with(
            method=method,
            url=full_path,
            params=params,
            verify=CERT_PATH,
            stream=True,
            **kwargs,
        )
    assert str(err.value) == str(err_msg)


def test_download_swagger_yaml():
    token = random_string()
    url = random_string()
    yaml_data = {"definitions": ["some_definitions"], "paths": ["some_paths"]}
    data = BytesIO(yaml.dump(yaml_data).encode())
    method = "GET"
    url = urljoin(BASE_URL, "http://localhost:8000/bulk/api/swagger.yaml")
    params = {}
    kwargs = {"headers": {"Authorization": "Token {}".format(token)}}
    response = Response()
    response._content = b""
    response.status_code = 200
    response.raw = data
    with mock.patch.object(requests, "request", return_value=response) as fn:
        client = Client(token, api_url=url)
        fn.assert_called_with(
            method=method,
            url=url,
            params=params,
            verify=CERT_PATH,
            stream=True,
            **kwargs,
        )
    assert client.definitions == yaml_data["definitions"]
    assert client.paths == yaml_data["paths"]


def test_request_caching(client):
    method = "GET"
    path = random_string()
    full_path = urljoin(client.api_url, path)
    response = Response()
    response.status_code = 200
    response.from_cache = True
    with mock.patch.object(requests, "request", return_value=response):
        res = client.request(method, full_path, {})
    res.from_cache = True


def test_client_app_method(client):
    """Test Client class works as intented"""
    test_app_name = random_string()
    with mock.patch.object(AppAPI, "__init__", return_value=None) as fn:
        test_app_obj = client.app(test_app_name)
        fn.assert_called_with(client, test_app_name)
    assert isinstance(test_app_obj, AppAPI)


def test_client_clear_cache(client):
    """Test cache clearing method clears request cache"""
    with mock.patch.object(requests_cache, "clear") as fn:
        client.clear_cache()
        fn.called = True
