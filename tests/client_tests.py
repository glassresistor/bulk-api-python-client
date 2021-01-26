import os
import pytest
import string
import random
import json
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


@pytest.mark.parametrize(
    "api_url,expiration_time,expected",
    [
        (None, None, ("https://data-warehouse.pivot/bulk/api/", 7200)),
        ("test", 1, ("test", 1)),
    ],
)
def test_client(api_url, expiration_time, expected):
    """Test Client class works as intented"""
    token = random_string()
    json_data = {"definitions": ["some_definitions"], "paths": ["some_paths"]}
    data = json.dumps(json_data)
    response = Response()
    response._content = data
    response.status_code = 200
    with mock.patch.object(requests, "request", return_value=response):
        test_client = Client(
            token, api_url=api_url, expiration_time=expiration_time
        )
    assert test_client.token == token
    assert test_client.api_url == expected[0]
    assert test_client.expiration_time == expected[1]


def test_client_request(client):
    """Test Client method uses request library"""
    method = "GET"
    path = random_string()
    full_path = urljoin(client.api_url, path)
    params = {"teset_param": 1}
    kwargs = {"headers": {"Authorization": "Token {}".format(client.token)}}
    response = Response()
    response._content = ""
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
    "status_code,err_vars",
    [
        (401, "detail: You do not have permission to perform this action."),
        (403, "detail: You do not have permission to perform this action."),
        (404, "detail: Not found."),
    ],
)
def test_client_request_errors(client, status_code, err_vars):
    """Test Client request method handles errors as intended"""
    method = "GET"
    path = random_string()
    full_path = urljoin(client.api_url, path)
    params = {"teset_param": 1}
    kwargs = {"headers": {"Authorization": "Token {}".format(client.token)}}
    err_msg = (
        "{} Error raised — something went wrong.\nPlease send this "
        "message to data-services+api-error@pivotbio.com, including "
        "the link below:\n\n{}\nIf you are curious as the the nature of"
        " the problem following the above link might provide some "
        "help.".format(status_code, full_path)
    )
    response = Response()
    response._content = err_msg
    response.status_code = status_code
    response.url = full_path
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

    assert str(err.value) == err_msg


@pytest.mark.parametrize(
    "status_code,err_msg",
    [
        (
            401,
            '{"detail": "You do not have permission to perform this action."}',
        ),
        (
            403,
            '{"detail": "You do not have permission to perform this action."}',
        ),
        (404, '{"detail": "Not found."}'),
    ],
)
def test_client_request_json_errors(client, status_code, err_msg):
    """Test Client request method handles errors as intended"""
    method = "GET"
    path = random_string()
    full_path = urljoin(client.api_url, path)
    params = {"teset_param": 1}
    kwargs = {"headers": {"Authorization": "Token {}".format(client.token)}}
    response = Response()
    response._content = err_msg
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


def test_download_swagger_json():
    token = random_string()
    url = random_string()
    json_data = {"definitions": ["some_definitions"], "paths": ["some_paths"]}
    data = json.dumps(json_data)
    method = "GET"
    url = urljoin(BASE_URL, "http://localhost:8000/bulk/api/swagger.json")
    params = {}
    kwargs = {"headers": {"Authorization": "Token {}".format(token)}}
    response = Response()
    response._content = data
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
    assert client.swagger_data["definitions"] == json_data["definitions"]
    assert client.swagger_data["paths"] == json_data["paths"]


def test_request_caching(client):
    method = "GET"
    path = random_string()
    full_path = urljoin(client.api_url, path)
    response = Response()
    response.status_code = 200
    response.from_cache = True
    with mock.patch.object(requests, "request", return_value=response):
        res = client.request(method, full_path, {})
    assert res.from_cache


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
        fn.assert_called()
