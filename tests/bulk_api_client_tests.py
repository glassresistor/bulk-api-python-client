import pytest
import random
import string
from unittest import mock
from urllib.parse import urljoin

from bulk_api_client import Client, requests


def random_string(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


def test_client():
    token = random_string()
    url = random_string()
    test_client = Client(token, api_url=url)
    assert test_client.token == token
    assert test_client.api_url == url


def test_client_request():
    token = random_string()
    url = random_string()
    test_client = Client(token, api_url=url)
    method = 'GET'
    path = random_string()
    full_path = urljoin(url, path)
    params = {'teset_param': 1}
    headers = {'Authorization': 'Token {}'.format(token)}
    with mock.patch.object(requests, 'request') as fn:
        test_client.request(method, path, params)
        fn.assert_called_with(
            method, full_path, params=params, headers=headers)
