import pytest
import string
import random
import json
from unittest import mock
from requests.models import Response

from bulk_api_client.client import Client
from bulk_api_client.app import AppAPI
from bulk_api_client.model import ModelAPI
from bulk_api_client.exceptions import BulkAPIError


def random_string(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(stringLength))


def test_app_api(client):
    """Test AppAPI class works as intented"""
    test_app_label = random_string()
    url = client.api_url
    params = {}
    data = {
        "app_label_1": "rgrg",
        "app_label_2": "rgrg",
        "app_label_3": "rgrg",
        test_app_label: url,
    }

    response = Response()
    response._content = json.dumps(data)
    response.status_code = 200

    with mock.patch.object(Client, "request", return_value=response) as fn:
        test_app_obj = AppAPI(client, test_app_label)
    fn.assert_called_with("GET", url, params)
    assert test_app_obj.app_label == test_app_label


def test_app_api_invalid_app(client):
    data = {"app_label_1": "rgrg", "app_label_2": "rgrg", "app_label_3": "rgrg"}
    response = Response()
    response._content = json.dumps(data)
    response.status_code = 200
    with mock.patch.object(Client, "request", return_value=response):
        with pytest.raises(BulkAPIError):
            AppAPI(client, "invalid_label")


def test_app_api_model_method(app_api):
    """Test AppAPI class works as intented"""

    test_model_name = "test_model_name"
    with mock.patch.object(ModelAPI, "__init__", return_value=None) as fn:
        test_model_obj = app_api.model(test_model_name)
        fn.assert_called_with(app_api, test_model_name)
    assert isinstance(test_model_obj, ModelAPI)


def test_app_api_str_method(app_api):
    assert str(app_api) == "AppAPI: {}".format(app_api.app_label)
