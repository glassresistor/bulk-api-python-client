import pytest
import random
import string
import json
import imp
import os
from io import BytesIO
from contextlib import contextmanager
from unittest import mock
from urllib.parse import urljoin
from requests.models import Response

from bulk_api_client.client import Client, requests
from bulk_api_client.app import AppAPI
from bulk_api_client.model import ModelAPI, ModelObj


def random_string(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_letters
    return "".join(random.choice(letters) for i in range(stringLength))


@contextmanager
def setenv(**mapping):
    """``with`` context to temporarily modify the environment variables"""
    backup_values = {}
    backup_remove = set()
    for key, value in mapping.items():
        if key in os.environ:
            backup_values[key] = os.environ[key]
        else:
            backup_remove.add(key)
        os.environ[key] = value

    try:
        yield
    finally:
        for k, v in backup_values.items():
            os.environ[k] = v
        for k in backup_remove:
            del os.environ[k]


pytest.setenv = setenv


def reimport_env_client():
    from bulk_api_client import env_client

    imp.reload(env_client)
    return env_client.env_client


pytest.reimport_env_client = reimport_env_client


@pytest.fixture
def client():
    token = random_string()
    url = "http://test.org/api/"
    Client.app_api_urls = None
    Client.model_api_urls = {
        "bulk_importer": {
            "examplefortesting": "/bulk/api/bulk_importer/examplefortesting"
        }
    }
    Client.app_api_cache = {}
    json_data = {
        "bulk_importer": "https://data-warehouse.pivot/bulk/api/bulk_importer/",
        "uav": "https://data-warehouse.pivot/bulk/api/uav/",
    }

    data = json.dumps(json_data)
    response = Response()
    response._content = data
    response.status_code = 200
    response.raw = data
    gh_res = Response()
    gh_res._content = b'[{"name":"0.0"}]'
    gh_res.status_code = 200
    with mock.patch.object(requests, "request") as fn:
        fn.side_effect = [response, gh_res]
        client = Client(token, api_url=url)
    client.clear_cache()
    return client


@pytest.fixture
def vcr_client():
    """
    Version of the client without anything mocked; use only in tests with
    `@pytest.mark.vcr()`
    """
    # unset this when recording new cassettes, then find-and-replace the
    # token in the resulting cassette.
    with pytest.setenv(BULK_API_TOKEN="fake-token"):
        client = reimport_env_client()

    return client


@pytest.fixture
def app_api(client):
    app_label = random_string().lower()
    data = {
        app_label: urljoin(client.api_url, "{}/".format(app_label)),
    }
    response = Response()
    response._content = json.dumps(data)
    response.status_code = 200
    with mock.patch.object(Client, "request", return_value=response):
        app_api = AppAPI(client, app_label)

    return app_api


@pytest.fixture
def model_api(app_api):
    model_name = random_string().lower()
    data = {
        model_name: urljoin(
            app_api.client.api_url,
            "{}/{}/".format(app_api.app_label, model_name),
        ),
    }
    model = ".".join([app_api.app_label, model_name])
    properties = {
        "id": {"title": "ID", "type": "integer", "read_only": True},
        "name": {
            "title": "Name",
            "type": "string",
            "maxLength": 256,
            "minLength": 1,
        },
        "text": {"title": "Text", "type": "string", "minLength": 1},
        "integer": {
            "title": "Integer",
            "type": "integer",
            "maximum": 2147483647,
            "minimum": -2147483648,
            "x-nullable": True,
        },
    }
    app_api.client.definitions[model] = {
        "properties": dict(
            random.sample(
                properties.items(), k=random.randint(1, len(properties))
            )
        )
    }
    response = Response()
    response._content = json.dumps(data)
    response.status_code = 200
    with mock.patch.object(Client, "request") as fn:
        fn.return_value = response
        model_api = ModelAPI(app_api, model_name)
    return model_api


@pytest.fixture
def model_api_file(app_api):
    model_name = random_string().lower()
    model = ".".join([app_api.app_label, model_name])

    model_properties = {
        "id": {"title": "ID", "type": "integer", "read_only": True},
        "text": {"title": "Text", "type": "string", "minLength": 1},
        "data_file": {
            "title": "Data File",
            "type": "foreignkey",
            "read_only": True,
        },
    }
    app_api.client.definitions[model] = {"properties": model_properties}
    response_data = {
        model_name: urljoin(app_api.client.api_url, model_name),
    }
    response = Response()
    response._content = json.dumps(response_data)
    response.status_code = 200

    with mock.patch.object(Client, "request", return_value=response):
        model_api = ModelAPI(app_api, model_name)
    return model_api


@pytest.fixture
def model_obj(model_api):
    uri = random_string()
    options = {
        "id": random.randint(0, 1000),
        "text": random_string(),
        "integer": random.randint(-2147483648, 2147483647),
    }
    data = dict(
        random.sample(options.items(), k=random.randint(1, len(options)))
    )
    with mock.patch.object(ModelAPI, "_get", return_value={"id": 1}):
        model_obj = ModelObj(model_api, uri, data)
    return model_obj


@pytest.fixture
def model_obj_file(model_api_file, tmpdir):
    outfile = tmpdir.mkdir("template").join("text.txt")
    outfile_path = str(outfile)
    with open(outfile_path, "w+") as f:
        f.write("abc123")
    obj_data = {
        "text": "model_text",
        "data_file": outfile_path,
    }
    data_file_uri = urljoin(
        "http://test.org/api/",
        "{}/{}/{}".format(app_api.app_label, "api_download", "test.txt"),
    )
    data = {
        "id": 1,
        "text": "model_text",
        "data_file": data_file_uri,
    }
    response = Response()
    response._content = json.dumps(data)
    response.status_code = 200
    with mock.patch.object(Client, "request", return_value=response):
        model_obj = model_api.create(obj_data)
    return model_obj
