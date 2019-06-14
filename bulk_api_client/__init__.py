import os
import pandas
import requests

from io import BytesIO
from urllib.parse import urljoin


CERT_PATH = os.path.join(
    os.path.dirname(
        os.path.realpath(__file__)),
    'data-warehouse.pivot.pem')


class Client(object):
    def __init__(self, token, api_url='https://data-warehouse.pivot'):
        self.token = token
        self.api_url = api_url

    def request(self, method, path, params, ):
        headers = {'Authorization': 'Token {}'.format(self.token)}
        return requests.request(
            method,
            urljoin(
                self.api_url,
                path),
            params=params,
            headers=headers,
            verify=CERT_PATH)

    def app(self, app_label):
        return AppAPI(self, app_label)


class AppAPI(object):
    def __init__(self, client, app_label,):
        self.client = client
        self.app_label = app_label

    def model(self, model_name):
        return ModelAPI(self, model_name)


class ModelAPI(object):
    def __init__(self, app_api, model_name):
        self.app = app_api
        self.model_name = model_name

    def query(self, fields=None, filter=None, order=None, page=None,
              page_size=None):
        path = 'bulk/pandas_views/{}/{}'.format(
            self.app.app_label, self.model_name)
        if fields:
            fields = ','.join(fields)
        params = {'fields': fields, 'filter': filter,
                  'ordering': order, 'page': page, 'page_size': page_size}
        response = self.app.client.request('GET', path, params=params,)
        csv_file = pandas.read_csv(BytesIO(response.content))
        return csv_file
