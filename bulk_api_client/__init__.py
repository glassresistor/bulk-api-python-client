import requests
from urllib.parse import urljoin


class Client(object):
    def __init__(self, token, api_url='https://data-warehouse.pivot'):
        self.token = token
        self.api_url = api_url

    def request(self, method, path, params, ):
        headers = {'Authorization': 'Token {}'.format(self.token)}
        return requests.request(method, urljoin(self.api_url, path),
                                params=params, headers=headers)

    def app(self, app_label):
        class AppAPI(object):
            def __init__(self, app_label):
                self.app_label = app_label

        return AppAPI


class AppAPI(Client):
    def __init__(self, token, app_label, api_url='https://data-warehouse.pivot',
                 ):
        super().__init__(token)
        self.app_label = app_label

    def model(self, model_name):
        return ModelAPI(model_name)


class ModelAPI(object):
    def __init__(self, model_name):
        self.model_name = model_name
