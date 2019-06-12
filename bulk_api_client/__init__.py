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
