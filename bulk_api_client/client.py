import os
import requests
import requests_cache
import json
import logging
from tempfile import gettempdir

from bulk_api_client.app import AppAPI
from bulk_api_client.exceptions import BulkAPIError

CERT_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "data-warehouse.pivot.pem"
)


def is_json(d):
    try:
        json.loads(d)
    except ValueError:
        return False
    return True


class Client(object):
    def __init__(self, token, api_url=None, expiration_time=None, log=False):
        """API Client object for bulk_importer to handle app and model requests.
        Requies a user token with access to data-warehouse

        Args:
            token (str): user token with permissions to access the API
            api_url (str): base url for api request; defaults to data_warehouse
            expiration_time (int): denote time requests expire from cache

        """
        self.token = token
        self.app_api_urls = None
        """Dict of Bulk Importer app urls. Updated with the initialization of a
        AppAPI object"""
        self.model_api_urls = {}
        """Dict of Bulk Importer model urls. Updated with the initialization of a
        ModelAPI object"""
        self.app_api_cache = {}
        """
        Dict of AppAPI objects, created via app(), key of app_label
        """

        if api_url is None:
            self.api_url = "https://data-warehouse.pivot/bulk/api/"
        else:
            self.api_url = api_url
        if expiration_time is None:
            self.expiration_time = 7200
        else:
            self.expiration_time = expiration_time
        requests_cache.install_cache(
            "bulk-api-cache",
            backend=requests_cache.backends.sqlite.DbCache(
                location=os.path.join(gettempdir(), "bulk-api-cache")
            ),
            expire_after=expiration_time,
            allowable_methods=("GET", "OPTIONS"),
        )
        self.log = log
        if self.log:
            logging.basicConfig(level=logging.DEBUG)
        self.definitions = {}

        self.load_apps()

    def load_apps(self):
        """
        Retrieves the top-level list of apps.
        """
        apps_res = self.request(method="GET", url=self.api_url, params={},)
        self.apps = json.loads(apps_res.content)

    @property
    def log(self):
        return self._log

    @log.setter
    def log(self, value):
        if value:
            logging.basicConfig(level=logging.DEBUG)
        else:
            root_logger = logging.getLogger()
            root_logger.setLevel(logging.WARNING)
            root_logger.handlers = []
        self._log = value

    def request(self, method, url, params, *args, **kwargs):
        """Request function to construct and send a request. Uses the Requests
        python library

        Args:
            method (str): method for the request
            path (str): path to the resource the client should access
            params (dict): (optional) Dictionary, list of tuples or bytes to
            send in the query string for the Request.

        Returns:
            response obj

        """
        headers = {
            "Authorization": "Token {}".format(self.token),
        }
        if kwargs.get("headers"):
            kwargs["headers"] = {**headers, **kwargs["headers"]}
        else:
            kwargs["headers"] = headers
        response = requests.request(
            method=method,
            url=url,
            params=params,
            verify=CERT_PATH,
            stream=True,
            **kwargs,
        )

        # catch 4XX client error or 5XX server error response
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            content = response.content

            if is_json(content):
                raise BulkAPIError(content)

            raise BulkAPIError(
                "{} Error raised — something went wrong.\nPlease send this "
                "message to data-services+api-error@pivotbio.com, including "
                "the link below:\n\n{}\nIf you are curious as the the nature of"
                " the problem following the above link might provide some "
                "help.".format(response.status_code, response.url)
            )
        else:
            return response

    def clear_cache(self):
        """Empty requests cache"""

        requests_cache.clear()

    def app(self, app_label):
        """Creates AppAPI object from a given app label

        Args:
            app_label (str): app label of the desired model of an app

        Returns:
            AppAPI obj

        """
        if app_label not in self.app_api_cache:
            self.app_api_cache[app_label] = AppAPI(self, app_label)
        return self.app_api_cache[app_label]
