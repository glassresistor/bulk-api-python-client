import asyncio
import sys
import json
from urllib.parse import urljoin

from bulk_api_client.env_client import env_client
from bulk_api_client.exceptions import BulkAPIError

models = sys.modules[__name__]
__all__ = []


class App:
    def __init__(self, app_name):
        self.app = env_client.app(app_name)
        setattr(models, app_name, self)
        __all__.append(app_name)

        module_name = ".".join([__name__, app_name])
        sys.modules[module_name] = self

    def add_model(self, model_name):
        path = "{}/{}/".format(self.app.app_label, model_name)
        url = urljoin(env_client.api_url, path, "/")
        res = env_client.request("options", url, {})
        django_model_name = self.get_metadata(res.content)["django_model_name"]
        app_model = self.app.model(model_name)
        setattr(models, django_model_name, app_model)
        setattr(self, django_model_name, app_model)

    async def add_model_async(self, model_name):
        path = "{}/{}/".format(self.app.app_label, model_name)
        url = urljoin(env_client.api_url, path, "/")
        print("Getting options at url:", url)
        res = await env_client.request_async("options", url)
        django_model_name = self.get_metadata(res.content)["django_model_name"]
        app_model = self.app.model(model_name)
        setattr(models, django_model_name, app_model)
        setattr(self, django_model_name, app_model)

    def get_metadata(self, content):
        for d in json.loads(content):
            if d["key"] == "_meta":
                return d
        return None

    async def add_models_async(self, app_response):
        """
        Get the list of models/urls from the ApiAppView and add them all.
        """
        for model_name in app_response.keys():
            await self.add_model_async(model_name)

    def add_models(self, app_response):
        """
        Get the list of models/urls from the ApiAppView and add them all.
        """
        for model_name in app_response.keys():
            self.add_model(model_name)


async def load_apps_async():
    tasks = [
        load_models_for_app_async(app_name, app_url)
        for app_name, app_url in env_client.apps.items()
    ]
    await asyncio.gather(*tasks)


def load_apps():
    for app_name, app_url in env_client.apps.items():
        load_models_for_app(app_name, app_url)


async def load_models_for_app_async(app_name, app_url):
    print(f"Loading models for {app_name}")
    try:
        res = await env_client.request_async("get", app_url)
    except BulkAPIError:
        print("Error getting app:", app_url)
        return  # some apps have no API-accessible models and 500 instead

    try:
        app_response = json.loads(res.content)
    except:
        print("Error loading JSON for app:", app_name)
        return  # endpoint doesn't return JSON for some reason, skip it

    app = App(app_name)
    await app.add_models_async(app_response)


def load_models_for_app(app_name, app_url):
    print(f"Loading models for {app_name}")
    try:
        res = env_client.request("get", app_url)
    except BulkAPIError:
        return  # some apps have no API-accessible models and 500 instead

    try:
        app_response = json.loads(res.content)
    except:
        return  # endpoint doesn't return JSON for some reason, skip it

    app = App(app_name)
    app.add_models(app_response)


# do it
load_apps()
# asyncio.run(load_apps_async())
