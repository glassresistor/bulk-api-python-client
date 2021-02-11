import sys
import json
from urllib.parse import urljoin

from bulk_api_client.env_client import env_client

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

    def get_metadata(self, content):
        for d in json.loads(content):
            if d["key"] == "_meta":
                return d
        return None

    def get_models(app):
        pass


for app_name, app_url in env_client.apps.items():
    # app_name, model_name = definition.split(".")
    # app = getattr(models, app_name, None)
    # if not app:
    app = App(app_name)
    app.get_models(app_url)
    app.add_model(model_name)
