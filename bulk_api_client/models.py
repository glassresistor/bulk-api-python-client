import re
import sys

from bulk_api_client.env_client import env_client

models = sys.modules[__name__]
__all__ = []


class App:
    def __init__(self, app_name):
        self.app = env_client.app(app_name)
        setattr(models, app_name, self)
        __all__.append(app_name)

    def add_model(self, model_name):
        setattr(self, model_name.capitalize(), self.app.model(model_name))


for definition in env_client.swagger_data["definitions"].keys():
    app_name, model_name = definition.split(".")
    app = getattr(models, app_name, None)
    if not app:
        app = App(app_name)
    app.add_model(model_name)
