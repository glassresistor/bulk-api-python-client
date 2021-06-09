import os
import json
import yaml
import pandas
import requests_cache
from collections import OrderedDict

from urllib.parse import urljoin
from io import BytesIO

from bulk_api_client.query_helpers import Q
from bulk_api_client.exceptions import BulkAPIError, ValidationError


CSV_CHUNKSIZE = 10 ** 6

filter_error = TypeError(
    {"detail": "filter must be a dict or yaml string containing a dict"}
)
field_error = TypeError(
    {"detail": "fields must be a list or yaml string containing a list"}
)


def is_kv(kv_str):
    """Determines if an input string is of key=value type

    Args:
        kv_str (str): string to use

    Returns:
        Bool
    """
    return "=" in kv_str


class ModelObjJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ModelObj):
            return obj.uri
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


class ModelAPI(object):
    def __init__(self, app_api, model_name):
        """Model object. Given a model name, this object makes a request — using
        the Client class — to the Bulk Importer API. If given a model in the
        previously specified app, the response is cached in model_api_urls
        dictionary.

        Args:
            app_api (obj): AppAPI object
            model_name (str): model name of the desired model of an app

        """

        self.app = app_api
        self.model_name = model_name.lower()

        url = self.app.client.app_api_urls[self.app.app_label]
        params = {}
        response = self.app.client.request("GET", url, params)

        if self.app.app_label not in self.app.client.model_api_urls:
            self.app.client.model_api_urls[self.app.app_label] = json.loads(
                response.content
            )
        if (
            self.model_name
            not in self.app.client.model_api_urls[self.app.app_label]
        ):
            raise BulkAPIError(
                {
                    "model_api": "Model {} does not exist in bulk api".format(
                        self.model_name
                    )
                }
            )

    def fields_dict_to_list(self, fields_dict):
        """
        Creates a list of single dict items from a dict to meet query api
        specification.

        E.g. converts
            {"field1": "field1_alias", "field2": "field2_alias"}
        to
            [{"field1": "field1_alias"}, {"field2": "field2_alias"}]

        Args:
            fields_dict (dict): dict of fields columns to desired aliases

        Returns:
            list of single dicts

        """
        return [{k: v} for k, v in fields_dict.items()]

    def query(
        self,
        fields=None,
        filter=None,
        order=None,
        distinct=False,
        page_size=None,
        skip_cache=None,
    ):
        """Queries to create a Pandas DataFrame for given queryset. The default
        query may be obtained by calling the function, without passing
        any parameters.

        Args:
            fields (list): list of specified fields for the fields query
            filter (str or dict): filter for the filter query; must be a dict
                or a yaml string representation of a dict
            order (str): order for the order query
            distinct (bool): whether to remove duplicate rows from the results
            page_size (str): page size for the page_size query; Default: 10,000
            skip_cache (bool): pause global caching for query request

        Returns:
            pandas dataframe

        """
        dataframes = []
        current_page = 1
        df_count = page_size

        while df_count == page_size:
            kwargs = {
                "fields": fields,
                "filter": filter,
                "distinct": distinct,
                "order": order,
                "page": current_page,
                "page_size": page_size,
            }
            if skip_cache:
                with requests_cache.disabled():
                    df = self._query(**kwargs)
            else:
                df = self._query(**kwargs)
            df_count = len(df.index)
            current_page += 1
            dataframes.append(df)
        return pandas.concat(dataframes)

    def _query(
        self,
        fields=None,
        filter=None,
        order=None,
        distinct=False,
        page=None,
        page_size=None,
    ):
        """Queries to create a Pandas DataFrame for given queryset. The default
        query may be obtained by calling the function, without passing
        any parameters.

        Args:
            fields (list): list of specified fields for the fields query
            filter (str or dict): filter for the filter query; must be a dict
                or a yaml string representation of a dict
            order (str): order for the order query
            distinct (bool): whether to remove duplicate rows from the results
            page (str): page number for the page query; Default: 1
            page_size (str): page size for the page_size query; Default: 10,000

        Returns:
            pandas dataframe

        """

        if fields is not None:
            # If fields is a string, validate it is correct YAML for a list
            if isinstance(fields, str):
                fields = yaml.safe_load(fields)
            if not any(
                [isinstance(fields, x) for x in [list, dict, OrderedDict]]
            ):
                raise field_error
            if any([isinstance(fields, x) for x in [OrderedDict, dict]]):
                fields = self.fields_dict_to_list(fields)
            fields = yaml.safe_dump(fields)
        if filter is not None:
            # If filter is a string, validate it is correct YAML for a dict
            if isinstance(filter, str):
                filter = yaml.safe_load(filter)
            if isinstance(filter, Q):
                filter = filter.output_filter()
            if not isinstance(filter, dict):
                raise filter_error
            # Whether it was a dict or string initially, convert to YAML
            # for sending over the wire.
            filter = yaml.safe_dump(filter)
        if order is not None:
            if not isinstance(order, str):
                raise TypeError({"detail": "order must be a string"})
        if not isinstance(distinct, bool):
            raise TypeError({"detail": "distinct must be a boolean"})
        if page is not None and (not isinstance(page, int) or page <= 0):
            raise TypeError({"detail": "page must be a positive integer"})
        if page is None:
            page = 1
        if page_size is not None and (
            not isinstance(page_size, int) or page_size <= 0
        ):
            raise TypeError({"detail": "page size must be a positive integer"})

        url_path = self.app.client.model_api_urls[self.app.app_label][
            self.model_name
        ]
        url = urljoin(url_path, "query")
        params = {
            "fields": fields,
            "filter": filter,
            "order": order,
            "distinct": distinct,
            "page": page,
            "page_size": page_size,
        }

        with self.app.client.request("GET", url, params=params) as response:
            if response.content:
                df = pandas.concat(
                    pandas.read_csv(
                        BytesIO(response.content), chunksize=CSV_CHUNKSIZE
                    ),
                    ignore_index=True,
                )
            else:
                df = pandas.DataFrame()
        return df

    def _list(self, page, filter=None, order=None):
        """Lists all model object of a given model; Makes a 'GET' method request
        to the Bulk API

        Args:
            page (int): page number of list of model instances
        Returns:
            list of dictionary objects of the model data

        """
        path = self.app.client.model_api_urls[self.app.app_label][
            self.model_name
        ]
        url = urljoin(self.app.client.api_url, path)
        if filter is not None:
            # If filter is a string, validate it is correct YAML for a dict
            if isinstance(filter, str):
                filter = yaml.safe_load(filter)
            if not isinstance(filter, dict):
                raise filter_error
            # Whether it was a dict or string initially, convert to YAML
            # for sending over the wire.
            filter = yaml.safe_dump(filter)
        if order is not None:
            if not isinstance(order, str):
                raise TypeError({"detail": "order must be a string"})
        params = {"page": page, "filter": filter, "order": order}
        with requests_cache.disabled():
            response = self.app.client.request("GET", url, params=params)
        return json.loads(response.content)["results"]

    def list(self, page, filter=None, order=None):
        """Makes call to private list method and creates list of ModelObj
        instances from returned model data

        Args:
            page (int): page number of list of model instances
        Returns:
            list of ModelObjs

        """
        data = self._list(page=page, filter=filter, order=order)
        path = self.app.client.model_api_urls[self.app.app_label][
            self.model_name
        ]
        objs = []
        for obj_data in data:
            uri = os.path.join(path, str(obj_data["id"]))
            objs.append(ModelObj.with_properties(self, uri, data=obj_data))
        return objs

    def _create(self, obj_data):
        """Creates a model object given it's primary key and new object data;
        Makes a 'POST' method request to the Bulk API

        Args:
            obj_data (dict): new data to create the object with

        Returns:
            dictionary object of the model data

        """
        path = self.app.client.model_api_urls[self.app.app_label][
            self.model_name
        ]
        url = urljoin(self.app.client.api_url, path)
        files = {}
        for field, val in obj_data.items():
            if hasattr(val, "read"):
                files[field] = val

        obj_data = {k: v for k, v in obj_data.items() if k not in files}
        kwargs = {}
        if files:
            kwargs = {
                "data": obj_data,
                "files": files,
            }
        else:
            data = json.dumps(obj_data, cls=ModelObjJSONEncoder)
            kwargs = {
                "data": data,
                "headers": {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            }

        response = self.app.client.request("POST", url, params={}, **kwargs)

        content = json.loads(response.content)
        # For validation of created bulk imports
        if content.get("results", dict()).get("errors"):
            raise ValidationError(
                "\n".join([error for error in content["results"]["errors"]])
            )

        return content

    def create(self, obj_data):
        """Makes call to private create method and creates ModelObj instance
        from returned model data

        Args:
            obj_data (dict): new data to create the object with
        Returns:
            ModelObj

        """
        data = self._create(obj_data)
        path = self.app.client.model_api_urls[self.app.app_label][
            self.model_name
        ]
        uri = os.path.join(path, str(data["id"]))
        return ModelObj.with_properties(self, uri=uri, data=data)

    def _get(self, uri):
        """Gets a model object given it's primary key; Makes a 'GET' method
        request to the Bulk API

        Args:
            uri (str): identifier of object

        Returns:
            dictionary object of the model data

        """

        url = urljoin(self.app.client.api_url, uri)
        with requests_cache.disabled():
            response = self.app.client.request("GET", url, params={})
        return json.loads(response.content)

    def get(self, pk):
        """Makes call to private get method and creates a ModelObj instance
        from returned model data

        Args:
            pk (int): primary key of object
        Returns:
            ModelObj

        """
        path = self.app.client.model_api_urls[self.app.app_label][
            self.model_name
        ]
        uri = os.path.join(path, str(pk))
        data = self._get(uri)

        return ModelObj.with_properties(self, uri, data=data)

    def _update(self, uri, obj_data, patch=True):
        """Updates a model object given it's primary key and new object data;
        Makes a 'PATCH' method request to the Bulk API

        Args:
            uri (str): identifier of object
            obj_data (dict): new data to update the object with
            patch(bool): partial update (default: True)

        Returns:

        """

        url = urljoin(self.app.client.api_url, uri)
        files = {}
        for field, val in obj_data.items():
            if hasattr(val, "read"):
                files[field] = val
        obj_data = {k: v for k, v in obj_data.items() if k not in files}
        kwargs = {}
        if files:
            kwargs = {
                "data": obj_data,
                "files": files,
            }
        else:
            data = json.dumps(obj_data, cls=ModelObjJSONEncoder)
            kwargs = {
                "data": data,
                "headers": {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            }
        method = "PATCH"
        if not patch:
            method = "PUT"
        response = self.app.client.request(method, url, params={}, **kwargs)
        if response.status_code == 200:
            return json.loads(response.content)
        raise BulkAPIError(
            {
                "model_api": "update not successful. Status code {}; {}".format(
                    response.status_code, response.content
                )
            }
        )

    def _delete(self, uri):
        """Deletes a model object given its primary key; Makes a delete
        method request to the Bulk API

        Args:
            uri (str): identifier of object

        Returns:

        """
        url = urljoin(self.app.client.api_url, uri)
        response = self.app.client.request("DELETE", url, params={})
        if response.status_code != 204:
            raise BulkAPIError(
                {
                    "model_api": "delete not successful. Status code {}; {}".format(
                        response.status_code, response.content
                    )
                }
            )

    def __str__(self):
        return "ModelAPI: {}.{}".format(self.app.app_label, self.model_name)


def _get_f(field, properties):
    """Dynamically builds a getter method using a string represnting the name of
     the property and properties of the attribute. Internal getter method used
     by ModelObj with_properties class method.

    Args:
        field (str): ModelAPI its related to
        properties (dict): Model properties from the OPTIONS endpoint, in
            {fieldname: {field definition metadata}} format

    Returns:
        getter method
    """

    def get_f(cls):
        field_val = cls.data.get(field)
        if properties[field].get("type") == "foreignkey":
            if hasattr(cls, "_%s" % field):
                # The related object was set locally with set_f; return that
                return getattr(cls, "_%s" % field)
            # Create a ModelObj for the related object and return it
            path = field_val.replace(cls.model_api.app.client.api_url, "")
            app_label, model_name, _id = path.split("/")
            model = cls.model_api.app.client.app(app_label).model(model_name)
            related_obj = ModelObj.with_properties(model, field_val)
            return related_obj

        return field_val

    return get_f


def _set_f(field, properties):
    """Dynamically builds a setter method using a string represnting the name of
     the property and properties of the attribute. Internal getter method used
     by ModelObj with_properties class method.

    Args:
        field (str): ModelAPI its related to
        properties (dict): Model properties from the OPTIONS endpoint, in
            {fieldname: {field definition metadata}} format

    Returns:
        setter method
    """

    def set_f(cls, val):
        if properties[field].get("read_only", False):
            raise BulkAPIError({"ModelObj": "Cannot set a read only property"})
        if properties[field].get("type") == "foreignkey":
            if not isinstance(val, ModelObj):
                raise BulkAPIError(
                    {"ModelObj": "New related model must be a _ModelObj"}
                )
            setattr(cls, "_%s" % field, val)
            val = val.uri
        cls.data[field] = val

    return set_f


class ModelObj:
    """
    **DO NOT CALL DIRECTLY**
    Base object which handles mapping local data to api actions. Must call the
    with_properties class method function to get properties

    Args:
        model_api (obj): ModelAPI its related to
        uri (str): uri of the resource
        data (dict): property which memoizes _data

    Returns:


    """

    def __init__(self, model_api, uri, data=None):
        self.model_api = model_api
        self.uri = uri
        self.data = data

    def set_data(self, data):
        self._data = data

    def get_data(self):
        if self._data:
            return self._data
        self.data = self.model_api._get(self.uri)
        return self._data

    data = property(get_data, set_data)

    @classmethod
    def with_properties(cls, model_api, uri, data=None):
        """
        Returns an object with proerties of the given model to be modified
        directly and reflected in the database. Mimics objects used by ORMs

        Args:
            model_api (obj): ModelAPI its related to
            uri (str): uri of the resource
            data (dict): property which memoizes _data

        Returns:
            ModelObjWithProperties obj

        """
        if not isinstance(model_api, ModelAPI):
            raise BulkAPIError(
                {"ModelObj": "Given model is not a ModelAPI object"}
            )

        class ModelObjWithProperties(cls):
            pass

        model_properties = ModelObj._get_model_properties(model_api)
        for field, _ in model_properties.items():
            if field == "_meta":
                # don't create a field named _meta;
                # that's the django definitions
                continue
            get_f = _get_f(field, model_properties)
            setattr(ModelObjWithProperties, "get_%s" % field, get_f)

            set_f = _set_f(field, model_properties)
            setattr(ModelObjWithProperties, "set_%s" % field, set_f)

            setattr(
                ModelObjWithProperties,
                field,
                property(
                    getattr(ModelObjWithProperties, "get_%s" % field),
                    getattr(ModelObjWithProperties, "set_%s" % field),
                ),
            )
        return ModelObjWithProperties(model_api, uri, data)

    @staticmethod
    def _get_model_properties(model_api):
        """
        Retrieves and caches the properties for a given model, provided by
        the model's api object.
        """
        model = ".".join([model_api.app.app_label, model_api.model_name])
        if model not in model_api.app.client.definitions:
            path = model_api.app.client.model_api_urls[model_api.app.app_label][
                model_api.model_name
            ]
            model_api.app.client.definitions[model] = ModelObj._get_definitions(
                model_api, path
            )
        return model_api.app.client.definitions[model]

    @staticmethod
    def _get_definitions(model_api, path):
        response = model_api.app.client.request("OPTIONS", path, params={})
        return ModelObj._metadata_to_field_properties(response.json())

    @staticmethod
    def _metadata_to_field_properties(metadata):
        return {item["key"]: item for item in metadata}

    def save(self):
        """Makes a call to the put update method of the model_api object

        Args:

        Returns:

        """

        self.model_api._update(self.uri, self.data, patch=False)

    def update(self, data):
        """Makes a call to the patch update method of the model_api object

        Args:
            data (dict): data to update the object with

        Returns:

        """
        self.data = self.model_api._update(self.uri, data)

    def delete(self):
        """Makes a call to the delete method of the model_api object

        Args:

        Returns:

        """
        self.model_api._delete(self.uri)

    def __str__(self):
        return "ModelObj: {}".format(self.uri)
