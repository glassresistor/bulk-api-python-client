import pytest
import requests_cache

from bulk_api_client.model import ModelAPI


@pytest.mark.vcr()
def test_models(vcr_client):
    """
    Tests of models initialization in the models.py pseudo-package.
    """
    vcr_client.clear_cache()

    with requests_cache.disabled():
        with pytest.setenv(BULK_API_TOKEN="token"):
            pytest.reimport_env_client()
            from bulk_api_client import models

    assert hasattr(models, "uav")
    assert hasattr(models, "bulk_importer")

    uav_models = [
        getattr(models.uav, name)
        for name in dir(models.uav)
        if isinstance(getattr(models.uav, name), ModelAPI)
    ]

    assert len(uav_models) == 4

    bulk_importer_models = [
        getattr(models.bulk_importer, name)
        for name in dir(models.bulk_importer)
        if isinstance(getattr(models.bulk_importer, name), ModelAPI)
    ]

    assert len(bulk_importer_models) == 10

    # try direct imports; should require no additional requests
    from bulk_api_client.models import uav
    from bulk_api_client.models.uav import Flight

    from bulk_api_client.models.bulk_importer import ExampleForTesting
