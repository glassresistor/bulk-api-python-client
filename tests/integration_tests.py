import pytest
from io import IOBase
from unittest import mock

from requests.models import Response

from bulk_api_client.model import ModelObj
from bulk_api_client.exceptions import BulkAPIError


@pytest.mark.vcr()
def test_relationships_spanning_apps(vcr_client):
    """
    Test showing that traversing a relationship into a new app works;
    accessing the related object should return another model object, not
    a string or uri.
    """
    client = vcr_client
    client.clear_cache()
    client.load_apps()

    PlotGeometry = client.app("uav").model("PlotGeometry")
    pg = PlotGeometry.get(1)
    assert "_meta" not in pg.data
    assert not hasattr(pg, "get__meta")

    plot = pg.plot
    assert not isinstance(plot, str)
    assert isinstance(plot, ModelObj)

    assert plot.uri == pg.data["plot"]


@pytest.mark.vcr()
def test_cannot_set_readonly_field(vcr_client):
    client = vcr_client
    client.clear_cache()
    client.load_apps()

    PlotGeometry = client.app("uav").model("PlotGeometry")
    pg = PlotGeometry.get(1)
    with pytest.raises(BulkAPIError):
        pg.created_at = "2024-01-01"
