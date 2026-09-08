from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eeclient.export.image import (
    AssetOptions,
    EarthEngineDestination,
    PyramidingPolicy,
    image_to_asset_async,
)
from eeclient.interfaces.export import ExportProtocol


def _asset_options(**kwargs):
    return AssetOptions(
        earth_engine_destination=EarthEngineDestination(name="projects/p/assets/x"),
        **kwargs,
    )


def test_asset_options_omit_pyramiding_by_default():
    dumped = _asset_options().model_dump(by_alias=True, exclude_none=True)
    assert "pyramidingPolicy" not in dumped
    assert "pyramidingPolicyOverrides" not in dumped


def test_asset_options_serialize_default_policy_as_camel_case():
    dumped = _asset_options(pyramiding_policy="MODE").model_dump(
        by_alias=True, exclude_none=True
    )
    assert dumped["pyramidingPolicy"] == "MODE"


def test_asset_options_serialize_per_band_overrides():
    dumped = _asset_options(pyramiding_policy_overrides={"B1": "MIN"}).model_dump(
        by_alias=True, exclude_none=True
    )
    assert dumped["pyramidingPolicyOverrides"] == {"B1": "MIN"}


def test_asset_options_accept_lowercase_policy():
    """`ee.batch` upper-cases whatever the caller passes, so callers write "mode"."""
    opts = _asset_options(
        pyramiding_policy="mode", pyramiding_policy_overrides={"B1": "min"}
    )
    assert opts.pyramiding_policy is PyramidingPolicy.MODE
    assert opts.pyramiding_policy_overrides == {"B1": PyramidingPolicy.MIN}


def test_asset_options_reject_unknown_policy():
    with pytest.raises(ValueError):
        _asset_options(pyramiding_policy="average")


def test_asset_options_reject_unknown_override_policy():
    with pytest.raises(ValueError):
        _asset_options(pyramiding_policy_overrides={"B1": "average"})


@pytest.mark.asyncio
async def test_image_to_asset_async_forwards_pyramiding_to_the_request():
    image = MagicMock(name="image")
    image._apply_crs_and_affine.return_value = (image, {}, False)
    image._apply_selection_and_scale.return_value = (image, {})

    client = MagicMock()
    client.rest_call = AsyncMock(return_value={})

    with (
        patch("eeclient.export.image.serializer.encode", return_value={}),
        patch("eeclient.export.image.Task.model_validate", return_value=MagicMock()),
    ):
        await image_to_asset_async(
            client=client,
            image=image,
            asset_id="projects/p/assets/x",
            pyramiding_policy="mode",
            pyramiding_policy_overrides={"B1": "min"},
        )

    asset_options = client.rest_call.await_args.kwargs["data"]["assetExportOptions"]
    assert asset_options["pyramidingPolicy"] == "MODE"
    assert asset_options["pyramidingPolicyOverrides"] == {"B1": "MIN"}


@pytest.mark.parametrize("param", ["pyramiding_policy", "pyramiding_policy_overrides"])
def test_export_protocol_matches_the_implementation(param):
    """`session.export.*` dispatches through the Protocol, so it must agree."""
    import inspect

    assert param in inspect.signature(ExportProtocol.image_to_asset_async).parameters
