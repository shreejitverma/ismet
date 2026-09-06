from __future__ import annotations

from importlib.metadata import EntryPoint

import pytest

from ismet.errors import ConfigError
from ismet.providers import Provider, ProviderRegistry
from ismet.providers.mock import MockProvider


class Custom(Provider):
    name = "custom"
    venues = frozenset({"xtst"})


def test_register_and_get() -> None:
    reg = ProviderRegistry()
    reg.register(Custom)
    reg.register(MockProvider, name="Alias")
    assert reg.get("CUSTOM") is Custom
    assert reg.get("alias") is MockProvider
    assert reg.names() == ["alias", "custom"]
    assert "custom" in reg and "nope" not in reg and 3 not in reg
    assert Custom.venues == frozenset({"XTST"})
    with pytest.raises(ConfigError, match="unknown provider"):
        reg.get("nope")


def test_discover_finds_installed_mock_entry_point() -> None:
    reg = ProviderRegistry.discover()
    assert "mock" in reg
    assert reg.get("mock") is MockProvider
    assert reg.get("mock") is MockProvider  # cached path


def test_bad_entry_points_raise_config_error() -> None:
    reg = ProviderRegistry()
    reg._entries["broken"] = EntryPoint(
        "broken", "ismet.nonexistent:Nope", "ismet.providers"
    )
    reg._entries["notaprovider"] = EntryPoint(
        "notaprovider", "ismet.errors:IsmetError", "ismet.providers"
    )
    with pytest.raises(ConfigError, match="failed to import"):
        reg.get("broken")
    with pytest.raises(ConfigError, match="not a Provider"):
        reg.get("notaprovider")


def test_provider_subclass_requires_name() -> None:
    with pytest.raises(TypeError, match="must define"):

        class Nameless(Provider):
            pass
