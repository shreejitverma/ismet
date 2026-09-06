"""Provider discovery through the ``ismet.providers`` entry-point group."""

from __future__ import annotations

from importlib.metadata import EntryPoint, entry_points

from ismet.errors import ConfigError
from ismet.providers.base import Provider

ENTRY_POINT_GROUP = "ismet.providers"


class ProviderRegistry:
    """Name to provider-class mapping, populated from entry points or by hand."""

    def __init__(self) -> None:
        self._classes: dict[str, type[Provider]] = {}
        self._entries: dict[str, EntryPoint] = {}

    @classmethod
    def discover(cls, group: str = ENTRY_POINT_GROUP) -> ProviderRegistry:
        """Registry with every provider advertised by installed packages."""
        registry = cls()
        for ep in entry_points(group=group):
            registry._entries[ep.name.lower()] = ep
        return registry

    def register(self, provider_cls: type[Provider], name: str | None = None) -> None:
        key = (name or provider_cls.name).lower()
        self._classes[key] = provider_cls

    def names(self) -> list[str]:
        return sorted(set(self._classes) | set(self._entries))

    def __contains__(self, name: object) -> bool:
        return isinstance(name, str) and name.lower() in set(self._classes) | set(
            self._entries
        )

    def get(self, name: str) -> type[Provider]:
        key = name.lower()
        if key in self._classes:
            return self._classes[key]
        ep = self._entries.get(key)
        if ep is None:
            raise ConfigError(
                f"unknown provider {name!r}; installed providers: {self.names()}"
            )
        try:
            loaded = ep.load()
        except Exception as exc:
            raise ConfigError(f"provider {name!r} failed to import: {exc}") from exc
        if not (isinstance(loaded, type) and issubclass(loaded, Provider)):
            raise ConfigError(
                f"entry point {ep.value!r} for provider {name!r} is not a Provider"
            )
        self._classes[key] = loaded
        return loaded


__all__ = ["ENTRY_POINT_GROUP", "ProviderRegistry"]
