"""Provider base class."""

from __future__ import annotations

from typing import ClassVar

from ismet.capabilities import Capability, capabilities_of
from ismet.config import ProviderSettings
from ismet.errors import NotSupported
from ismet.transport.clock import SYSTEM_CLOCK, Clock


class Provider:
    """Base for every provider adapter.

    A provider is who gives you access; a venue (MIC) is where instruments
    trade. ``venues`` lists the MICs this provider can serve. Capabilities are
    discovered structurally from the protocols the subclass implements.
    """

    name: ClassVar[str]
    venues: ClassVar[frozenset[str]] = frozenset()

    def __init__(self, *, clock: Clock = SYSTEM_CLOCK) -> None:
        self.clock = clock

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        if not getattr(cls, "name", None):
            raise TypeError(f"{cls.__name__} must define a class-level 'name'")
        cls.venues = frozenset(v.upper() for v in cls.venues)

    @classmethod
    def from_settings(cls, settings: ProviderSettings) -> Provider:  # noqa: ARG003
        """Build a provider from resolved settings. Override to read credentials."""
        return cls()

    async def open(self) -> None:
        """Open connections. Idempotent."""

    async def close(self) -> None:
        """Release connections. Idempotent."""

    async def __aenter__(self) -> Provider:
        await self.open()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    def capabilities(self) -> frozenset[Capability]:
        return capabilities_of(self)

    def supports(self, capability: Capability) -> bool:
        return capability in self.capabilities()

    def require(self, capability: Capability) -> None:
        """Raise :class:`NotSupported` unless this provider has ``capability``."""
        if not self.supports(capability):
            raise NotSupported(self.name, capability.value)

    def serves(self, venue: str) -> bool:
        return venue.upper() in self.venues

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(name={self.name!r}, venues={sorted(self.venues)})"
        )


__all__ = ["Provider"]
