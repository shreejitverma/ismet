"""Settings from explicit values, environment, and a config file.

Precedence, highest first: explicit argument, environment variable, config
file, default. Secrets are wrapped in :class:`pydantic.SecretStr` so they never
appear in reprs or logs.

Environment convention::

    ISMET_PROVIDERS=alpaca,mock
    ISMET_CONFIG_FILE=/path/to/config.toml      # optional override
    ISMET_ALPACA_API_KEY=...     # provider "alpaca", credential "api_key"
    ISMET_ALPACA_PAPER=true      # provider "alpaca", option "paper"

A variable is a credential when its key contains KEY, SECRET, TOKEN, PASSWORD,
or PASSPHRASE; otherwise it is an option.

Config file (TOML, default location from ``platformdirs``)::

    providers = ["alpaca"]

    [provider.alpaca.credentials]
    api_key = "..."

    [provider.alpaca.options]
    paper = true
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from platformdirs import user_config_dir
from pydantic import BaseModel, ConfigDict, Field, SecretStr

from ismet.errors import ConfigError

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover - exercised only on 3.10
    import tomli as tomllib

APP_NAME = "ismet"
ENV_PREFIX = "ISMET_"
_SECRET_MARKERS = ("KEY", "SECRET", "TOKEN", "PASSWORD", "PASSPHRASE")


def default_config_path() -> Path:
    """``<user config dir>/ismet/config.toml`` for the current OS."""
    return Path(user_config_dir(APP_NAME)) / "config.toml"


class ProviderSettings(BaseModel):
    """Per-provider credentials (secret) and options (not secret)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    credentials: dict[str, SecretStr] = Field(default_factory=dict)
    options: dict[str, Any] = Field(default_factory=dict)

    def credential(self, key: str) -> SecretStr:
        try:
            return self.credentials[key]
        except KeyError:
            env = f"{ENV_PREFIX}{self.name.upper()}_{key.upper()}"
            raise ConfigError(
                f"provider {self.name!r} is missing credential {key!r}; "
                f"set {env} or [provider.{self.name}.credentials] {key} in the "
                "config file"
            ) from None

    def option(self, key: str, default: Any = None) -> Any:
        return self.options.get(key, default)


class Settings(BaseModel):
    """Resolved application settings."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    providers: tuple[str, ...] = ()
    provider_settings: dict[str, ProviderSettings] = Field(default_factory=dict)
    config_path: Path | None = None

    def for_provider(self, name: str) -> ProviderSettings:
        return self.provider_settings.get(name, ProviderSettings(name=name))

    @classmethod
    def load(
        cls,
        *,
        explicit: Mapping[str, Any] | None = None,
        env: Mapping[str, str] | None = None,
        config_file: Path | None = None,
        use_file: bool = True,
    ) -> Settings:
        """Merge sources by precedence: explicit > env > file > default."""
        environ = os.environ if env is None else env
        layers: list[dict[str, Any]] = []
        path: Path | None = None
        if use_file:
            override = environ.get(f"{ENV_PREFIX}CONFIG_FILE")
            path = config_file or (
                Path(override) if override else default_config_path()
            )
            layers.append(_read_file(path))
        layers.append(_read_env(environ))
        layers.append(_normalise(dict(explicit or {})))
        merged = _merge(layers)
        return cls(
            providers=tuple(merged.get("providers", ())),
            provider_settings={
                name: ProviderSettings(
                    name=name,
                    credentials={
                        k: v if isinstance(v, SecretStr) else SecretStr(str(v))
                        for k, v in body.get("credentials", {}).items()
                    },
                    options=dict(body.get("options", {})),
                )
                for name, body in merged.get("provider", {}).items()
            },
            config_path=path,
        )


def _normalise(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the shape of one layer and lower-case provider names."""
    out: dict[str, Any] = {}
    providers = raw.get("providers")
    if providers is not None:
        if isinstance(providers, str):
            providers = [p for p in (s.strip() for s in providers.split(",")) if p]
        if not isinstance(providers, list | tuple) or not all(
            isinstance(p, str) for p in providers
        ):
            raise ConfigError("'providers' must be a list of provider names")
        out["providers"] = [p.lower() for p in providers]
    provider = raw.get("provider", {})
    if not isinstance(provider, Mapping):
        raise ConfigError("'provider' must be a table keyed by provider name")
    out["provider"] = {}
    for name, body in provider.items():
        if not isinstance(body, Mapping):
            raise ConfigError(f"provider.{name} must be a table")
        creds = body.get("credentials", {})
        opts = body.get("options", {})
        if not isinstance(creds, Mapping) or not isinstance(opts, Mapping):
            raise ConfigError(
                f"provider.{name}.credentials and .options must be tables"
            )
        unknown = set(body) - {"credentials", "options"}
        if unknown:
            raise ConfigError(
                f"provider.{name} has unknown keys {sorted(unknown)}; "
                "use [provider.<name>.credentials] or [provider.<name>.options]"
            )
        out["provider"][str(name).lower()] = {
            "credentials": dict(creds),
            "options": dict(opts),
        }
    return out


def _read_file(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"provider": {}}
    try:
        with path.open("rb") as fh:
            data = tomllib.load(fh)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConfigError(f"cannot read config file {path}: {exc}") from exc
    return _normalise(data)


def _read_env(environ: Mapping[str, str]) -> dict[str, Any]:
    out: dict[str, Any] = {"provider": {}}
    providers = environ.get(f"{ENV_PREFIX}PROVIDERS")
    if providers is not None:
        out["providers"] = providers
    for var, value in environ.items():
        if not var.startswith(ENV_PREFIX) or var in (
            f"{ENV_PREFIX}PROVIDERS",
            f"{ENV_PREFIX}CONFIG_FILE",
        ):
            continue
        rest = var[len(ENV_PREFIX) :]
        name, sep, key = rest.partition("_")
        if not sep or not key:
            continue
        body = out["provider"].setdefault(
            name.lower(), {"credentials": {}, "options": {}}
        )
        if any(marker in key for marker in _SECRET_MARKERS):
            body["credentials"][key.lower()] = SecretStr(value)
        else:
            body["options"][key.lower()] = _coerce_option(value)
    return _normalise(out)


def _coerce_option(value: str) -> Any:
    lowered = value.strip().lower()
    if lowered in ("true", "false"):
        return lowered == "true"
    try:
        return int(value)
    except ValueError:
        return value


def _merge(layers: list[dict[str, Any]]) -> dict[str, Any]:
    """Later layers win. Provider tables merge key by key."""
    merged: dict[str, Any] = {"provider": {}}
    for layer in layers:
        if "providers" in layer:
            merged["providers"] = layer["providers"]
        for name, body in layer.get("provider", {}).items():
            target = merged["provider"].setdefault(
                name, {"credentials": {}, "options": {}}
            )
            target["credentials"].update(body.get("credentials", {}))
            target["options"].update(body.get("options", {}))
    return merged


__all__ = [
    "APP_NAME",
    "ENV_PREFIX",
    "ProviderSettings",
    "Settings",
    "default_config_path",
]
