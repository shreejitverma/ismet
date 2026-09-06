from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import SecretStr

from ismet.config import ProviderSettings, Settings, default_config_path
from ismet.errors import ConfigError


def test_env_layer_parses_providers_credentials_and_options() -> None:
    env = {
        "ISMET_PROVIDERS": "Alpaca, mock",
        "ISMET_ALPACA_API_KEY": "sekrit-value",
        "ISMET_ALPACA_API_SECRET": "s",
        "ISMET_ALPACA_PAPER": "true",
        "ISMET_ALPACA_RETRIES": "3",
        "ISMET_ALPACA_REGION": "us",
        "ISMET_BOGUS": "ignored",
        "OTHER": "x",
    }
    s = Settings.load(env=env, use_file=False)
    assert s.providers == ("alpaca", "mock")
    a = s.for_provider("alpaca")
    assert a.credential("api_key").get_secret_value() == "sekrit-value"
    assert a.credential("api_secret").get_secret_value() == "s"
    assert a.options == {"paper": True, "retries": 3, "region": "us"}
    assert a.option("missing", 7) == 7
    assert "sekrit" not in repr(a) and "sekrit" not in str(a)
    assert s.for_provider("nothere") == ProviderSettings(name="nothere")


def test_missing_credential_error_names_env_var() -> None:
    with pytest.raises(ConfigError, match="ISMET_ALPACA_API_KEY"):
        ProviderSettings(name="alpaca").credential("api_key")


def test_file_layer_and_precedence(tmp_path: Path) -> None:
    cfg = tmp_path / "config.toml"
    cfg.write_text(
        'providers = ["kite"]\n'
        '[provider.kite.credentials]\napi_key = "file-key"\ntoken = "file-token"\n'
        '[provider.kite.options]\nexchange = "NSE"\n'
    )
    env = {"ISMET_KITE_API_KEY": "env-key", "ISMET_CONFIG_FILE": str(cfg)}
    s = Settings.load(env=env)
    assert s.config_path == cfg
    assert s.providers == ("kite",)
    k = s.for_provider("kite")
    assert k.credential("api_key").get_secret_value() == "env-key"
    assert k.credential("token").get_secret_value() == "file-token"
    assert k.options == {"exchange": "NSE"}

    s2 = Settings.load(
        env=env,
        explicit={
            "providers": ["mock"],
            "provider": {"kite": {"credentials": {"api_key": SecretStr("explicit")}}},
        },
    )
    assert s2.providers == ("mock",)
    assert (
        s2.for_provider("kite").credential("api_key").get_secret_value() == "explicit"
    )


def test_missing_file_is_fine_and_default_path_is_under_user_config(
    tmp_path: Path,
) -> None:
    s = Settings.load(env={}, config_file=tmp_path / "nope.toml")
    assert s.providers == ()
    assert default_config_path().name == "config.toml"
    assert "ismet" in str(default_config_path())


def test_malformed_file_and_shapes_raise_config_error(tmp_path: Path) -> None:
    bad = tmp_path / "bad.toml"
    bad.write_text("providers = [\n")
    with pytest.raises(ConfigError, match="cannot read"):
        Settings.load(env={}, config_file=bad)
    with pytest.raises(ConfigError, match="list of provider names"):
        Settings.load(env={}, use_file=False, explicit={"providers": 3})
    with pytest.raises(ConfigError, match="table keyed"):
        Settings.load(env={}, use_file=False, explicit={"provider": []})
    with pytest.raises(ConfigError, match="must be a table"):
        Settings.load(env={}, use_file=False, explicit={"provider": {"x": 1}})
    with pytest.raises(ConfigError, match="unknown keys"):
        Settings.load(
            env={}, use_file=False, explicit={"provider": {"x": {"api_key": 1}}}
        )
    with pytest.raises(ConfigError, match="must be tables"):
        Settings.load(
            env={}, use_file=False, explicit={"provider": {"x": {"credentials": 1}}}
        )
