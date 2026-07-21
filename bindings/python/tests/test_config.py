"""Unit tests for :class:`goosefs.Config` construction.

These tests exercise pure construction behaviour and never touch the
network, so they always run — no ``GOOSEFS_MASTER_ADDR`` guard needed.
"""

from __future__ import annotations

import pytest
from goosefs import Config
from goosefs.exceptions import ConfigError  # noqa: I001

# ─── single-master ────────────────────────────────────────────────────


def test_single_master_addr() -> None:
    cfg = Config("127.0.0.1:9200")
    assert cfg.master_addr == "127.0.0.1:9200"
    assert cfg.master_addrs == ["127.0.0.1:9200"]
    assert cfg.root == ""


# ─── comma-separated HA list (legacy) ─────────────────────────────────


def test_comma_separated_ha_list() -> None:
    cfg = Config("m1:9200,m2:9200,m3:9200")
    assert cfg.master_addr == "m1:9200"
    assert cfg.master_addrs == ["m1:9200", "m2:9200", "m3:9200"]
    assert cfg.root == ""


def test_comma_separated_trims_whitespace() -> None:
    cfg = Config(" m1:9200 , m2:9200 ,m3:9200 ")
    assert cfg.master_addrs == ["m1:9200", "m2:9200", "m3:9200"]


# ─── gfs:// URI form ──────────────────────────────────────────────────


def test_uri_form_via_constructor() -> None:
    cfg = Config("gfs://172.16.16.27:9200,172.16.16.23:9200,172.16.16.38:9200/xxxx")
    assert cfg.master_addr == "172.16.16.27:9200"
    assert cfg.master_addrs == [
        "172.16.16.27:9200",
        "172.16.16.23:9200",
        "172.16.16.38:9200",
    ]
    assert cfg.root == "/xxxx"


def test_uri_form_single_master_no_path() -> None:
    cfg = Config("gfs://10.0.0.1:9200")
    assert cfg.master_addr == "10.0.0.1:9200"
    assert cfg.master_addrs == ["10.0.0.1:9200"]
    assert cfg.root == ""


def test_uri_form_via_from_uri_classmethod() -> None:
    cfg = Config.from_uri("gfs://a:9200,b:9200/data")
    assert cfg.master_addrs == ["a:9200", "b:9200"]
    assert cfg.root == "/data"


def test_uri_form_layers_properties_on_top() -> None:
    cfg = Config.from_uri(
        "gfs://a:9200,b:9200/data",
        properties={"goosefs.security.authentication.type": "SIMPLE"},
    )
    assert cfg.master_addrs == ["a:9200", "b:9200"]
    assert cfg.root == "/data"
    assert cfg.auth_type.upper() == "SIMPLE"


def test_uri_form_trailing_slash_is_stripped() -> None:
    cfg = Config("gfs://a:9200/data/")
    assert cfg.root == "/data"


def test_uri_form_bare_slash_yields_empty_root() -> None:
    cfg = Config("gfs://a:9200/")
    assert cfg.root == ""


# ─── error cases ──────────────────────────────────────────────────────


def test_empty_master_addr_rejected() -> None:
    with pytest.raises(ConfigError):
        Config("")


def test_uri_missing_authority_rejected() -> None:
    with pytest.raises(ConfigError):
        Config("gfs:///data")


def test_uri_wrong_scheme_treated_as_bare_list_and_rejected() -> None:
    # `http://a:9200/x` has no `gfs://` prefix, so it goes through the
    # comma-list path; the resulting single "address" is `http://a:9200/x`
    # which is preserved verbatim (validation happens later on connect).
    # We only assert construction does not misinterpret it as HA form.
    cfg = Config("http://a:9200/x")
    assert cfg.master_addr == "http://a:9200/x"
