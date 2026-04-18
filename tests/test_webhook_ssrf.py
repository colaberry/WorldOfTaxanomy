"""Tests for the SSRF allowlist on LEAD_WEBHOOK_URL."""

import pytest

from world_of_taxonomy.webhook import _validate_webhook_url


def test_https_public_url_allowed():
    assert _validate_webhook_url("https://hooks.example.com/webhook") is None


def test_empty_url_rejected():
    assert _validate_webhook_url("") is not None


def test_non_https_rejected():
    assert "scheme" in _validate_webhook_url(
        "file:///etc/passwd"
    )


def test_loopback_hostname_rejected():
    reason = _validate_webhook_url("https://localhost/webhook")
    assert reason is not None
    assert "internal" in reason


def test_loopback_ip_rejected():
    reason = _validate_webhook_url("https://127.0.0.1/webhook")
    assert reason is not None
    assert "internal" in reason


def test_private_rfc1918_rejected():
    reason = _validate_webhook_url("https://10.0.0.5/webhook")
    assert reason is not None
    assert "internal" in reason


def test_link_local_169_254_rejected():
    reason = _validate_webhook_url("https://169.254.169.254/latest/meta-data/")
    assert reason is not None
    assert "internal" in reason


def test_missing_host_rejected():
    reason = _validate_webhook_url("https:///path")
    assert reason is not None
    assert "host" in reason.lower()
