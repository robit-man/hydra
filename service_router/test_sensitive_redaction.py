#!/usr/bin/env python3
"""Unit checks for API/config redaction helper behavior."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import router  # type: ignore


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> int:
    sample = {
        "service_relays": {
            "whisper_asr": {
                "seed_hex": "a" * 64,
                "name": "whisper-asr-relay-aaaaaaaa",
            }
        },
        "security": {
            "password": "super-secret",
            "api_key": "api-key-value",
            "require_auth": True,
        },
        "headers": {
            "authorization": "Bearer abc",
            "x-safe": "ok",
        },
        "nkn": {
            "seed_persisted": True,
            "addresses": ["node-1"],
        },
    }

    redacted = router._redact_sensitive_fields(sample)

    assert_true(redacted is not sample, "redaction must return a new object")
    assert_true(redacted["service_relays"]["whisper_asr"]["seed_hex"] == router.SENSITIVE_VALUE_PLACEHOLDER, "seed_hex not redacted")
    assert_true(redacted["security"]["password"] == router.SENSITIVE_VALUE_PLACEHOLDER, "password not redacted")
    assert_true(redacted["security"]["api_key"] == router.SENSITIVE_VALUE_PLACEHOLDER, "api_key not redacted")
    assert_true(redacted["headers"]["authorization"] == router.SENSITIVE_VALUE_PLACEHOLDER, "authorization not redacted")
    assert_true(redacted["security"]["require_auth"] is True, "non-secret auth metadata should remain")
    assert_true(redacted["nkn"]["seed_persisted"] is True, "seed_persisted should not be redacted")
    assert_true(sample["security"]["password"] == "super-secret", "original payload should remain unchanged")

    print("Sensitive redaction checks passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover
        print(f"Sensitive redaction checks failed: {exc}")
        raise
