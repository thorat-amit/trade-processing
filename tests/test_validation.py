"""Unit tests for the trade validation rules.

Run with: pytest tests/ -v
Everything runs on the DirectRunner with in-memory data, so no GCP or Snowflake
connection is needed.
"""
import sys
import os
from datetime import date, timedelta

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src", "pipeline"))
sys.path.insert(0, os.path.join(ROOT, "src", "pipeline", "transforms"))

import apache_beam as beam  # noqa: E402
from apache_beam.testing.test_pipeline import TestPipeline  # noqa: E402
from apache_beam.testing.util import assert_that, equal_to  # noqa: E402

from transforms.validation import (  # noqa: E402
    ParseAndStaticValidate,
    VersionResolve,
    TAG_VALID,
    TAG_REJECTED,
    R_MALFORMED,
    R_PAST_MATURITY,
    R_LOWER_VERSION,
)
from trade_schema import TradeEvent  # noqa: E402


def _trade(trade_id, version, maturity_offset_days=365):
    maturity = (date.today() + timedelta(days=maturity_offset_days)).isoformat()
    return TradeEvent(
        trade_id=trade_id, version=version, counter_party_id="CP-001",
        book_id="BOOK-NY", instrument="IRS", maturity_date=maturity,
    ).to_bytes()


def test_malformed_payload_is_rejected():
    with TestPipeline() as p:
        out = (
            p | beam.Create([b"{not valid json"])
            | beam.ParDo(ParseAndStaticValidate()).with_outputs(TAG_REJECTED, TAG_VALID)
        )
        reasons = out[TAG_REJECTED] | beam.Map(lambda r: r["rejection_reason"])
        assert_that(reasons, equal_to([R_MALFORMED]))


def test_past_maturity_is_rejected():
    with TestPipeline() as p:
        out = (
            p | beam.Create([_trade("T-1", 1, maturity_offset_days=-5)])
            | beam.ParDo(ParseAndStaticValidate()).with_outputs(TAG_REJECTED, TAG_VALID)
        )
        reasons = out[TAG_REJECTED] | beam.Map(lambda r: r["rejection_reason"])
        assert_that(reasons, equal_to([R_PAST_MATURITY]))


def test_future_maturity_is_valid():
    with TestPipeline() as p:
        out = (
            p | beam.Create([_trade("T-1", 1, maturity_offset_days=30)])
            | beam.ParDo(ParseAndStaticValidate()).with_outputs(TAG_REJECTED, TAG_VALID)
        )
        ids = out[TAG_VALID] | beam.Map(lambda kv: kv[0])
        assert_that(ids, equal_to(["T-1"]))


def _valid_tuple(trade_id, version):
    return (trade_id, {"trade_id": trade_id, "version": version, "_raw": "{}"})


def test_version_upgrade_accepted():
    with TestPipeline() as p:
        out = (
            p | beam.Create([_valid_tuple("T-1", 1), _valid_tuple("T-1", 2)])
            | beam.ParDo(VersionResolve()).with_outputs(TAG_REJECTED, TAG_VALID)
        )
        actions = out[TAG_VALID] | beam.Map(lambda r: r["version_action"])
        assert_that(actions, equal_to(["NEW", "UPGRADE"]))


def test_same_version_replaced():
    with TestPipeline() as p:
        out = (
            p | beam.Create([_valid_tuple("T-1", 2), _valid_tuple("T-1", 2)])
            | beam.ParDo(VersionResolve()).with_outputs(TAG_REJECTED, TAG_VALID)
        )
        actions = out[TAG_VALID] | beam.Map(lambda r: r["version_action"])
        assert_that(actions, equal_to(["NEW", "REPLACE"]))


def test_lower_version_rejected():
    with TestPipeline() as p:
        out = (
            p | beam.Create([_valid_tuple("T-1", 3), _valid_tuple("T-1", 1)])
            | beam.ParDo(VersionResolve()).with_outputs(TAG_REJECTED, TAG_VALID)
        )
        reasons = out[TAG_REJECTED] | beam.Map(lambda r: r["rejection_reason"])
        assert_that(reasons, equal_to([R_LOWER_VERSION]))


def test_merge_sql_dedupes_per_trade_id():
    # A batch with several versions of one trade_id must dedupe to the highest,
    # or the MERGE hits Snowflake's non-deterministic match error.
    sys.path.insert(0, os.path.join(ROOT, "src", "pipeline"))
    import importlib
    sink = importlib.import_module("snowflake_sink")
    sql = sink._build_merge_sql("MY_STAGE")
    assert "QUALIFY ROW_NUMBER() OVER (PARTITION BY trade_id ORDER BY version DESC) = 1" in sql
    assert "s.version >= t.version" in sql
    assert "MY_STAGE" in sql
    assert "TRADES_STAGING" not in sql


def test_private_key_pem_is_converted_to_der_bytes():
    # The connector needs DER bytes, not a PEM string.
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    sys.path.insert(0, os.path.join(ROOT, "src", "pipeline"))
    import importlib
    sink = importlib.import_module("snowflake_sink")

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")

    der = sink._load_der_private_key(pem)
    assert isinstance(der, bytes)
    reloaded = serialization.load_der_private_key(der, password=None)
    assert reloaded.key_size == 2048
    assert der[:1] != b"-"


def _sink():
    sys.path.insert(0, os.path.join(ROOT, "src", "pipeline"))
    import importlib
    return importlib.import_module("snowflake_sink")


def test_reject_id_is_deterministic():
    # Same rejection content -> same id (so a retried write can't duplicate the
    # audit row); different content -> different id.
    sink = _sink()
    row = {
        "raw_payload": '{"x":1}', "rejection_reason": R_MALFORMED,
        "rejection_detail": "bad json", "rejected_at": "2026-06-02T10:00:00+00:00",
    }
    assert sink._reject_id(row) == sink._reject_id(dict(row))
    assert sink._reject_id(row) != sink._reject_id(dict(row, rejection_detail="other"))


def test_reject_merge_is_insert_only_and_keyed_on_reject_id():
    sink = _sink()
    sql = sink._build_reject_merge_sql("RS")
    assert "ON t.reject_id = s.reject_id" in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql
    assert "UPDATE SET" not in sql            # append-only audit semantics
    assert "PARTITION BY reject_id" in sql    # dedupe within the batch too


def test_retry_does_not_retry_non_transient_errors():
    # A non-transient error (e.g. bad SQL) must surface immediately, not be
    # retried three times.
    sink = _sink()
    from snowflake.connector import errors

    calls = {"n": 0}

    def boom():
        calls["n"] += 1
        raise errors.ProgrammingError("syntax error")

    with pytest.raises(errors.ProgrammingError):
        sink._retry_with_backoff(boom, base_delay=0)
    assert calls["n"] == 1


def test_retry_recovers_from_transient_error():
    sink = _sink()
    from snowflake.connector import errors

    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise errors.OperationalError("warehouse resuming")
        return "ok"

    assert sink._retry_with_backoff(flaky, base_delay=0) == "ok"
    assert calls["n"] == 3


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
