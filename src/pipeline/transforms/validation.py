"""Trade validation transforms (the business rules).

Version handling (rules 1-2) is stateful, keyed by trade_id; the malformed and
past-maturity checks are stateless. Expiry (rule 4) is handled by the Snowflake
sweep. The in-pipeline version state is only a first line of defence - the
Snowflake MERGE re-applies the same check, so a state-loss restart can't corrupt
the warehouse.
"""
import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.transforms.userstate import (
    ReadModifyWriteStateSpec,
    TimerSpec,
    on_timer,
)
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.coders import VarIntCoder
from apache_beam.utils.timestamp import Duration, Timestamp

from trade_schema import parse_trade, parse_maturity, TradeValidationError

log = logging.getLogger("trade-validation")

TAG_VALID = "valid"
TAG_REJECTED = "rejected"

# Rejection reasons - stable strings so dashboards/audit can group on them.
R_MALFORMED = "MALFORMED_PAYLOAD"
R_LOWER_VERSION = "LOWER_VERSION"
R_PAST_MATURITY = "PAST_MATURITY_AT_INGEST"

S_ACTIVE = "ACTIVE"
S_EXPIRED = "EXPIRED"

# Drop idle per-trade version state after a week so it doesn't grow forever.
_STATE_TTL = Duration(seconds=7 * 24 * 60 * 60)


def _today_utc():
    return datetime.now(timezone.utc).date()


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _reject(raw_payload, reason, detail, trade_id=None, version=None):
    return {
        "trade_id": trade_id,
        "version": version,
        "rejection_reason": reason,
        "rejection_detail": detail,
        "raw_payload": raw_payload,
        "rejected_at": _now_iso(),
    }


class ParseAndStaticValidate(beam.DoFn):
    """Parse the payload and apply the stateless checks (malformed, past maturity).

    Valid records are emitted as (trade_id, record) so the next step can key on
    trade_id for version resolution. Rejections go out on the rejected tag.
    """

    def setup(self):
        self._m_valid = Metrics.counter(self.__class__, "valid_trades")
        self._m_malformed = Metrics.counter(self.__class__, "malformed_trades")
        self._m_past_maturity = Metrics.counter(self.__class__, "past_maturity_trades")

    def process(self, element):
        raw = element.decode("utf-8") if isinstance(element, (bytes, bytearray)) else str(element)

        # Malformed -> reject (rule: log rejected trades for audit).
        try:
            trade = parse_trade(raw)
        except TradeValidationError as exc:
            self._m_malformed.inc()
            log.warning("rejecting malformed payload: %s", exc)
            yield beam.pvalue.TaggedOutput(TAG_REJECTED, _reject(raw, R_MALFORMED, str(exc)))
            return

        # Rule 3: reject a trade whose maturity is already in the past at ingest.
        maturity = parse_maturity(trade.__dict__)
        today = _today_utc()
        if maturity is not None and maturity < today:
            self._m_past_maturity.inc()
            detail = f"maturity {maturity.isoformat()} < today {today.isoformat()}"
            yield beam.pvalue.TaggedOutput(
                TAG_REJECTED,
                _reject(raw, R_PAST_MATURITY, detail, trade_id=trade.trade_id, version=trade.version),
            )
            return

        record = {
            "trade_id": trade.trade_id,
            "version": trade.version,
            "counter_party_id": trade.counter_party_id,
            "book_id": trade.book_id,
            "instrument": trade.instrument,
            "notional": trade.notional,
            "currency": trade.currency,
            "trade_date": trade.trade_date,
            "maturity_date": trade.maturity_date,
            "status": S_ACTIVE,
            "created_at": trade.created_at,
            "processed_at": _now_iso(),
            "_raw": raw,
        }
        self._m_valid.inc()
        yield beam.pvalue.TaggedOutput(TAG_VALID, (trade.trade_id, record))


class VersionResolve(beam.DoFn):
    """Version resolution per trade_id (rules 1 and 2).

    Keeps the highest version seen per trade: higher -> upgrade, equal -> replace,
    lower -> reject. Beam processes all elements for a key on one worker in order,
    so the stored "seen version" stays consistent. A wall-clock timer clears the
    state after a week of inactivity so it can't grow forever.
    """

    SEEN_VERSION = ReadModifyWriteStateSpec("seen_version", VarIntCoder())
    EXPIRY_TIMER = TimerSpec("expiry", TimeDomain.REAL_TIME)

    def setup(self):
        self._m_new = Metrics.counter(self.__class__, "new_trades")
        self._m_upgrade = Metrics.counter(self.__class__, "upgraded_trades")
        self._m_replace = Metrics.counter(self.__class__, "replaced_trades")
        self._m_lower = Metrics.counter(self.__class__, "lower_version_trades")

    def process(
        self,
        element,
        seen=beam.DoFn.StateParam(SEEN_VERSION),
        expiry=beam.DoFn.TimerParam(EXPIRY_TIMER),
    ):
        trade_id, record = element
        incoming = int(record["version"])
        current = seen.read()

        # REAL_TIME timer fires on wall-clock, so set it from now() not event time.
        expiry.set(Timestamp.now() + _STATE_TTL)

        if current is None or incoming > current:
            seen.write(incoming)
            record["version_action"] = "NEW" if current is None else "UPGRADE"
            (self._m_new if current is None else self._m_upgrade).inc()
            yield beam.pvalue.TaggedOutput(TAG_VALID, record)
        elif incoming == current:
            record["version_action"] = "REPLACE"
            self._m_replace.inc()
            yield beam.pvalue.TaggedOutput(TAG_VALID, record)
        else:
            self._m_lower.inc()
            detail = f"incoming v{incoming} < seen v{current}"
            yield beam.pvalue.TaggedOutput(
                TAG_REJECTED,
                _reject(
                    record.get("_raw", json.dumps(record)),
                    R_LOWER_VERSION, detail,
                    trade_id=trade_id, version=incoming,
                ),
            )

    @on_timer(EXPIRY_TIMER)
    def _expire_state(self, seen=beam.DoFn.StateParam(SEEN_VERSION)):
        seen.clear()


class StripInternalFields(beam.DoFn):
    """Drop the helper fields (prefixed with _) before the sink."""

    def process(self, record):
        yield {k: v for k, v in record.items() if not k.startswith("_")}
