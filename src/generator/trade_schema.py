"""Trade-event schema and JSON (de)serialization, shared by generator and pipeline.

Parsing is strict on purpose: bad input raises TradeValidationError so the
pipeline routes it to the reject path instead of letting bad data through.
"""
import json
from dataclasses import dataclass, asdict, field
from datetime import date, datetime, timezone

DATE_FMT = "%Y-%m-%d"

# A trade is unusable without these. Everything else has a sensible default.
REQUIRED_FIELDS = ("trade_id", "version", "counter_party_id", "book_id", "instrument", "maturity_date")


@dataclass
class TradeEvent:
    # Dates are ISO strings ("YYYY-MM-DD") because JSON has no date type.
    trade_id: str
    version: int
    counter_party_id: str
    book_id: str
    instrument: str
    maturity_date: str
    trade_date: str = field(default_factory=lambda: date.today().isoformat())
    notional: float = 0.0
    currency: str = "USD"
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_json(self):
        return json.dumps(asdict(self), separators=(",", ":"))

    def to_bytes(self):
        return self.to_json().encode("utf-8")


class TradeValidationError(ValueError):
    """Raised when a payload can't be parsed into a valid TradeEvent."""


def parse_trade(raw):
    """Parse bytes, a JSON string, or a dict into a validated TradeEvent."""
    # Normalize to a dict, whatever form the payload arrived in.
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise TradeValidationError(f"invalid JSON: {exc}") from exc
    elif isinstance(raw, dict):
        data = raw
    else:
        raise TradeValidationError(f"unsupported payload type: {type(raw).__name__}")

    # Required fields must be present and non-empty.
    missing = [name for name in REQUIRED_FIELDS if data.get(name) in (None, "")]
    if missing:
        raise TradeValidationError(f"missing required fields: {missing}")

    # Version must be a whole number >= 1.
    try:
        version = int(data["version"])
    except (TypeError, ValueError) as exc:
        raise TradeValidationError(f"version not an integer: {data['version']!r}") from exc
    if version < 1:
        raise TradeValidationError(f"version must be >= 1, got {version}")

    # Maturity date must be YYYY-MM-DD.
    try:
        datetime.strptime(str(data["maturity_date"]), DATE_FMT)
    except ValueError as exc:
        raise TradeValidationError(f"maturity_date must be {DATE_FMT}: {data['maturity_date']!r}") from exc

    # Build the trade, coercing types and filling optional defaults.
    return TradeEvent(
        trade_id=str(data["trade_id"]),
        version=version,
        counter_party_id=str(data["counter_party_id"]),
        book_id=str(data["book_id"]),
        instrument=str(data["instrument"]),
        maturity_date=str(data["maturity_date"]),
        trade_date=str(data.get("trade_date", date.today().isoformat())),
        notional=float(data.get("notional", 0.0)),
        currency=str(data.get("currency", "USD")),
        created_at=str(data.get("created_at", datetime.now(timezone.utc).isoformat())),
    )


def parse_maturity(trade_like):
    """Return the maturity as a date, or None if it's missing or unparseable."""
    try:
        return datetime.strptime(str(trade_like["maturity_date"]), DATE_FMT).date()
    except (KeyError, ValueError, TypeError):
        return None
