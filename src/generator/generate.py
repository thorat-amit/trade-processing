"""Publish mock trade events to Pub/Sub for testing the ETL pipeline.

Each --scenario targets a validation rule so you can exercise the pipeline:
  valid          well-formed trades, all should be accepted
  past_maturity  maturity date in the past, should be rejected
  versions       same trade_id republished with different versions
  mixed          a realistic blend of the above (default)

Example:
    python -m generator.generate --project_id my-proj --topic_id trades-ingest \
        --num_trades 200 --rate_per_sec 25 --scenario mixed
"""
import argparse
import logging
import random
import time
from datetime import date, timedelta

from google.cloud import pubsub_v1

from trade_schema import TradeEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s | %(message)s")
log = logging.getLogger("trade-generator")

INSTRUMENTS = ["IRS", "FX_FWD", "CDS", "EQ_SWAP", "FX_OPT", "BOND_FUT"]
CURRENCIES = ["USD", "EUR", "GBP", "JPY", "INR"]
COUNTERPARTIES = [f"CP-{i:03d}" for i in range(1, 41)]
BOOKS = ["BOOK-NY", "BOOK-LDN", "BOOK-TKO", "BOOK-SGP", "BOOK-MUM"]


def make_trade(trade_id, version, maturity=None):
    """Build one trade event. Defaults to a random maturity well in the future."""
    if maturity is None:
        maturity = date.today() + timedelta(days=random.randint(30, 3650))
    return TradeEvent(
        trade_id=trade_id,
        version=version,
        counter_party_id=random.choice(COUNTERPARTIES),
        book_id=random.choice(BOOKS),
        instrument=random.choice(INSTRUMENTS),
        maturity_date=maturity.isoformat(),
        trade_date=date.today().isoformat(),
        notional=round(random.uniform(1e5, 5e8), 2),
        currency=random.choice(CURRENCIES),
    )


def make_past_maturity_trade(trade_id):
    """A trade that already matured - the pipeline should reject it."""
    matured = date.today() - timedelta(days=random.randint(1, 365))
    return make_trade(trade_id, version=1, maturity=matured)


def make_versioned_trade(trade_id):
    """A trade at a random version, to trigger the version rules."""
    return make_trade(trade_id, version=random.choice([1, 2, 3]))


def build_events(num_trades, scenario):
    """Generate the list of trade events for the chosen scenario.

    trade_ids come from a small pool (~10% of the total) so the same id shows up
    repeatedly with different versions - that's what exercises the version rules.
    """
    id_pool = max(1, num_trades // 10)
    events = []

    for _ in range(num_trades):
        trade_id = f"T-{random.randint(1, id_pool):05d}"

        if scenario == "valid":
            events.append(make_trade(trade_id, version=1))
        elif scenario == "past_maturity":
            events.append(make_past_maturity_trade(trade_id))
        elif scenario == "versions":
            events.append(make_versioned_trade(trade_id))
        else:  # mixed: ~10% past maturity, ~35% versioned, the rest plain valid
            roll = random.random()
            if roll < 0.10:
                events.append(make_past_maturity_trade(trade_id))
            elif roll < 0.45:
                events.append(make_versioned_trade(trade_id))
            else:
                events.append(make_trade(trade_id, version=1))

    return events


def publish(project_id, topic_id, num_trades, rate_per_sec, scenario):
    """Publish the generated events and return how many failed."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    events = build_events(num_trades, scenario)
    log.info("publishing %d events to %s (scenario=%s)", len(events), topic_path, scenario)

    delay = 1.0 / rate_per_sec if rate_per_sec > 0 else 0
    pending = []
    for event in events:
        pending.append(publisher.publish(topic_path, event.to_bytes()))
        if delay:
            time.sleep(delay)

    # Block until every publish finishes; result() re-raises any publish error.
    failures = 0
    for future in pending:
        try:
            future.result(timeout=60)
        except Exception as exc:
            failures += 1
            log.error("publish failed: %s", exc)

    log.info("done: %d published, %d failed", len(events) - failures, failures)
    return failures


def parse_args():
    parser = argparse.ArgumentParser(description="Publish mock trade events to Pub/Sub")
    parser.add_argument("--project_id", required=True)
    parser.add_argument("--topic_id", required=True)
    parser.add_argument("--num_trades", type=int, default=200)
    parser.add_argument("--rate_per_sec", type=float, default=25.0)
    parser.add_argument("--scenario", choices=["mixed", "valid", "past_maturity", "versions"], default="mixed")
    parser.add_argument("--seed", type=int, help="seed the RNG for reproducible runs")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    failures = publish(args.project_id, args.topic_id, args.num_trades, args.rate_per_sec, args.scenario)
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
