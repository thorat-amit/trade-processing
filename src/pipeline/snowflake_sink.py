"""Snowflake sink: MERGE valid trades into TRADES, append rejects to REJECTED_TRADES.

Uses the Snowflake connector directly (not SnowflakeIO) for MERGE/upsert. Each
batch stages into its own session TEMPORARY table and merges from there; both
writes are idempotent so a retried write can't duplicate. Credentials come from
Secret Manager at worker startup.

snowflake/cryptography/pandas are imported inside the functions that use them so
the deps ship to the workers, not the launch environment.
"""
import hashlib
import json
import logging
import time
import uuid

import apache_beam as beam

log = logging.getLogger("snowflake-sink")


def _get_secret(secret_resource):
    from google.cloud import secretmanager

    client = secretmanager.SecretManagerServiceClient()
    resp = client.access_secret_version(name=secret_resource)
    return json.loads(resp.payload.data.decode("utf-8"))


def _load_der_private_key(pem, passphrase=None):
    # The connector wants the private key as DER bytes, not a PEM string.
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    pem_bytes = pem.encode("utf-8") if isinstance(pem, str) else pem
    pwd = passphrase.encode("utf-8") if isinstance(passphrase, str) and passphrase else None
    key = serialization.load_pem_private_key(pem_bytes, password=pwd, backend=default_backend())
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _retry_with_backoff(fn, max_attempts=3, base_delay=1.0):
    """Retry fn on transient connection errors only, with backoff.

    DatabaseError is excluded on purpose - it's the base for things like bad SQL,
    which retrying would never fix.
    """
    from snowflake.connector import errors

    transient = (errors.OperationalError, errors.InterfaceError)
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except transient as exc:
            if attempt == max_attempts:
                raise
            delay = base_delay * (2 ** (attempt - 1))
            log.warning("snowflake error (%s), retry %d in %.0fs", type(exc).__name__, attempt, delay)
            time.sleep(delay)


class _SnowflakeConnection:
    """Opens one connection per DoFn instance and reuses it across batches."""

    def __init__(self, secret_resource):
        self._secret_resource = secret_resource
        self._conn = None
        self._cfg = None

    def cfg(self):
        if self._cfg is None:
            self._cfg = _get_secret(self._secret_resource)
        return self._cfg

    def connect(self):
        if self._conn is not None:
            return self._conn
        import snowflake.connector

        cfg = self.cfg()
        kwargs = dict(
            account=cfg["account"],
            user=cfg["user"],
            role=cfg.get("role"),
            warehouse=cfg.get("warehouse", "TRADE_WH"),
            database=cfg.get("database", "TRADE_DB"),
            schema=cfg.get("schema", "PUBLIC"),
            client_session_keep_alive=True,
            network_timeout=30,
            login_timeout=30,
        )
        if cfg.get("private_key"):
            kwargs["private_key"] = _load_der_private_key(cfg["private_key"], cfg.get("private_key_passphrase"))
        else:
            kwargs["password"] = cfg["password"]

        self._conn = snowflake.connector.connect(**kwargs)
        return self._conn

    def close(self):
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None


def _rows_to_dataframe(rows, columns):
    import pandas as pd
    return pd.DataFrame([[r.get(c) for c in columns] for r in rows], columns=columns)


def _stage_load_merge(conn, stage, create_sql, df, merge_sql):
    """Load df into a fresh temp table and MERGE it into the target.

    create temp table -> bulk load -> MERGE -> commit, with rollback and
    temp-table cleanup on the way out.
    """
    from snowflake.connector.pandas_tools import write_pandas

    cur = conn.cursor()
    try:
        cur.execute(create_sql)
        write_pandas(conn, df, stage, quote_identifiers=False, overwrite=False)
        cur.execute(merge_sql)
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            cur.execute(f"DROP TABLE IF EXISTS {stage}")
        except Exception:
            pass
        try:
            cur.close()
        except Exception:
            pass


class _SnowflakeWriter(beam.DoFn):
    """Connection lifecycle shared by both writer DoFns.

    Subclasses implement _write_batch(rows) for their own table.
    """

    def __init__(self, secret_resource):
        self._secret_resource = secret_resource
        self._sf = None

    def setup(self):
        self._sf = _SnowflakeConnection(self._secret_resource)
        self._sf.connect()

    def process(self, batch):
        rows = list(batch)
        if not rows:
            return
        _retry_with_backoff(lambda: self._write_batch(rows))
        yield len(rows)

    def teardown(self):
        if self._sf:
            self._sf.close()

    def _write_batch(self, rows):
        raise NotImplementedError


# --- Valid trades: upsert into TRADES -------------------------------------

_VALID_COLS = [
    "trade_id", "version", "counter_party_id", "book_id", "instrument",
    "notional", "currency", "trade_date", "maturity_date", "status",
    "version_action", "created_at", "processed_at",
]

_VALID_STAGE_DDL = """CREATE TEMPORARY TABLE {stage} (
    trade_id STRING, version NUMBER(10,0), counter_party_id STRING,
    book_id STRING, instrument STRING, notional NUMBER(38,2), currency STRING,
    trade_date DATE, maturity_date DATE, status STRING,
    version_action STRING, created_at TIMESTAMP_NTZ,
    processed_at TIMESTAMP_NTZ
)"""

# Dedupe the source to one row per trade_id (highest version) inside the MERGE,
# otherwise a batch with several versions of the same trade hits Snowflake's
# non-deterministic MERGE error. Only overwrite when incoming version >= stored.
_VALID_MERGE_TMPL = """
MERGE INTO TRADES t
USING (
    SELECT {cols} FROM {stage}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY trade_id ORDER BY version DESC) = 1
) s
  ON t.trade_id = s.trade_id
WHEN MATCHED AND s.version >= t.version THEN UPDATE SET
    t.version          = s.version,
    t.counter_party_id = s.counter_party_id,
    t.book_id          = s.book_id,
    t.instrument       = s.instrument,
    t.notional         = s.notional,
    t.currency         = s.currency,
    t.trade_date       = s.trade_date,
    t.maturity_date    = s.maturity_date,
    t.status           = s.status,
    t.version_action   = s.version_action,
    t.created_at       = s.created_at,
    t.processed_at     = s.processed_at,
    t.loaded_at        = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    {cols}, loaded_at
) VALUES (
    {s_cols}, CURRENT_TIMESTAMP()
);
"""


def _build_merge_sql(stage):
    cols = ", ".join(_VALID_COLS)
    s_cols = ", ".join(f"s.{c}" for c in _VALID_COLS)
    return _VALID_MERGE_TMPL.format(stage=stage, cols=cols, s_cols=s_cols)


class WriteValidTradesToSnowflake(_SnowflakeWriter):
    """Upsert a batch of valid trades into TRADES via a temp table + MERGE."""

    def _write_batch(self, rows):
        conn = self._sf.connect()
        stage = f"TRADES_STAGE_{uuid.uuid4().hex[:12]}".upper()
        merge_sql = _build_merge_sql(stage)
        df = _rows_to_dataframe(rows, _VALID_COLS)
        try:
            _stage_load_merge(conn, stage, _VALID_STAGE_DDL.format(stage=stage), df, merge_sql)
        except Exception:
            log.exception("failed to merge valid batch via %s", stage)
            self._sf.close()  # drop the connection so a retry reconnects
            raise
        log.info("merged %d valid trades via %s", len(rows), stage)


# --- Rejected trades: idempotent append into REJECTED_TRADES ---------------

_REJECT_COLS = [
    "reject_id", "trade_id", "version", "rejection_reason",
    "rejection_detail", "raw_payload", "rejected_at",
]

_REJECT_STAGE_DDL = """CREATE TEMPORARY TABLE {stage} (
    reject_id STRING, trade_id STRING, version NUMBER(10,0),
    rejection_reason STRING, rejection_detail STRING,
    raw_payload STRING, rejected_at TIMESTAMP_NTZ
)"""

# Insert-only MERGE keyed on a content hash, so re-running a batch after a lost
# commit can't duplicate audit rows. Dedupe within the batch too, in case the
# same payload was rejected more than once.
_REJECT_MERGE_TMPL = """
MERGE INTO REJECTED_TRADES t
USING (
    SELECT {cols} FROM {stage}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY reject_id ORDER BY rejected_at) = 1
) s
  ON t.reject_id = s.reject_id
WHEN NOT MATCHED THEN INSERT (
    {cols}, loaded_at
) VALUES (
    {s_cols}, CURRENT_TIMESTAMP()
);
"""


def _build_reject_merge_sql(stage):
    cols = ", ".join(_REJECT_COLS)
    s_cols = ", ".join(f"s.{c}" for c in _REJECT_COLS)
    return _REJECT_MERGE_TMPL.format(stage=stage, cols=cols, s_cols=s_cols)


def _reject_id(row):
    """Stable id derived from the rejection's content.

    rejected_at is stamped once when the trade is rejected (not at write time),
    so the same rejection always hashes to the same id - that's what makes the
    MERGE idempotent across retries.
    """
    parts = [
        row.get("raw_payload"),
        row.get("rejection_reason"),
        row.get("rejection_detail"),
        row.get("rejected_at"),
    ]
    blob = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


class WriteRejectedTradesToSnowflake(_SnowflakeWriter):
    """Append a batch of rejected trades into REJECTED_TRADES, idempotently."""

    def _write_batch(self, rows):
        rows = [dict(r, reject_id=_reject_id(r)) for r in rows]
        conn = self._sf.connect()
        stage = f"REJECTS_STAGE_{uuid.uuid4().hex[:12]}".upper()
        merge_sql = _build_reject_merge_sql(stage)
        df = _rows_to_dataframe(rows, _REJECT_COLS)
        try:
            _stage_load_merge(conn, stage, _REJECT_STAGE_DDL.format(stage=stage), df, merge_sql)
        except Exception:
            log.exception("failed to merge rejected batch via %s", stage)
            self._sf.close()
            raise
        log.info("merged %d rejected trades via %s", len(rows), stage)
