-- Snowflake schema for the trade ETL pipeline. Run once during setup.
-- Defaults match the Terraform variables and the pipeline config.

-- XSMALL + auto-suspend: streaming micro-batches are tiny, so this is plenty
-- and costs nothing while idle.
CREATE WAREHOUSE IF NOT EXISTS TRADE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Warehouse for trade ETL ingestion and reporting';

CREATE DATABASE IF NOT EXISTS TRADE_DB COMMENT = 'Trade data warehouse';
CREATE SCHEMA IF NOT EXISTS TRADE_DB.PUBLIC;

USE DATABASE TRADE_DB;
USE SCHEMA PUBLIC;

-- Valid trades: current state, one row per trade_id. The pipeline MERGEs into
-- this so it always holds the latest accepted version.
CREATE TABLE IF NOT EXISTS TRADES (
    trade_id          STRING        NOT NULL,
    version           NUMBER(10,0)  NOT NULL,
    counter_party_id  STRING,
    book_id           STRING,
    instrument        STRING,
    notional          NUMBER(38,2),
    currency          STRING,
    trade_date        DATE,
    maturity_date     DATE          NOT NULL,
    status            STRING        NOT NULL,   -- ACTIVE | EXPIRED
    version_action    STRING,                   -- NEW | UPGRADE | REPLACE
    created_at        TIMESTAMP_NTZ,
    processed_at      TIMESTAMP_NTZ,
    loaded_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_trades PRIMARY KEY (trade_id)
);

-- Rejected trades: append-only audit log for compliance.
CREATE TABLE IF NOT EXISTS REJECTED_TRADES (
    reject_id         STRING DEFAULT UUID_STRING(),
    trade_id          STRING,
    version           NUMBER(10,0),
    rejection_reason  STRING NOT NULL,   -- MALFORMED_PAYLOAD | LOWER_VERSION | PAST_MATURITY_AT_INGEST
    rejection_detail  STRING,
    raw_payload       STRING,            -- original message, kept for replay
    rejected_at       TIMESTAMP_NTZ,
    loaded_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Run audit, written by the Airflow DAG.
CREATE TABLE IF NOT EXISTS PIPELINE_AUDIT (
    run_id            STRING,
    task_id           STRING,
    event             STRING,            -- EXPIRY_SWEEP, HEALTH_CHECK, ...
    rows_affected     NUMBER,
    detail            STRING,
    event_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- The streaming writer creates its own session-scoped TEMPORARY staging table
-- per micro-batch and MERGEs from it, so no persistent staging table is needed.

-- Reporting views for the dashboard.
CREATE OR REPLACE VIEW V_TRADE_STATUS_SUMMARY AS
SELECT
    status,
    COUNT(*)            AS trade_count,
    SUM(notional)       AS total_notional,
    MIN(maturity_date)  AS earliest_maturity,
    MAX(maturity_date)  AS latest_maturity
FROM TRADES
GROUP BY status;

CREATE OR REPLACE VIEW V_REJECTION_SUMMARY AS
SELECT
    rejection_reason,
    COUNT(*)            AS reject_count,
    MAX(rejected_at)    AS last_seen
FROM REJECTED_TRADES
GROUP BY rejection_reason;
