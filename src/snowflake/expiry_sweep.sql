-- Rule 4: flip matured trades to EXPIRED. Idempotent - only touches ACTIVE rows
-- whose maturity is already past, so it's safe to run on any schedule.
UPDATE TRADES
SET status = 'EXPIRED',
    loaded_at = CURRENT_TIMESTAMP()
WHERE status = 'ACTIVE'
  AND maturity_date < CURRENT_DATE();
