-- Per-attempt lease token (dispatch hardening): incremented on every
-- LeaseBatch so an in-flight sender whose lease was reclaimed and re-leased
-- can never mutate a newer attempt's outcome. Every attempt mutation
-- (MarkWireStarted, MigrateRecipient, CompleteAttempt, ReleaseLease) guards on
-- this token in addition to state='leased'.
ALTER TABLE recipients ADD COLUMN lease_token INTEGER NOT NULL DEFAULT 0;
