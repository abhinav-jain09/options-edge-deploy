-- Dedicated least-privilege Postgres role for the pre-market reset (P3).
--
-- This role can do EXACTLY ONE destructive thing: TRUNCATE the explicit
-- intraday allow-list below, in the options_flow database only. It is granted
-- NOTHING on pin_session_close (the cross-day realized-close label table, which
-- must be preserved), and it cannot reach the Keycloak database (a separate
-- Postgres instance / role entirely). Truncating the wrong thing is impossible
-- by construction, not just by convention.
--
-- Apply once, on the options_flow database, as a superuser/owner:
--   psql "host=... dbname=options_flow user=postgres" \
--        -v reset_pwd="$(openssl rand -base64 24)" \
--        -f scripts/ops/premarket-reset-role.sql
-- Capture the password into the Jenkins credential / k8s secret the reset uses.
--
-- Idempotent: safe to re-run (role create guarded; grants are additive).

\set ON_ERROR_STOP on

-- 1. Role (login). Password supplied via -v reset_pwd=... (never hard-coded here).
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'premarket_reset') THEN
    CREATE ROLE premarket_reset LOGIN;
  END IF;
END$$;
ALTER ROLE premarket_reset WITH PASSWORD :'reset_pwd';

-- 2. Connect to THIS database only; usage on the data schema.
GRANT CONNECT ON DATABASE options_flow TO premarket_reset;
GRANT USAGE   ON SCHEMA public        TO premarket_reset;

-- 3. TRUNCATE grant on the explicit intraday allow-list ONLY.
--    pin_session_close is deliberately ABSENT (preserve realized-close labels).
GRANT TRUNCATE ON
    public.hpsf_signal,
    public.hpsf_latest_signal,
    public.hpsf_market_flow,
    public.hpsf_strike_score,
    public.hpsf_strike_flow,
    public.hpsf_exit_signal,
    public.hpsf_dlq,
    public.hpsf_writer_dlq,
    public.pin_strike_minute,
    public.pin_uw_crosscheck_minute,
    public.pin_self_gex_minute,
    public.pin_market_minute,
    public.pin_strike_fine,
    public.databento_option_raw_snapshot,
    public.databento_option_raw_quarantine
  TO premarket_reset;

-- 4. Audit table in a SEPARATE schema so it survives the wipe it records, and is
--    out of reach of any TRUNCATE grant above. The reset writes a pre-intent row
--    then updates it to done/failed.
CREATE SCHEMA IF NOT EXISTS premarket;
GRANT USAGE ON SCHEMA premarket TO premarket_reset;
CREATE TABLE IF NOT EXISTS premarket.reset_audit (
    id            bigserial primary key,
    trade_date    date        not null,
    started_at    timestamptz not null default now(),
    finished_at   timestamptz,
    status        text        not null default 'INTENT'
                  check (status in ('INTENT','DONE','FAILED')),
    topics_purged int,
    tables_truncated int,
    detail        text
);
GRANT INSERT, UPDATE, SELECT ON premarket.reset_audit TO premarket_reset;
GRANT USAGE, SELECT ON SEQUENCE premarket.reset_audit_id_seq TO premarket_reset;

-- 5. Belt-and-braces: ensure the role can NEVER truncate the preserved label table.
REVOKE TRUNCATE ON public.pin_session_close FROM premarket_reset;
