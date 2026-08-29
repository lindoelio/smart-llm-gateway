-- Canonical PostgreSQL schema. SQLite uses a dialect-specific equivalent in its
-- adapter; both schemas expose the same logical entities and constraints.
CREATE SCHEMA IF NOT EXISTS smart_llm_gateway;
SET search_path TO smart_llm_gateway;

CREATE TABLE gateway_keys (
  id TEXT PRIMARY KEY,
  key_hash TEXT NOT NULL UNIQUE,
  description TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE logical_models (name TEXT PRIMARY KEY, enabled BOOLEAN NOT NULL DEFAULT TRUE);

CREATE TABLE provider_accounts (
  id TEXT PRIMARY KEY,
  provider TEXT NOT NULL,
  credential_ref TEXT NOT NULL,
  base_url TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  CONSTRAINT provider_accounts_credential_ref_env_check
    CHECK (credential_ref ~ '^env:[A-Za-z_][A-Za-z0-9_]*$')
);

CREATE TABLE provider_routes (
  id TEXT PRIMARY KEY,
  logical_model TEXT NOT NULL REFERENCES logical_models(name),
  provider_account_id TEXT NOT NULL REFERENCES provider_accounts(id),
  upstream_model TEXT NOT NULL,
  priority INTEGER NOT NULL CHECK (priority >= 0),
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  CONSTRAINT provider_routes_model_priority_unique UNIQUE (logical_model, priority)
);

CREATE TABLE model_fallbacks (
  source_model TEXT NOT NULL REFERENCES logical_models(name),
  target_model TEXT NOT NULL REFERENCES logical_models(name),
  priority INTEGER NOT NULL CHECK (priority >= 0),
  CONSTRAINT model_fallbacks_source_priority_unique UNIQUE (source_model, priority)
);

CREATE TABLE provider_route_state (
  route_id TEXT PRIMARY KEY REFERENCES provider_routes(id),
  state TEXT NOT NULL DEFAULT 'closed' CHECK (state IN ('closed', 'open', 'half_open')),
  reason TEXT,
  retry_at BIGINT
);

CREATE TABLE provider_route_probe_leases (
  route_id TEXT PRIMARY KEY REFERENCES provider_routes(id),
  lease_id TEXT NOT NULL,
  expires_at BIGINT NOT NULL
);

CREATE TABLE provider_account_state (
  account_id TEXT PRIMARY KEY REFERENCES provider_accounts(id),
  state TEXT NOT NULL DEFAULT 'unknown' CHECK (state IN ('unknown', 'available', 'blocked')),
  reason TEXT,
  retry_at BIGINT
);

CREATE TABLE usage_attempts (
  id TEXT PRIMARY KEY,
  request_id TEXT NOT NULL,
  route_id TEXT NOT NULL,
  outcome TEXT NOT NULL,
  failure_category TEXT,
  observed_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT
);

-- Immutable capacity evidence received from a provider's authenticated control
-- API. These values are never calculated from local traffic or price tables.
CREATE TABLE provider_quota_snapshots (
  id TEXT PRIMARY KEY,
  provider_account_id TEXT NOT NULL REFERENCES provider_accounts(id),
  constraint_id TEXT NOT NULL,
  unit_kind TEXT NOT NULL,
  currency_code TEXT,
  custom_name TEXT,
  allowance_unscaled BIGINT,
  allowance_scale SMALLINT,
  consumed_unscaled BIGINT,
  consumed_scale SMALLINT,
  remaining_unscaled BIGINT,
  remaining_scale SMALLINT,
  reset_at BIGINT,
  observed_at BIGINT NOT NULL,
  fresh_until BIGINT NOT NULL,
  source_id TEXT NOT NULL CHECK (length(btrim(source_id)) > 0),
  evidence_version TEXT,
  CHECK (fresh_until >= observed_at),
  CHECK (allowance_unscaled IS NOT NULL OR consumed_unscaled IS NOT NULL OR remaining_unscaled IS NOT NULL),
  CHECK ((allowance_unscaled IS NULL) = (allowance_scale IS NULL)),
  CHECK ((consumed_unscaled IS NULL) = (consumed_scale IS NULL)),
  CHECK ((remaining_unscaled IS NULL) = (remaining_scale IS NULL)),
  CHECK (
    (unit_kind = 'currency' AND currency_code ~ '^[A-Z]{3}$' AND custom_name IS NULL)
    OR (unit_kind = 'custom' AND length(btrim(custom_name)) > 0 AND currency_code IS NULL)
    OR (unit_kind IN ('requests', 'input_tokens', 'cached_input_tokens', 'output_tokens', 'reasoning_tokens', 'total_tokens', 'concurrent_requests') AND currency_code IS NULL AND custom_name IS NULL)
  )
);

-- Immutable provider-reported billing facts reconciled to an existing gateway
-- attempt. Unit rows avoid hiding provider measurements inside a JSON blob.
CREATE TABLE provider_billing_records (
  id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL REFERENCES usage_attempts(id),
  provider_account_id TEXT NOT NULL REFERENCES provider_accounts(id),
  provider_request_id TEXT,
  charge_unit_kind TEXT,
  charge_currency_code TEXT,
  charge_custom_name TEXT,
  charge_unscaled BIGINT,
  charge_scale SMALLINT,
  observed_at BIGINT NOT NULL,
  fresh_until BIGINT NOT NULL,
  source_id TEXT NOT NULL CHECK (length(btrim(source_id)) > 0),
  evidence_version TEXT,
  CHECK (fresh_until >= observed_at),
  CHECK ((charge_unscaled IS NULL) = (charge_scale IS NULL)),
  CHECK (
    (charge_unscaled IS NULL AND charge_unit_kind IS NULL AND charge_currency_code IS NULL AND charge_custom_name IS NULL)
    OR (charge_unscaled IS NOT NULL AND charge_unit_kind = 'currency' AND charge_currency_code ~ '^[A-Z]{3}$' AND charge_custom_name IS NULL)
  )
);

CREATE TABLE provider_billing_units (
  billing_record_id TEXT NOT NULL REFERENCES provider_billing_records(id),
  unit_index INTEGER NOT NULL CHECK (unit_index >= 0),
  unit_kind TEXT NOT NULL,
  currency_code TEXT,
  custom_name TEXT,
  value_unscaled BIGINT NOT NULL,
  value_scale SMALLINT NOT NULL,
  PRIMARY KEY (billing_record_id, unit_index),
  CHECK (
    (unit_kind = 'currency' AND currency_code ~ '^[A-Z]{3}$' AND custom_name IS NULL)
    OR (unit_kind = 'custom' AND length(btrim(custom_name)) > 0 AND currency_code IS NULL)
    OR (unit_kind IN ('requests', 'input_tokens', 'cached_input_tokens', 'output_tokens', 'reasoning_tokens', 'total_tokens', 'concurrent_requests') AND currency_code IS NULL AND custom_name IS NULL)
  )
);
