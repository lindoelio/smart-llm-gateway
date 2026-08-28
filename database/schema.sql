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
