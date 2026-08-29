//! Portable `PostgreSQL` control-plane adapter with no Neon-specific behavior.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use slg_domain::{
    AttemptId, AuthoritativeSource, CredentialReference, FixedDecimal, ProviderBillingRecord,
    ProviderFailure, ProviderQuotaSnapshot, ProviderReportedQuantity, ProviderUnit,
    ProviderUnitKind, RouteCandidate, candidate_plan,
};
use slg_ports::{AttemptRecord, AuthoritativeAccountingRepository, ConfigurationRepository};
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

#[derive(Clone)]
pub struct PostgresStore {
    client: Arc<Mutex<Client>>,
    probe_leases: Arc<Mutex<BTreeMap<String, String>>>,
}

const HALF_OPEN_LEASE_SECONDS: i64 = 60;
const MIGRATION_LOCK_KEY: i64 = 2_103_290_575_048_973_248;
const MIGRATION_SQL: &str = r"
CREATE SCHEMA IF NOT EXISTS smart_llm_gateway;
SET search_path TO smart_llm_gateway;
CREATE TABLE IF NOT EXISTS gateway_keys (
  id TEXT PRIMARY KEY, key_hash TEXT NOT NULL UNIQUE, description TEXT NOT NULL, enabled BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE TABLE IF NOT EXISTS logical_models (name TEXT PRIMARY KEY, enabled BOOLEAN NOT NULL DEFAULT TRUE);
CREATE TABLE IF NOT EXISTS provider_accounts (
  id TEXT PRIMARY KEY, provider TEXT NOT NULL, credential_ref TEXT NOT NULL, base_url TEXT NOT NULL, enabled BOOLEAN NOT NULL DEFAULT TRUE,
  CONSTRAINT provider_accounts_credential_ref_env_check CHECK (credential_ref ~ '^env:[A-Za-z_][A-Za-z0-9_]*$')
);
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'provider_accounts_credential_ref_env_check'
      AND conrelid = 'provider_accounts'::regclass
  ) THEN
    ALTER TABLE provider_accounts
      ADD CONSTRAINT provider_accounts_credential_ref_env_check
      CHECK (credential_ref ~ '^env:[A-Za-z_][A-Za-z0-9_]*$');
  END IF;
END
$$;
CREATE TABLE IF NOT EXISTS provider_routes (
  id TEXT PRIMARY KEY, logical_model TEXT NOT NULL REFERENCES logical_models(name), provider_account_id TEXT NOT NULL REFERENCES provider_accounts(id), upstream_model TEXT NOT NULL, priority INTEGER NOT NULL CHECK (priority >= 0), enabled BOOLEAN NOT NULL DEFAULT TRUE,
  CONSTRAINT provider_routes_model_priority_unique UNIQUE(logical_model, priority)
);
CREATE TABLE IF NOT EXISTS model_fallbacks (
  source_model TEXT NOT NULL REFERENCES logical_models(name), target_model TEXT NOT NULL REFERENCES logical_models(name), priority INTEGER NOT NULL CHECK (priority >= 0),
  CONSTRAINT model_fallbacks_source_priority_unique UNIQUE(source_model, priority)
);
CREATE TABLE IF NOT EXISTS provider_route_state (
  route_id TEXT PRIMARY KEY REFERENCES provider_routes(id), state TEXT NOT NULL DEFAULT 'closed' CHECK (state IN ('closed', 'open', 'half_open')), reason TEXT, retry_at BIGINT
);
CREATE TABLE IF NOT EXISTS provider_account_state (
  account_id TEXT PRIMARY KEY REFERENCES provider_accounts(id), state TEXT NOT NULL DEFAULT 'unknown' CHECK (state IN ('unknown', 'available', 'blocked')), reason TEXT, retry_at BIGINT
);
CREATE TABLE IF NOT EXISTS usage_attempts (
  id TEXT PRIMARY KEY, request_id TEXT NOT NULL, route_id TEXT NOT NULL, outcome TEXT NOT NULL, failure_category TEXT, observed_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT
);
CREATE TABLE IF NOT EXISTS provider_route_probe_leases (
  route_id TEXT PRIMARY KEY REFERENCES provider_routes(id), lease_id TEXT NOT NULL, expires_at BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS provider_quota_snapshots (
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
CREATE TABLE IF NOT EXISTS provider_billing_records (
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
CREATE TABLE IF NOT EXISTS provider_billing_units (
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
";

/// Sanitized data used by the process-local last-known-good snapshot.
///
/// Gateway keys remain hashed and provider credentials remain environment
/// references; neither raw credential material nor prompt content is exported.
#[derive(Debug)]
pub struct ControlSnapshotData {
    pub gateway_key_hashes: Vec<String>,
    pub routes: Vec<RouteCandidate>,
    pub fallbacks: BTreeMap<String, Vec<String>>,
    pub route_states: BTreeMap<String, (String, Option<i64>)>,
    pub account_states: BTreeMap<String, (String, Option<i64>)>,
}

impl PostgresStore {
    pub async fn connect(connection_string: &str) -> Result<Self, String> {
        let (client, connection) = tokio_postgres::connect(connection_string, NoTls)
            .await
            .map_err(|error| error.to_string())?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let store = Self {
            client: Arc::new(Mutex::new(client)),
            probe_leases: Arc::new(Mutex::new(BTreeMap::new())),
        };
        store.migrate().await?;
        Ok(store)
    }

    async fn migrate(&self) -> Result<(), String> {
        let mut client = self.client.lock().await;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| error.to_string())?;
        transaction
            .query_one("SELECT pg_advisory_xact_lock($1)", &[&MIGRATION_LOCK_KEY])
            .await
            .map_err(|error| error.to_string())?;
        transaction
            .batch_execute(MIGRATION_SQL)
            .await
            .map_err(|error| error.to_string())?;
        transaction
            .commit()
            .await
            .map_err(|error| error.to_string())
    }

    pub async fn create_gateway_key(&self, description: &str) -> Result<String, String> {
        let raw_key = format!("slg_{}", Uuid::new_v4().simple());
        self.client
            .lock()
            .await
            .execute(
                "INSERT INTO gateway_keys (id, key_hash, description) VALUES ($1, $2, $3)",
                &[&Uuid::new_v4().to_string(), &digest(&raw_key), &description],
            )
            .await
            .map_err(|error| error.to_string())?;
        Ok(raw_key)
    }

    pub async fn create_model(&self, name: &str) -> Result<(), String> {
        self.client
            .lock()
            .await
            .execute("INSERT INTO logical_models (name) VALUES ($1)", &[&name])
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub async fn create_account(
        &self,
        id: &str,
        provider: &str,
        credential_ref: &str,
        base_url: &str,
    ) -> Result<(), String> {
        let credential_ref = credential_reference(credential_ref)?;
        let mut client = self.client.lock().await;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| error.to_string())?;
        transaction.execute("INSERT INTO provider_accounts (id, provider, credential_ref, base_url) VALUES ($1, $2, $3, $4)", &[&id, &provider, &credential_ref.as_str(), &base_url.trim_end_matches('/')]).await.map_err(|error| error.to_string())?;
        transaction
            .execute(
                "INSERT INTO provider_account_state (account_id) VALUES ($1)",
                &[&id],
            )
            .await
            .map_err(|error| error.to_string())?;
        transaction
            .commit()
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub async fn create_route(
        &self,
        id: &str,
        logical_model: &str,
        account_id: &str,
        upstream_model: &str,
        priority: u32,
    ) -> Result<(), String> {
        let priority = i32::try_from(priority)
            .map_err(|_| "route priority exceeds PostgreSQL INTEGER".to_owned())?;
        let mut client = self.client.lock().await;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| error.to_string())?;
        transaction.execute("INSERT INTO provider_routes (id, logical_model, provider_account_id, upstream_model, priority) VALUES ($1, $2, $3, $4, $5)", &[&id, &logical_model, &account_id, &upstream_model, &priority]).await.map_err(|error| error.to_string())?;
        transaction
            .execute(
                "INSERT INTO provider_route_state (route_id) VALUES ($1)",
                &[&id],
            )
            .await
            .map_err(|error| error.to_string())?;
        transaction
            .commit()
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub async fn add_fallback(
        &self,
        source: &str,
        target: &str,
        priority: u32,
    ) -> Result<(), String> {
        let priority = i32::try_from(priority)
            .map_err(|_| "fallback priority exceeds PostgreSQL INTEGER".to_owned())?;
        self.client.lock().await.execute("INSERT INTO model_fallbacks (source_model, target_model, priority) VALUES ($1, $2, $3)", &[&source, &target, &priority]).await.map_err(|error| error.to_string())?;
        Ok(())
    }

    pub async fn control_snapshot_data(&self) -> Result<ControlSnapshotData, String> {
        let client = self.client.lock().await;
        let gateway_key_hashes = client
            .query(
                "SELECT key_hash FROM gateway_keys WHERE enabled = TRUE",
                &[],
            )
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        let routes = client
            .query(
                "SELECT r.id, r.logical_model, a.id, a.provider, a.credential_ref, a.base_url, r.upstream_model, r.priority
                 FROM provider_routes r JOIN provider_accounts a ON a.id = r.provider_account_id
                 WHERE r.enabled = TRUE AND a.enabled = TRUE",
                &[],
            )
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .map(|row| {
                let priority: i32 = row.get(7);
                let credential_ref: String = row.get(4);
                Ok(RouteCandidate {
                    route_id: row.get(0),
                    logical_model: row.get(1),
                    provider_account_id: row.get(2),
                    provider: row.get(3),
                    credential_ref: credential_reference(&credential_ref)?,
                    base_url: row.get(5),
                    upstream_model: row.get(6),
                    priority: u32::try_from(priority)
                        .map_err(|_| "negative route priority in PostgreSQL".to_owned())?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let mut fallbacks = BTreeMap::new();
        for row in client
            .query(
                "SELECT source_model, target_model FROM model_fallbacks ORDER BY source_model, priority",
                &[],
            )
            .await
            .map_err(|error| error.to_string())?
        {
            fallbacks
                .entry(row.get::<_, String>(0))
                .or_insert_with(Vec::new)
                .push(row.get(1));
        }
        let route_states = client
            .query(
                "SELECT route_id, state, retry_at FROM provider_route_state",
                &[],
            )
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .map(|row| (row.get(0), (row.get(1), row.get(2))))
            .collect();
        let account_states = client
            .query(
                "SELECT account_id, state, retry_at FROM provider_account_state",
                &[],
            )
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .map(|row| (row.get(0), (row.get(1), row.get(2))))
            .collect();
        Ok(ControlSnapshotData {
            gateway_key_hashes,
            routes,
            fallbacks,
            route_states,
            account_states,
        })
    }

    /// Acquires the cluster-wide lease for one expired half-open route probe.
    ///
    /// The lease expires automatically, so a crashed process cannot strand a
    /// route in `half_open`. The returned lease is retained locally and is
    /// released when the route receives its terminal transition.
    pub async fn acquire_half_open_probe(&self, route_id: &str) -> Result<bool, String> {
        let mut owned_leases = self.probe_leases.lock().await;
        if owned_leases.contains_key(route_id) {
            return Ok(false);
        }
        let lease_id = Uuid::new_v4().to_string();
        let mut client = self.client.lock().await;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| error.to_string())?;
        let lease = transaction
            .query_opt(
                "INSERT INTO provider_route_probe_leases (route_id, lease_id, expires_at)
                 VALUES ($1, $2, EXTRACT(EPOCH FROM NOW())::BIGINT + $3)
                 ON CONFLICT (route_id) DO UPDATE
                   SET lease_id = EXCLUDED.lease_id, expires_at = EXCLUDED.expires_at
                   WHERE provider_route_probe_leases.expires_at <= EXTRACT(EPOCH FROM NOW())::BIGINT
                 RETURNING lease_id",
                &[&route_id, &lease_id, &HALF_OPEN_LEASE_SECONDS],
            )
            .await
            .map_err(|error| error.to_string())?;
        if lease.is_none() {
            transaction
                .commit()
                .await
                .map_err(|error| error.to_string())?;
            return Ok(false);
        }
        let transitioned = transaction
            .execute(
                "UPDATE provider_route_state
                 SET state = 'half_open'
                 WHERE route_id = $1 AND state IN ('open', 'half_open')
                   AND retry_at IS NOT NULL AND retry_at <= EXTRACT(EPOCH FROM NOW())::BIGINT",
                &[&route_id],
            )
            .await
            .map_err(|error| error.to_string())?;
        if transitioned != 1 {
            transaction
                .rollback()
                .await
                .map_err(|error| error.to_string())?;
            return Ok(false);
        }
        transaction
            .commit()
            .await
            .map_err(|error| error.to_string())?;
        owned_leases.insert(route_id.into(), lease_id);
        Ok(true)
    }

    async fn release_half_open_probe(&self, route_id: &str) -> Result<(), String> {
        let lease_id = self.probe_leases.lock().await.remove(route_id);
        let Some(lease_id) = lease_id else {
            return Ok(());
        };
        self.client
            .lock()
            .await
            .execute(
                "DELETE FROM provider_route_probe_leases WHERE route_id = $1 AND lease_id = $2",
                &[&route_id, &lease_id],
            )
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }
}

fn digest(raw: &str) -> String {
    format!("{:x}", Sha256::digest(raw.as_bytes()))
}

fn credential_reference(value: &str) -> Result<CredentialReference, String> {
    CredentialReference::parse(value)
        .map_err(|_| "invalid credential reference in PostgreSQL configuration".to_owned())
}

#[async_trait]
impl ConfigurationRepository for PostgresStore {
    async fn authenticate(&self, raw_key: &str) -> Result<bool, String> {
        self.client
            .lock()
            .await
            .query_opt(
                "SELECT 1 FROM gateway_keys WHERE key_hash = $1 AND enabled = TRUE",
                &[&digest(raw_key)],
            )
            .await
            .map(|value| value.is_some())
            .map_err(|error| error.to_string())
    }

    async fn candidates(&self, logical_model: &str) -> Result<Vec<RouteCandidate>, String> {
        let (routes, route_states, fallbacks) = {
            let client = self.client.lock().await;
            let rows = client
                .query(
                    "WITH RECURSIVE reachable_models(model) AS (
                       VALUES ($1::TEXT)
                       UNION
                       SELECT fallback.target_model
                       FROM model_fallbacks fallback
                       JOIN reachable_models reachable ON fallback.source_model = reachable.model
                     )
                     SELECT r.id, r.logical_model, a.id, a.provider, a.credential_ref, a.base_url, r.upstream_model, r.priority, state.state
                     FROM provider_routes r JOIN provider_accounts a ON a.id = r.provider_account_id
                     JOIN provider_route_state state ON state.route_id = r.id
                     JOIN provider_account_state account_state ON account_state.account_id = a.id
                     WHERE r.logical_model IN (SELECT model FROM reachable_models)
                       AND r.enabled = TRUE AND a.enabled = TRUE AND account_state.state != 'blocked'
                       AND (state.state = 'closed' OR (state.state IN ('open', 'half_open')
                            AND state.retry_at IS NOT NULL AND state.retry_at <= EXTRACT(EPOCH FROM NOW())::BIGINT))",
                    &[&logical_model],
                )
                .await
                .map_err(|error| error.to_string())?;
            let mut route_states: BTreeMap<String, String> = BTreeMap::new();
            let routes = rows
                .into_iter()
                .map(|row| {
                    let priority: i32 = row.get(7);
                    let route_id: String = row.get(0);
                    let credential_ref: String = row.get(4);
                    route_states.insert(route_id.clone(), row.get(8));
                    Ok(RouteCandidate {
                        route_id,
                        logical_model: row.get(1),
                        provider_account_id: row.get(2),
                        provider: row.get(3),
                        credential_ref: credential_reference(&credential_ref)?,
                        base_url: row.get(5),
                        upstream_model: row.get(6),
                        priority: u32::try_from(priority)
                            .map_err(|_| "negative route priority in PostgreSQL".to_owned())?,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            let fallback_rows = client
                .query(
                    "SELECT source_model, target_model FROM model_fallbacks ORDER BY source_model, priority",
                    &[],
                )
                .await
                .map_err(|error| error.to_string())?;
            let mut fallbacks = BTreeMap::new();
            for row in fallback_rows {
                fallbacks
                    .entry(row.get::<_, String>(0))
                    .or_insert_with(Vec::new)
                    .push(row.get(1));
            }
            (routes, route_states, fallbacks)
        };
        let plan = candidate_plan(logical_model, &routes, &fallbacks)
            .map_err(|error| error.to_string())?;
        let mut eligible = Vec::new();
        for candidate in plan {
            let Some(state) = route_states.get(&candidate.route_id) else {
                continue;
            };
            if state == "closed" {
                eligible.push(candidate);
                continue;
            }
            if eligible.is_empty() && self.acquire_half_open_probe(&candidate.route_id).await? {
                eligible.push(candidate);
            }
        }
        if eligible.is_empty() {
            return Err(format!("no eligible route exists for `{logical_model}`"));
        }
        Ok(eligible)
    }

    async fn record_attempt(&self, attempt: AttemptRecord) -> Result<(), String> {
        self.client.lock().await.execute("INSERT INTO usage_attempts (id, request_id, route_id, outcome, failure_category) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO NOTHING", &[&attempt.attempt_id.to_string(), &attempt.request_id, &attempt.route_id, &attempt.outcome, &attempt.failure_category]).await.map_err(|error| error.to_string())?;
        Ok(())
    }

    async fn mark_route_success(&self, route_id: &str) -> Result<(), String> {
        self.client.lock().await.execute("UPDATE provider_route_state SET state = 'closed', reason = NULL, retry_at = NULL WHERE route_id = $1", &[&route_id]).await.map_err(|error| error.to_string())?;
        self.release_half_open_probe(route_id).await?;
        Ok(())
    }

    async fn mark_route_failure(
        &self,
        route_id: &str,
        failure: &ProviderFailure,
    ) -> Result<(), String> {
        self.client.lock().await.execute("UPDATE provider_route_state SET state = 'open', reason = $2, retry_at = $3 WHERE route_id = $1", &[&route_id, &format!("{:?}", failure.category), &failure.retry_at_unix]).await.map_err(|error| error.to_string())?;
        self.release_half_open_probe(route_id).await?;
        Ok(())
    }

    async fn mark_account_failure(
        &self,
        account_id: &str,
        failure: &ProviderFailure,
    ) -> Result<(), String> {
        let route_ids = {
            let client = self.client.lock().await;
            client.execute("UPDATE provider_account_state SET state = 'blocked', reason = $2, retry_at = $3 WHERE account_id = $1", &[&account_id, &format!("{:?}", failure.category), &failure.retry_at_unix]).await.map_err(|error| error.to_string())?;
            client
                .query(
                    "SELECT id FROM provider_routes WHERE provider_account_id = $1",
                    &[&account_id],
                )
                .await
                .map_err(|error| error.to_string())?
                .into_iter()
                .map(|row| row.get::<_, String>(0))
                .collect::<Vec<_>>()
        };
        for route_id in route_ids {
            self.release_half_open_probe(&route_id).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl AuthoritativeAccountingRepository for PostgresStore {
    async fn record_quota_snapshot(&self, snapshot: ProviderQuotaSnapshot) -> Result<(), String> {
        snapshot.validate().map_err(|error| error.to_string())?;
        self.client
            .lock()
            .await
            .execute(
                "INSERT INTO provider_quota_snapshots (
                   id, provider_account_id, constraint_id, unit_kind, currency_code, custom_name,
                   allowance_unscaled, allowance_scale, consumed_unscaled, consumed_scale,
                   remaining_unscaled, remaining_scale, reset_at, observed_at, fresh_until,
                   source_id, evidence_version
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                 ON CONFLICT (id) DO NOTHING",
                &[
                    &snapshot.snapshot_id,
                    &snapshot.provider_account_id,
                    &snapshot.constraint_id,
                    &unit_kind_name(snapshot.unit.kind),
                    &snapshot.unit.currency_code,
                    &snapshot.unit.custom_name,
                    &snapshot.allowance.map(|value| value.unscaled),
                    &snapshot.allowance.map(|value| i16::from(value.scale)),
                    &snapshot.consumed.map(|value| value.unscaled),
                    &snapshot.consumed.map(|value| i16::from(value.scale)),
                    &snapshot.remaining.map(|value| value.unscaled),
                    &snapshot.remaining.map(|value| i16::from(value.scale)),
                    &snapshot.reset_at_unix,
                    &snapshot.observed_at_unix,
                    &snapshot.fresh_until_unix,
                    &snapshot.source.source_id,
                    &snapshot.source.evidence_version,
                ],
            )
            .await
            .map_err(|_| "PostgreSQL authoritative quota persistence failed".to_owned())?;
        Ok(())
    }

    async fn record_billing_record(&self, record: ProviderBillingRecord) -> Result<(), String> {
        record.validate().map_err(|error| error.to_string())?;
        let mut client = self.client.lock().await;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| "PostgreSQL authoritative billing transaction failed".to_owned())?;
        let charge = record.charge.as_ref();
        transaction
            .execute(
                "INSERT INTO provider_billing_records (
                   id, attempt_id, provider_account_id, provider_request_id, charge_unit_kind,
                   charge_currency_code, charge_custom_name, charge_unscaled, charge_scale,
                   observed_at, fresh_until, source_id, evidence_version
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                 ON CONFLICT (id) DO NOTHING",
                &[
                    &record.record_id,
                    &record.attempt_id.to_string(),
                    &record.provider_account_id,
                    &record.provider_request_id,
                    &charge.map(|value| unit_kind_name(value.unit.kind)),
                    &charge.and_then(|value| value.unit.currency_code.as_deref()),
                    &charge.and_then(|value| value.unit.custom_name.as_deref()),
                    &charge.map(|value| value.value.unscaled),
                    &charge.map(|value| i16::from(value.value.scale)),
                    &record.observed_at_unix,
                    &record.fresh_until_unix,
                    &record.source.source_id,
                    &record.source.evidence_version,
                ],
            )
            .await
            .map_err(|_| "PostgreSQL authoritative billing persistence failed".to_owned())?;
        for (index, quantity) in record.billed_units.iter().enumerate() {
            let index = i32::try_from(index)
                .map_err(|_| "too many provider-reported billing units".to_owned())?;
            transaction
                .execute(
                    "INSERT INTO provider_billing_units (
                       billing_record_id, unit_index, unit_kind, currency_code, custom_name,
                       value_unscaled, value_scale
                     ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                     ON CONFLICT (billing_record_id, unit_index) DO NOTHING",
                    &[
                        &record.record_id,
                        &index,
                        &unit_kind_name(quantity.unit.kind),
                        &quantity.unit.currency_code,
                        &quantity.unit.custom_name,
                        &quantity.value.unscaled,
                        &i16::from(quantity.value.scale),
                    ],
                )
                .await
                .map_err(|_| {
                    "PostgreSQL authoritative billing unit persistence failed".to_owned()
                })?;
        }
        transaction
            .commit()
            .await
            .map_err(|_| "PostgreSQL authoritative billing transaction failed".to_owned())?;
        Ok(())
    }

    async fn quota_snapshots(
        &self,
        provider_account_id: &str,
    ) -> Result<Vec<ProviderQuotaSnapshot>, String> {
        let rows = self
            .client
            .lock()
            .await
            .query(
                "SELECT id, provider_account_id, constraint_id, unit_kind, currency_code, custom_name,
                        allowance_unscaled, allowance_scale, consumed_unscaled, consumed_scale,
                        remaining_unscaled, remaining_scale, reset_at, observed_at, fresh_until,
                        source_id, evidence_version
                 FROM provider_quota_snapshots
                 WHERE provider_account_id = $1
                 ORDER BY observed_at DESC, id DESC",
                &[&provider_account_id],
            )
            .await
            .map_err(|_| "PostgreSQL authoritative quota inspection failed".to_owned())?;
        rows.into_iter()
            .map(|row| {
                let snapshot = ProviderQuotaSnapshot {
                    snapshot_id: row.get(0),
                    provider_account_id: row.get(1),
                    constraint_id: row.get(2),
                    unit: provider_unit(row.get(3), row.get(4), row.get(5))?,
                    allowance: fixed_decimal(row.get(6), scale_as_i64(row.get(7)))?,
                    consumed: fixed_decimal(row.get(8), scale_as_i64(row.get(9)))?,
                    remaining: fixed_decimal(row.get(10), scale_as_i64(row.get(11)))?,
                    reset_at_unix: row.get(12),
                    observed_at_unix: row.get(13),
                    fresh_until_unix: row.get(14),
                    source: AuthoritativeSource {
                        source_id: row.get(15),
                        evidence_version: row.get(16),
                    },
                };
                snapshot.validate().map_err(|error| error.to_string())?;
                Ok(snapshot)
            })
            .collect()
    }

    async fn billing_records(
        &self,
        attempt_id: &AttemptId,
    ) -> Result<Vec<ProviderBillingRecord>, String> {
        let client = self.client.lock().await;
        let rows = client
            .query(
                "SELECT id, attempt_id, provider_account_id, provider_request_id, charge_unit_kind,
                        charge_currency_code, charge_custom_name, charge_unscaled, charge_scale,
                        observed_at, fresh_until, source_id, evidence_version
                 FROM provider_billing_records
                 WHERE attempt_id = $1
                 ORDER BY observed_at, id",
                &[&attempt_id.to_string()],
            )
            .await
            .map_err(|_| "PostgreSQL authoritative billing inspection failed".to_owned())?;
        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let record_id: String = row.get(0);
            let unit_rows = client
                .query(
                    "SELECT unit_kind, currency_code, custom_name, value_unscaled, value_scale
                     FROM provider_billing_units
                     WHERE billing_record_id = $1
                     ORDER BY unit_index",
                    &[&record_id],
                )
                .await
                .map_err(|_| "PostgreSQL authoritative billing inspection failed".to_owned())?;
            let billed_units = unit_rows
                .into_iter()
                .map(|unit| {
                    Ok(ProviderReportedQuantity {
                        unit: provider_unit(unit.get(0), unit.get(1), unit.get(2))?,
                        value: fixed_decimal(
                            Some(unit.get(3)),
                            Some(i64::from(unit.get::<_, i16>(4))),
                        )?
                        .ok_or_else(|| "missing billing unit value".to_owned())?,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            let charge_kind: Option<String> = row.get(4);
            let charge = match charge_kind {
                Some(kind) => Some(ProviderReportedQuantity {
                    unit: provider_unit(&kind, row.get(5), row.get(6))?,
                    value: fixed_decimal(row.get(7), scale_as_i64(row.get(8)))?
                        .ok_or_else(|| "missing provider-reported charge".to_owned())?,
                }),
                None => None,
            };
            let record = ProviderBillingRecord {
                record_id,
                attempt_id: AttemptId::parse(&row.get::<_, String>(1))
                    .map_err(|_| "invalid stored attempt identifier".to_owned())?,
                provider_account_id: row.get(2),
                provider_request_id: row.get(3),
                billed_units,
                charge,
                observed_at_unix: row.get(9),
                fresh_until_unix: row.get(10),
                source: AuthoritativeSource {
                    source_id: row.get(11),
                    evidence_version: row.get(12),
                },
            };
            record.validate().map_err(|error| error.to_string())?;
            records.push(record);
        }
        Ok(records)
    }
}

fn unit_kind_name(kind: ProviderUnitKind) -> &'static str {
    match kind {
        ProviderUnitKind::Requests => "requests",
        ProviderUnitKind::InputTokens => "input_tokens",
        ProviderUnitKind::CachedInputTokens => "cached_input_tokens",
        ProviderUnitKind::OutputTokens => "output_tokens",
        ProviderUnitKind::ReasoningTokens => "reasoning_tokens",
        ProviderUnitKind::TotalTokens => "total_tokens",
        ProviderUnitKind::ConcurrentRequests => "concurrent_requests",
        ProviderUnitKind::Currency => "currency",
        ProviderUnitKind::Custom => "custom",
    }
}

fn provider_unit(
    kind: &str,
    currency_code: Option<String>,
    custom_name: Option<String>,
) -> Result<ProviderUnit, String> {
    let kind = match kind {
        "requests" => ProviderUnitKind::Requests,
        "input_tokens" => ProviderUnitKind::InputTokens,
        "cached_input_tokens" => ProviderUnitKind::CachedInputTokens,
        "output_tokens" => ProviderUnitKind::OutputTokens,
        "reasoning_tokens" => ProviderUnitKind::ReasoningTokens,
        "total_tokens" => ProviderUnitKind::TotalTokens,
        "concurrent_requests" => ProviderUnitKind::ConcurrentRequests,
        "currency" => ProviderUnitKind::Currency,
        "custom" => ProviderUnitKind::Custom,
        _ => return Err("invalid stored provider unit".to_owned()),
    };
    let unit = ProviderUnit {
        kind,
        currency_code,
        custom_name,
    };
    unit.validate().map_err(|error| error.to_string())?;
    Ok(unit)
}

fn fixed_decimal(
    unscaled: Option<i64>,
    scale: Option<i64>,
) -> Result<Option<FixedDecimal>, String> {
    match (unscaled, scale) {
        (None, None) => Ok(None),
        (Some(unscaled), Some(scale)) => Ok(Some(FixedDecimal {
            unscaled,
            scale: u8::try_from(scale).map_err(|_| "invalid stored decimal scale".to_owned())?,
        })),
        _ => Err("invalid stored decimal value".to_owned()),
    }
}

fn scale_as_i64(scale: Option<i16>) -> Option<i64> {
    scale.map(i64::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use slg_adapter_storage_sqlite::SqliteStore;
    use slg_ports::AuthoritativeAccountingRepository;

    fn integration_url() -> Option<String> {
        std::env::var("SLG_POSTGRES_TEST_URL").ok()
    }

    fn unique(prefix: &str) -> String {
        format!("{prefix}-{}", Uuid::new_v4().simple())
    }

    async fn configure_route(store: &PostgresStore, model: &str, account: &str, route: &str) {
        store.create_model(model).await.unwrap();
        store
            .create_account(
                account,
                "openai-compatible",
                "env:POSTGRES_TEST_PROVIDER_KEY",
                "https://provider.example.test",
            )
            .await
            .unwrap();
        store
            .create_route(route, model, account, "upstream-model", 1)
            .await
            .unwrap();
    }

    fn configure_sqlite_route(sqlite: &SqliteStore, model: &str, account: &str, route: &str) {
        sqlite.create_model(model).unwrap();
        sqlite
            .create_account(
                account,
                "openai-compatible",
                "env:POSTGRES_TEST_PROVIDER_KEY",
                "https://provider.example.test",
            )
            .unwrap();
        sqlite
            .create_route(route, model, account, "upstream-model", 1)
            .unwrap();
    }

    fn authoritative_quota_snapshot(account: &str) -> ProviderQuotaSnapshot {
        ProviderQuotaSnapshot {
            snapshot_id: unique("quota-snapshot"),
            provider_account_id: account.into(),
            constraint_id: "daily-input-tokens".into(),
            unit: ProviderUnit {
                kind: ProviderUnitKind::InputTokens,
                currency_code: None,
                custom_name: None,
            },
            allowance: Some(FixedDecimal {
                unscaled: 1_000_000,
                scale: 0,
            }),
            consumed: Some(FixedDecimal {
                unscaled: 123_456,
                scale: 0,
            }),
            remaining: None,
            reset_at_unix: Some(1_700_003_600),
            observed_at_unix: 1_700_000_000,
            fresh_until_unix: 1_700_000_300,
            source: AuthoritativeSource {
                source_id: "provider-quota-endpoint".into(),
                evidence_version: Some("2026-08".into()),
            },
        }
    }

    fn authoritative_billing_record(account: &str, attempt_id: AttemptId) -> ProviderBillingRecord {
        ProviderBillingRecord {
            record_id: unique("billing-record"),
            attempt_id,
            provider_account_id: account.into(),
            provider_request_id: Some("provider-request-opaque-id".into()),
            billed_units: vec![
                ProviderReportedQuantity {
                    unit: ProviderUnit {
                        kind: ProviderUnitKind::InputTokens,
                        currency_code: None,
                        custom_name: None,
                    },
                    value: FixedDecimal {
                        unscaled: 321,
                        scale: 0,
                    },
                },
                ProviderReportedQuantity {
                    unit: ProviderUnit {
                        kind: ProviderUnitKind::Custom,
                        currency_code: None,
                        custom_name: Some("provider_compute_units".into()),
                    },
                    value: FixedDecimal {
                        unscaled: 75,
                        scale: 2,
                    },
                },
            ],
            charge: Some(ProviderReportedQuantity {
                unit: ProviderUnit {
                    kind: ProviderUnitKind::Currency,
                    currency_code: Some("USD".into()),
                    custom_name: None,
                },
                value: FixedDecimal {
                    unscaled: 1234,
                    scale: 4,
                },
            }),
            observed_at_unix: 1_700_000_010,
            fresh_until_unix: 1_700_000_310,
            source: AuthoritativeSource {
                source_id: "provider-billing-export".into(),
                evidence_version: Some("v3".into()),
            },
        }
    }

    async fn record_authoritative_accounting_in_both_stores(
        postgres: &PostgresStore,
        sqlite: &SqliteStore,
        snapshot: ProviderQuotaSnapshot,
        billing: ProviderBillingRecord,
    ) {
        for store in [
            postgres as &dyn AuthoritativeAccountingRepository,
            sqlite as &dyn AuthoritativeAccountingRepository,
        ] {
            store.record_quota_snapshot(snapshot.clone()).await.unwrap();
            store.record_quota_snapshot(snapshot.clone()).await.unwrap();
            store.record_billing_record(billing.clone()).await.unwrap();
            store.record_billing_record(billing.clone()).await.unwrap();
        }
    }

    #[tokio::test]
    async fn postgres_lease_is_exclusive_and_candidate_order_matches_sqlite() {
        let Some(url) = integration_url() else {
            return;
        };
        let postgres = PostgresStore::connect(&url).await.unwrap();
        let peer = PostgresStore::connect(&url).await.unwrap();
        let sqlite = SqliteStore::in_memory().unwrap();
        let model = unique("lease-model");
        let account = unique("lease-account");
        let route = unique("lease-route");
        configure_route(&postgres, &model, &account, &route).await;
        sqlite.create_model(&model).unwrap();
        sqlite
            .create_account(
                &account,
                "openai-compatible",
                "env:POSTGRES_TEST_PROVIDER_KEY",
                "https://provider.example.test",
            )
            .unwrap();
        sqlite
            .create_route(&route, &model, &account, "upstream-model", 1)
            .unwrap();

        assert_eq!(
            postgres
                .candidates(&model)
                .await
                .unwrap()
                .into_iter()
                .map(|candidate| candidate.route_id)
                .collect::<Vec<_>>(),
            sqlite
                .candidates(&model)
                .await
                .unwrap()
                .into_iter()
                .map(|candidate| candidate.route_id)
                .collect::<Vec<_>>(),
        );

        let failure = ProviderFailure {
            category: slg_domain::ErrorCategory::ProviderUnavailable,
            message: "provider unavailable".into(),
            status: Some(503),
            retry_at_unix: Some(0),
        };
        postgres.mark_route_failure(&route, &failure).await.unwrap();
        let (first, second) = tokio::join!(
            postgres.acquire_half_open_probe(&route),
            peer.acquire_half_open_probe(&route)
        );
        assert_eq!(
            usize::from(first.unwrap()) + usize::from(second.unwrap()),
            1
        );
    }

    #[tokio::test]
    async fn postgres_concurrent_migrations_succeed() {
        let Some(url) = integration_url() else {
            return;
        };
        let (first, second) =
            tokio::join!(PostgresStore::connect(&url), PostgresStore::connect(&url));
        assert!(first.is_ok());
        assert!(second.is_ok());
    }

    #[tokio::test]
    async fn postgres_rejects_literal_credential_references_without_echoing_them() {
        let Some(url) = integration_url() else {
            return;
        };
        let store = PostgresStore::connect(&url).await.unwrap();
        let account = unique("invalid-reference-account");
        let literal = "literal-credential-must-not-leak";
        {
            let client = store.client.lock().await;
            let error = client
                .execute(
                    "INSERT INTO provider_accounts (id, provider, credential_ref, base_url) VALUES ($1, $2, $3, $4)",
                    &[&account, &"openai-compatible", &literal, &"https://provider.example.test"],
                )
                .await
                .unwrap_err();
            assert_eq!(
                error.code(),
                Some(&tokio_postgres::error::SqlState::CHECK_VIOLATION)
            );
            assert!(!error.to_string().contains(literal));
        }
    }

    #[tokio::test]
    async fn postgres_records_the_same_attempt_once() {
        let Some(url) = integration_url() else {
            return;
        };
        let store = PostgresStore::connect(&url).await.unwrap();
        let attempt = AttemptRecord {
            attempt_id: slg_domain::AttemptId::new(),
            request_id: unique("attempt-request"),
            route_id: unique("attempt-route"),
            outcome: "succeeded".into(),
            failure_category: None,
        };

        store.record_attempt(attempt.clone()).await.unwrap();
        store.record_attempt(attempt.clone()).await.unwrap();

        let client = store.client.lock().await;
        let count: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM usage_attempts WHERE id = $1",
                &[&attempt.attempt_id.to_string()],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn postgres_preserves_authoritative_accounting_with_sqlite_parity() {
        let Some(url) = integration_url() else {
            return;
        };
        let postgres = PostgresStore::connect(&url).await.unwrap();
        let sqlite = SqliteStore::in_memory().unwrap();
        let model = unique("accounting-model");
        let account = unique("accounting-account");
        let route = unique("accounting-route");
        configure_route(&postgres, &model, &account, &route).await;
        configure_sqlite_route(&sqlite, &model, &account, &route);

        let attempt = AttemptRecord {
            attempt_id: AttemptId::new(),
            request_id: unique("accounting-request"),
            route_id: route,
            outcome: "succeeded".into(),
            failure_category: None,
        };
        postgres.record_attempt(attempt.clone()).await.unwrap();
        sqlite.record_attempt(attempt.clone()).await.unwrap();

        record_authoritative_accounting_in_both_stores(
            &postgres,
            &sqlite,
            authoritative_quota_snapshot(&account),
            authoritative_billing_record(&account, attempt.attempt_id.clone()),
        )
        .await;

        assert_eq!(
            postgres.quota_snapshots(&account).await.unwrap(),
            sqlite.quota_snapshots(&account).await.unwrap(),
        );
        assert_eq!(
            postgres.billing_records(&attempt.attempt_id).await.unwrap(),
            sqlite.billing_records(&attempt.attempt_id).await.unwrap(),
        );
    }
}
