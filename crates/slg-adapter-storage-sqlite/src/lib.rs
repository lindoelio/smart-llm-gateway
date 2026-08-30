//! `SQLite` control plane for a single gateway process.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, params, types::Type};
use sha2::{Digest, Sha256};
use slg_domain::{
    AttemptId, AuthoritativeSource, CredentialReference, FixedDecimal, ProviderBillingRecord,
    ProviderFailure, ProviderQuotaSnapshot, ProviderReportedQuantity, ProviderUnit,
    ProviderUnitKind, RouteCandidate, candidate_plan,
};
use slg_ports::{AttemptRecord, AuthoritativeAccountingRepository, ConfigurationRepository};
use uuid::Uuid;

#[derive(Clone)]
pub struct SqliteStore {
    connection: Arc<Mutex<Connection>>,
}

impl SqliteStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        let connection = Connection::open(path).map_err(|error| error.to_string())?;
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .map_err(|error| error.to_string())?;
        let store = Self {
            connection: Arc::new(Mutex::new(connection)),
        };
        store.migrate()?;
        Ok(store)
    }

    pub fn in_memory() -> Result<Self, String> {
        let connection = Connection::open_in_memory().map_err(|error| error.to_string())?;
        let store = Self {
            connection: Arc::new(Mutex::new(connection)),
        };
        store.migrate()?;
        Ok(store)
    }

    pub fn migrate(&self) -> Result<(), String> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| "sqlite mutex poisoned".to_owned())?;
        migrate_schema(&connection)
    }

    pub fn create_gateway_key(&self, description: &str) -> Result<String, String> {
        let raw_key = format!("slg_{}", Uuid::new_v4().simple());
        let connection = self
            .connection
            .lock()
            .map_err(|_| "sqlite mutex poisoned".to_owned())?;
        connection
            .execute(
                "INSERT INTO gateway_keys (id, key_hash, description) VALUES (?1, ?2, ?3)",
                params![Uuid::new_v4().to_string(), digest(&raw_key), description],
            )
            .map_err(|error| error.to_string())?;
        Ok(raw_key)
    }

    pub fn create_model(&self, name: &str) -> Result<(), String> {
        self.connection
            .lock()
            .map_err(|_| "sqlite mutex poisoned".to_owned())?
            .execute("INSERT INTO logical_models (name) VALUES (?1)", [name])
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub fn create_account(
        &self,
        id: &str,
        provider: &str,
        credential_ref: &str,
        base_url: &str,
    ) -> Result<(), String> {
        let credential_ref =
            CredentialReference::parse(credential_ref).map_err(|error| error.to_string())?;
        self.create_account_with_reference(id, provider, &credential_ref, base_url)
    }

    /// Persists an account whose credential reference has already passed the
    /// domain boundary. PostgreSQL implementations must provide the same
    /// typed path and enforce `env:NAME` in their storage schema.
    pub fn create_account_with_reference(
        &self,
        id: &str,
        provider: &str,
        credential_ref: &CredentialReference,
        base_url: &str,
    ) -> Result<(), String> {
        self.connection.lock().map_err(|_| "sqlite mutex poisoned".to_owned())?.execute("INSERT INTO provider_accounts (id, provider, credential_ref, base_url) VALUES (?1, ?2, ?3, ?4)", params![id, provider, credential_ref.as_str(), base_url.trim_end_matches('/')]).map_err(|error| error.to_string())?;
        self.connection
            .lock()
            .map_err(|_| "sqlite mutex poisoned".to_owned())?
            .execute(
                "INSERT OR IGNORE INTO provider_account_state (account_id) VALUES (?1)",
                [id],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub fn create_route(
        &self,
        id: &str,
        logical_model: &str,
        account_id: &str,
        upstream_model: &str,
        priority: u32,
    ) -> Result<(), String> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| "sqlite mutex poisoned".to_owned())?;
        connection.execute("INSERT INTO provider_routes (id, logical_model, provider_account_id, upstream_model, priority) VALUES (?1, ?2, ?3, ?4, ?5)", params![id, logical_model, account_id, upstream_model, priority]).map_err(|error| error.to_string())?;
        connection
            .execute(
                "INSERT OR IGNORE INTO provider_route_state (route_id) VALUES (?1)",
                [id],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub fn add_fallback(&self, source: &str, target: &str, priority: u32) -> Result<(), String> {
        self.connection.lock().map_err(|_| "sqlite mutex poisoned".to_owned())?.execute("INSERT INTO model_fallbacks (source_model, target_model, priority) VALUES (?1, ?2, ?3)", params![source, target, priority]).map_err(|error| error.to_string())?;
        Ok(())
    }
}

fn digest(raw: &str) -> String {
    format!("{:x}", Sha256::digest(raw.as_bytes()))
}

fn migrate_schema(connection: &Connection) -> Result<(), String> {
    connection
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS gateway_keys (id TEXT PRIMARY KEY, key_hash TEXT NOT NULL UNIQUE, description TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1);
             CREATE TABLE IF NOT EXISTS logical_models (name TEXT PRIMARY KEY, enabled INTEGER NOT NULL DEFAULT 1);
             CREATE TABLE IF NOT EXISTS provider_accounts (id TEXT PRIMARY KEY, provider TEXT NOT NULL, credential_ref TEXT NOT NULL CHECK (credential_ref GLOB 'env:[A-Za-z_]*' AND substr(credential_ref, 5) NOT GLOB '*[^A-Za-z0-9_]*'), base_url TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1);
             CREATE TABLE IF NOT EXISTS provider_routes (id TEXT PRIMARY KEY, logical_model TEXT NOT NULL REFERENCES logical_models(name), provider_account_id TEXT NOT NULL REFERENCES provider_accounts(id), upstream_model TEXT NOT NULL, priority INTEGER NOT NULL, enabled INTEGER NOT NULL DEFAULT 1, UNIQUE(logical_model, priority));
             CREATE TABLE IF NOT EXISTS model_fallbacks (source_model TEXT NOT NULL REFERENCES logical_models(name), target_model TEXT NOT NULL REFERENCES logical_models(name), priority INTEGER NOT NULL, PRIMARY KEY(source_model, priority));
             CREATE TABLE IF NOT EXISTS provider_route_state (route_id TEXT PRIMARY KEY REFERENCES provider_routes(id), state TEXT NOT NULL DEFAULT 'closed', reason TEXT, retry_at INTEGER);
             CREATE TABLE IF NOT EXISTS provider_account_state (account_id TEXT PRIMARY KEY REFERENCES provider_accounts(id), state TEXT NOT NULL DEFAULT 'unknown', reason TEXT, retry_at INTEGER);
             CREATE TABLE IF NOT EXISTS usage_attempts (id TEXT PRIMARY KEY, request_id TEXT NOT NULL, route_id TEXT NOT NULL, outcome TEXT NOT NULL CHECK (outcome IN ('failed', 'committed', 'succeeded', 'partial_failed', 'cancelled')), failure_category TEXT, observed_at INTEGER NOT NULL DEFAULT (unixepoch()));
             CREATE TABLE IF NOT EXISTS provider_quota_snapshots (id TEXT PRIMARY KEY, provider_account_id TEXT NOT NULL REFERENCES provider_accounts(id), constraint_id TEXT NOT NULL, unit_kind TEXT NOT NULL, currency_code TEXT, custom_name TEXT, allowance_unscaled INTEGER, allowance_scale INTEGER, consumed_unscaled INTEGER, consumed_scale INTEGER, remaining_unscaled INTEGER, remaining_scale INTEGER, reset_at INTEGER, observed_at INTEGER NOT NULL, fresh_until INTEGER NOT NULL, source_id TEXT NOT NULL CHECK (length(trim(source_id)) > 0), evidence_version TEXT, CHECK (fresh_until >= observed_at), CHECK (allowance_unscaled IS NOT NULL OR consumed_unscaled IS NOT NULL OR remaining_unscaled IS NOT NULL), CHECK ((allowance_unscaled IS NULL) = (allowance_scale IS NULL)), CHECK ((consumed_unscaled IS NULL) = (consumed_scale IS NULL)), CHECK ((remaining_unscaled IS NULL) = (remaining_scale IS NULL)), CHECK ((unit_kind = 'currency' AND currency_code GLOB '[A-Z][A-Z][A-Z]' AND custom_name IS NULL) OR (unit_kind = 'custom' AND length(trim(custom_name)) > 0 AND currency_code IS NULL) OR (unit_kind IN ('requests', 'input_tokens', 'cached_input_tokens', 'output_tokens', 'reasoning_tokens', 'total_tokens', 'concurrent_requests') AND currency_code IS NULL AND custom_name IS NULL)));
             CREATE TABLE IF NOT EXISTS provider_billing_records (id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL REFERENCES usage_attempts(id), provider_account_id TEXT NOT NULL REFERENCES provider_accounts(id), provider_request_id TEXT, charge_unit_kind TEXT, charge_currency_code TEXT, charge_custom_name TEXT, charge_unscaled INTEGER, charge_scale INTEGER, observed_at INTEGER NOT NULL, fresh_until INTEGER NOT NULL, source_id TEXT NOT NULL CHECK (length(trim(source_id)) > 0), evidence_version TEXT, CHECK (fresh_until >= observed_at), CHECK ((charge_unscaled IS NULL) = (charge_scale IS NULL)), CHECK ((charge_unscaled IS NULL AND charge_unit_kind IS NULL AND charge_currency_code IS NULL AND charge_custom_name IS NULL) OR (charge_unscaled IS NOT NULL AND charge_unit_kind = 'currency' AND charge_currency_code GLOB '[A-Z][A-Z][A-Z]' AND charge_custom_name IS NULL)));
             CREATE TABLE IF NOT EXISTS provider_billing_units (billing_record_id TEXT NOT NULL REFERENCES provider_billing_records(id), unit_index INTEGER NOT NULL CHECK (unit_index >= 0), unit_kind TEXT NOT NULL, currency_code TEXT, custom_name TEXT, value_unscaled INTEGER NOT NULL, value_scale INTEGER NOT NULL, PRIMARY KEY (billing_record_id, unit_index), CHECK ((unit_kind = 'currency' AND currency_code GLOB '[A-Z][A-Z][A-Z]' AND custom_name IS NULL) OR (unit_kind = 'custom' AND length(trim(custom_name)) > 0 AND currency_code IS NULL) OR (unit_kind IN ('requests', 'input_tokens', 'cached_input_tokens', 'output_tokens', 'reasoning_tokens', 'total_tokens', 'concurrent_requests') AND currency_code IS NULL AND custom_name IS NULL)));",
        )
        .map_err(|error| error.to_string())
}

impl SqliteStore {
    fn authenticate_internal(&self, raw_key: &str) -> Result<bool, String> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| "sqlite mutex poisoned".to_owned())?;
        connection
            .query_row(
                "SELECT 1 FROM gateway_keys WHERE key_hash = ?1 AND enabled = 1",
                [digest(raw_key)],
                |_| Ok(()),
            )
            .optional()
            .map(|value| value.is_some())
            .map_err(|error| error.to_string())
    }

    fn candidates_internal(&self, logical_model: &str) -> Result<Vec<RouteCandidate>, String> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| "sqlite mutex poisoned".to_owned())?;
        let mut statement = connection.prepare(
            "SELECT r.id, r.logical_model, a.id, a.provider, a.credential_ref, a.base_url, r.upstream_model, r.priority
             FROM provider_routes r JOIN provider_accounts a ON a.id = r.provider_account_id
             JOIN provider_route_state s ON s.route_id = r.id
             JOIN provider_account_state account_state ON account_state.account_id = a.id
             WHERE r.enabled = 1 AND a.enabled = 1 AND account_state.state != 'blocked'
               AND (s.state = 'closed' OR (s.retry_at IS NOT NULL AND s.retry_at <= unixepoch()))"
        ).map_err(|error| error.to_string())?;
        let routes = statement
            .query_map([], |row| {
                let credential_ref: String = row.get(4)?;
                let credential_ref =
                    CredentialReference::parse(&credential_ref).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(4, Type::Text, Box::new(error))
                    })?;
                Ok(RouteCandidate {
                    route_id: row.get(0)?,
                    logical_model: row.get(1)?,
                    provider_account_id: row.get(2)?,
                    provider: row.get(3)?,
                    credential_ref,
                    base_url: row.get(5)?,
                    upstream_model: row.get(6)?,
                    priority: row.get(7)?,
                })
            })
            .map_err(|error| error.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        let mut fallback_statement = connection.prepare("SELECT source_model, target_model FROM model_fallbacks ORDER BY source_model, priority").map_err(|error| error.to_string())?;
        let mut fallbacks: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for row in fallback_statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|error| error.to_string())?
        {
            let (source, target) = row.map_err(|error| error.to_string())?;
            fallbacks.entry(source).or_default().push(target);
        }
        candidate_plan(logical_model, &routes, &fallbacks).map_err(|error| error.to_string())
    }

    fn record_attempt_internal(&self, attempt: &AttemptRecord) -> Result<(), String> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| "sqlite mutex poisoned".to_owned())?;
        let attempt_id = attempt.attempt_id.to_string();
        connection
            .execute(
                "INSERT INTO usage_attempts (id, request_id, route_id, outcome, failure_category)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(id) DO UPDATE SET
               outcome = excluded.outcome,
               failure_category = excluded.failure_category
             WHERE usage_attempts.request_id = excluded.request_id
               AND usage_attempts.route_id = excluded.route_id
               AND usage_attempts.outcome = 'committed'
               AND excluded.outcome IN ('succeeded', 'partial_failed', 'cancelled')",
                params![
                    attempt_id,
                    attempt.request_id,
                    attempt.route_id,
                    attempt.outcome.as_str(),
                    attempt.failure_category
                ],
            )
            .map_err(|error| error.to_string())?;
        let identity_matches = connection
            .query_row(
                "SELECT request_id = ?2 AND route_id = ?3 FROM usage_attempts WHERE id = ?1",
                params![
                    attempt.attempt_id.to_string(),
                    attempt.request_id,
                    attempt.route_id
                ],
                |row| row.get::<_, bool>(0),
            )
            .map_err(|error| error.to_string())?;
        if !identity_matches {
            return Err("attempt identity does not match the persisted attempt".into());
        }
        Ok(())
    }

    fn mark_route_success_internal(&self, route_id: &str) -> Result<(), String> {
        self.connection
            .lock()
            .map_err(|_| "sqlite mutex poisoned".to_owned())?
            .execute(
                "UPDATE provider_route_state SET state = 'closed', reason = NULL, retry_at = NULL WHERE route_id = ?1",
                [route_id],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }
}

impl SqliteStore {
    fn mark_route_failure_internal(
        &self,
        route_id: &str,
        failure: &ProviderFailure,
    ) -> Result<(), String> {
        let state = if failure.category.blocks_account() || failure.category.opens_route() {
            "open"
        } else {
            "closed"
        };
        self.connection.lock().map_err(|_| "sqlite mutex poisoned".to_owned())?.execute("UPDATE provider_route_state SET state = ?2, reason = ?3, retry_at = ?4 WHERE route_id = ?1", params![route_id, state, format!("{:?}", failure.category), failure.retry_at_unix]).map_err(|error| error.to_string())?;
        Ok(())
    }

    fn mark_account_failure_internal(
        &self,
        account_id: &str,
        failure: &ProviderFailure,
    ) -> Result<(), String> {
        self.connection
            .lock()
            .map_err(|_| "sqlite mutex poisoned".to_owned())?
            .execute(
                "UPDATE provider_account_state SET state = 'blocked', reason = ?2, retry_at = ?3 WHERE account_id = ?1",
                params![account_id, format!("{:?}", failure.category), failure.retry_at_unix],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }
}

#[async_trait]
impl ConfigurationRepository for SqliteStore {
    async fn authenticate(&self, raw_key: &str) -> Result<bool, String> {
        SqliteStore::authenticate_internal(self, raw_key)
    }

    async fn candidates(&self, logical_model: &str) -> Result<Vec<RouteCandidate>, String> {
        SqliteStore::candidates_internal(self, logical_model)
    }

    async fn record_attempt(&self, attempt: AttemptRecord) -> Result<(), String> {
        SqliteStore::record_attempt_internal(self, &attempt)
    }

    async fn mark_route_success(&self, route_id: &str) -> Result<(), String> {
        SqliteStore::mark_route_success_internal(self, route_id)
    }

    async fn mark_route_failure(
        &self,
        route_id: &str,
        failure: &ProviderFailure,
    ) -> Result<(), String> {
        SqliteStore::mark_route_failure_internal(self, route_id, failure)
    }

    async fn mark_account_failure(
        &self,
        account_id: &str,
        failure: &ProviderFailure,
    ) -> Result<(), String> {
        SqliteStore::mark_account_failure_internal(self, account_id, failure)
    }
}

impl SqliteStore {
    fn record_quota_snapshot_internal(
        &self,
        snapshot: &ProviderQuotaSnapshot,
    ) -> Result<(), String> {
        snapshot.validate().map_err(|error| error.to_string())?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| "sqlite mutex poisoned".to_owned())?;
        connection
            .execute(
                "INSERT INTO provider_quota_snapshots (
                   id, provider_account_id, constraint_id, unit_kind, currency_code, custom_name,
                   allowance_unscaled, allowance_scale, consumed_unscaled, consumed_scale,
                   remaining_unscaled, remaining_scale, reset_at, observed_at, fresh_until,
                   source_id, evidence_version
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)
                 ON CONFLICT(id) DO NOTHING",
                params![
                    snapshot.snapshot_id,
                    snapshot.provider_account_id,
                    snapshot.constraint_id,
                    unit_kind_name(snapshot.unit.kind),
                    snapshot.unit.currency_code,
                    snapshot.unit.custom_name,
                    snapshot.allowance.map(|value| value.unscaled),
                    snapshot.allowance.map(|value| i64::from(value.scale)),
                    snapshot.consumed.map(|value| value.unscaled),
                    snapshot.consumed.map(|value| i64::from(value.scale)),
                    snapshot.remaining.map(|value| value.unscaled),
                    snapshot.remaining.map(|value| i64::from(value.scale)),
                    snapshot.reset_at_unix,
                    snapshot.observed_at_unix,
                    snapshot.fresh_until_unix,
                    snapshot.source.source_id,
                    snapshot.source.evidence_version,
                ],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    fn record_billing_record_internal(&self, record: &ProviderBillingRecord) -> Result<(), String> {
        record.validate().map_err(|error| error.to_string())?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| "sqlite mutex poisoned".to_owned())?;
        let transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let charge = record.charge.as_ref();
        transaction
            .execute(
                "INSERT INTO provider_billing_records (
                   id, attempt_id, provider_account_id, provider_request_id, charge_unit_kind,
                   charge_currency_code, charge_custom_name, charge_unscaled, charge_scale,
                   observed_at, fresh_until, source_id, evidence_version
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                 ON CONFLICT(id) DO NOTHING",
                params![
                    record.record_id,
                    record.attempt_id.to_string(),
                    record.provider_account_id,
                    record.provider_request_id,
                    charge.map(|value| unit_kind_name(value.unit.kind)),
                    charge.and_then(|value| value.unit.currency_code.as_deref()),
                    charge.and_then(|value| value.unit.custom_name.as_deref()),
                    charge.map(|value| value.value.unscaled),
                    charge.map(|value| i64::from(value.value.scale)),
                    record.observed_at_unix,
                    record.fresh_until_unix,
                    record.source.source_id,
                    record.source.evidence_version,
                ],
            )
            .map_err(|error| error.to_string())?;
        for (index, quantity) in record.billed_units.iter().enumerate() {
            let index = i64::try_from(index)
                .map_err(|_| "too many provider-reported billing units".to_owned())?;
            transaction
                .execute(
                    "INSERT INTO provider_billing_units (
                       billing_record_id, unit_index, unit_kind, currency_code, custom_name,
                       value_unscaled, value_scale
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                     ON CONFLICT(billing_record_id, unit_index) DO NOTHING",
                    params![
                        record.record_id,
                        index,
                        unit_kind_name(quantity.unit.kind),
                        quantity.unit.currency_code,
                        quantity.unit.custom_name,
                        quantity.value.unscaled,
                        i64::from(quantity.value.scale),
                    ],
                )
                .map_err(|error| error.to_string())?;
        }
        transaction.commit().map_err(|error| error.to_string())?;
        Ok(())
    }

    fn quota_snapshots_internal(
        &self,
        provider_account_id: &str,
    ) -> Result<Vec<ProviderQuotaSnapshot>, String> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| "sqlite mutex poisoned".to_owned())?;
        let mut statement = connection
            .prepare(
                "SELECT id, provider_account_id, constraint_id, unit_kind, currency_code, custom_name,
                        allowance_unscaled, allowance_scale, consumed_unscaled, consumed_scale,
                        remaining_unscaled, remaining_scale, reset_at, observed_at, fresh_until,
                        source_id, evidence_version
                 FROM provider_quota_snapshots
                 WHERE provider_account_id = ?1
                 ORDER BY observed_at DESC, id DESC",
            )
            .map_err(|error| error.to_string())?;
        let rows = statement
            .query_map([provider_account_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                    row.get::<_, Option<i64>>(8)?,
                    row.get::<_, Option<i64>>(9)?,
                    row.get::<_, Option<i64>>(10)?,
                    row.get::<_, Option<i64>>(11)?,
                    row.get::<_, Option<i64>>(12)?,
                    row.get::<_, i64>(13)?,
                    row.get::<_, i64>(14)?,
                    row.get::<_, String>(15)?,
                    row.get::<_, Option<String>>(16)?,
                ))
            })
            .map_err(|error| error.to_string())?;
        rows.map(|row| {
            let (
                snapshot_id,
                provider_account_id,
                constraint_id,
                kind,
                currency_code,
                custom_name,
                allowance_unscaled,
                allowance_scale,
                consumed_unscaled,
                consumed_scale,
                remaining_unscaled,
                remaining_scale,
                reset_at_unix,
                observed_at_unix,
                fresh_until_unix,
                source_id,
                evidence_version,
            ) = row.map_err(|error| error.to_string())?;
            let snapshot = ProviderQuotaSnapshot {
                snapshot_id,
                provider_account_id,
                constraint_id,
                unit: provider_unit(&kind, currency_code, custom_name)?,
                allowance: fixed_decimal(allowance_unscaled, allowance_scale)?,
                consumed: fixed_decimal(consumed_unscaled, consumed_scale)?,
                remaining: fixed_decimal(remaining_unscaled, remaining_scale)?,
                reset_at_unix,
                observed_at_unix,
                fresh_until_unix,
                source: AuthoritativeSource {
                    source_id,
                    evidence_version,
                },
            };
            snapshot.validate().map_err(|error| error.to_string())?;
            Ok(snapshot)
        })
        .collect()
    }

    fn billing_records_internal(
        &self,
        attempt_id: &AttemptId,
    ) -> Result<Vec<ProviderBillingRecord>, String> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| "sqlite mutex poisoned".to_owned())?;
        let mut records_statement = connection
            .prepare(
                "SELECT id, attempt_id, provider_account_id, provider_request_id, charge_unit_kind,
                        charge_currency_code, charge_custom_name, charge_unscaled, charge_scale,
                        observed_at, fresh_until, source_id, evidence_version
                 FROM provider_billing_records WHERE attempt_id = ?1 ORDER BY observed_at, id",
            )
            .map_err(|error| error.to_string())?;
        let record_rows = records_statement
            .query_map([attempt_id.to_string()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                    row.get::<_, Option<i64>>(8)?,
                    row.get::<_, i64>(9)?,
                    row.get::<_, i64>(10)?,
                    row.get::<_, String>(11)?,
                    row.get::<_, Option<String>>(12)?,
                ))
            })
            .map_err(|error| error.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        let mut records = Vec::with_capacity(record_rows.len());
        for (
            record_id,
            stored_attempt_id,
            provider_account_id,
            provider_request_id,
            charge_kind,
            charge_currency_code,
            charge_custom_name,
            charge_unscaled,
            charge_scale,
            observed_at_unix,
            fresh_until_unix,
            source_id,
            evidence_version,
        ) in record_rows
        {
            let billed_units = load_billed_units(&connection, &record_id)?;
            let charge = match charge_kind {
                Some(kind) => Some(ProviderReportedQuantity {
                    unit: provider_unit(&kind, charge_currency_code, charge_custom_name)?,
                    value: fixed_decimal(charge_unscaled, charge_scale)?
                        .ok_or_else(|| "missing provider-reported charge".to_owned())?,
                }),
                None => None,
            };
            let record = ProviderBillingRecord {
                record_id,
                attempt_id: AttemptId::parse(&stored_attempt_id)
                    .map_err(|_| "invalid stored attempt identifier".to_owned())?,
                provider_account_id,
                provider_request_id,
                billed_units,
                charge,
                observed_at_unix,
                fresh_until_unix,
                source: AuthoritativeSource {
                    source_id,
                    evidence_version,
                },
            };
            record.validate().map_err(|error| error.to_string())?;
            records.push(record);
        }
        Ok(records)
    }
}

#[async_trait]
impl AuthoritativeAccountingRepository for SqliteStore {
    async fn record_quota_snapshot(&self, snapshot: ProviderQuotaSnapshot) -> Result<(), String> {
        SqliteStore::record_quota_snapshot_internal(self, &snapshot)
    }

    async fn record_billing_record(&self, record: ProviderBillingRecord) -> Result<(), String> {
        SqliteStore::record_billing_record_internal(self, &record)
    }

    async fn quota_snapshots(
        &self,
        provider_account_id: &str,
    ) -> Result<Vec<ProviderQuotaSnapshot>, String> {
        SqliteStore::quota_snapshots_internal(self, provider_account_id)
    }

    async fn billing_records(
        &self,
        attempt_id: &AttemptId,
    ) -> Result<Vec<ProviderBillingRecord>, String> {
        SqliteStore::billing_records_internal(self, attempt_id)
    }
}

fn load_billed_units(
    connection: &Connection,
    record_id: &str,
) -> Result<Vec<ProviderReportedQuantity>, String> {
    let mut statement = connection
        .prepare(
            "SELECT unit_kind, currency_code, custom_name, value_unscaled, value_scale
             FROM provider_billing_units WHERE billing_record_id = ?1 ORDER BY unit_index",
        )
        .map_err(|error| error.to_string())?;
    statement
        .query_map([record_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })
        .map_err(|error| error.to_string())?
        .map(|row| {
            let (kind, currency_code, custom_name, unscaled, scale) =
                row.map_err(|error| error.to_string())?;
            Ok(ProviderReportedQuantity {
                unit: provider_unit(&kind, currency_code, custom_name)?,
                value: fixed_decimal(Some(unscaled), Some(scale))?
                    .ok_or_else(|| "missing billing unit value".to_owned())?,
            })
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use rusqlite::params;
    use slg_domain::AttemptId;
    use slg_ports::{AttemptOutcome, AttemptRecord};

    use super::SqliteStore;
    use slg_ports::ConfigurationRepository;

    #[tokio::test]
    async fn authenticates_and_orders_routes() {
        let store = SqliteStore::in_memory().unwrap();
        let key = store.create_gateway_key("test").unwrap();
        store.create_model("main").unwrap();
        store
            .create_account(
                "account",
                "openai-compatible",
                "env:TEST_KEY",
                "https://example.test",
            )
            .unwrap();
        store
            .create_route("slow", "main", "account", "model", 2)
            .unwrap();
        store
            .create_route("fast", "main", "account", "model", 1)
            .unwrap();
        assert!(store.authenticate(&key).await.unwrap());
        assert_eq!(store.candidates("main").await.unwrap()[0].route_id, "fast");
    }

    #[test]
    fn rejects_literal_credential_references_before_persistence() {
        let store = SqliteStore::in_memory().unwrap();
        store.create_model("main").unwrap();
        let literal = "sk-live-must-not-be-persisted";
        let error = store
            .create_account(
                "account",
                "openai-compatible",
                literal,
                "https://example.test",
            )
            .unwrap_err();
        assert!(error.contains("env:NAME"));
        assert!(!error.contains(literal));

        store
            .create_account(
                "account",
                "openai-compatible",
                "env:PROVIDER_API_KEY",
                "https://example.test",
            )
            .unwrap();
    }

    #[test]
    fn schema_rejects_invalid_credential_references_from_direct_writes() {
        let store = SqliteStore::in_memory().unwrap();
        let connection = store.connection.lock().unwrap();
        let literal = "sk-live-must-not-be-persisted";
        let error = connection
            .execute(
                "INSERT INTO provider_accounts (id, provider, credential_ref, base_url) VALUES (?1, ?2, ?3, ?4)",
                params!["account", "openai-compatible", literal, "https://example.test"],
            )
            .unwrap_err();
        assert!(error.to_string().contains("CHECK constraint failed"));
        assert!(!error.to_string().contains(literal));
    }

    #[tokio::test]
    async fn attempt_transitions_are_monotonic_and_idempotent() {
        let store = SqliteStore::in_memory().unwrap();
        let committed = AttemptRecord {
            attempt_id: AttemptId::new(),
            request_id: "request".into(),
            route_id: "route".into(),
            outcome: AttemptOutcome::Committed,
            failure_category: None,
        };

        store.record_attempt(committed.clone()).await.unwrap();
        let mut succeeded = committed.clone();
        succeeded.outcome = AttemptOutcome::Succeeded;
        store.record_attempt(succeeded.clone()).await.unwrap();
        store.record_attempt(succeeded).await.unwrap();
        let mut stale_failure = committed.clone();
        stale_failure.outcome = AttemptOutcome::PartialFailed;
        stale_failure.failure_category = Some("ProviderUnavailable".into());
        store.record_attempt(stale_failure).await.unwrap();

        let connection = store.connection.lock().unwrap();
        let (count, outcome, failure_category): (i64, String, Option<String>) = connection
            .query_row(
                "SELECT COUNT(*), outcome, failure_category FROM usage_attempts WHERE id = ?1",
                [committed.attempt_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(outcome, "succeeded");
        assert_eq!(failure_category, None);
    }
}
