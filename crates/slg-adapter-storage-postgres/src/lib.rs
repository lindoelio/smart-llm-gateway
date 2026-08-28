//! Portable `PostgreSQL` control-plane adapter with no Neon-specific behavior.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use slg_domain::{CredentialReference, ProviderFailure, RouteCandidate, candidate_plan};
use slg_ports::{AttemptRecord, ConfigurationRepository};
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
            .batch_execute(
            "
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
            ",
        )
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

#[cfg(test)]
mod tests {
    use super::*;
    use slg_adapter_storage_sqlite::SqliteStore;

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
}
