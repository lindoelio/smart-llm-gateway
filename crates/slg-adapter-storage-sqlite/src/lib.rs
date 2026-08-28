//! `SQLite` control plane for a single gateway process.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, params, types::Type};
use sha2::{Digest, Sha256};
use slg_domain::{CredentialReference, ProviderFailure, RouteCandidate, candidate_plan};
use slg_ports::{AttemptRecord, ConfigurationRepository};
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
        connection.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS gateway_keys (
              id TEXT PRIMARY KEY, key_hash TEXT NOT NULL UNIQUE, description TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS logical_models (name TEXT PRIMARY KEY, enabled INTEGER NOT NULL DEFAULT 1);
            CREATE TABLE IF NOT EXISTS provider_accounts (
              id TEXT PRIMARY KEY,
              provider TEXT NOT NULL,
              credential_ref TEXT NOT NULL CHECK (
                credential_ref GLOB 'env:[A-Za-z_]*'
                AND substr(credential_ref, 5) NOT GLOB '*[^A-Za-z0-9_]*'
              ),
              base_url TEXT NOT NULL,
              enabled INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS provider_routes (
              id TEXT PRIMARY KEY, logical_model TEXT NOT NULL REFERENCES logical_models(name), provider_account_id TEXT NOT NULL REFERENCES provider_accounts(id), upstream_model TEXT NOT NULL, priority INTEGER NOT NULL, enabled INTEGER NOT NULL DEFAULT 1,
              UNIQUE(logical_model, priority)
            );
            CREATE TABLE IF NOT EXISTS model_fallbacks (
              source_model TEXT NOT NULL REFERENCES logical_models(name), target_model TEXT NOT NULL REFERENCES logical_models(name), priority INTEGER NOT NULL,
              PRIMARY KEY(source_model, priority)
            );
            CREATE TABLE IF NOT EXISTS provider_route_state (
              route_id TEXT PRIMARY KEY REFERENCES provider_routes(id), state TEXT NOT NULL DEFAULT 'closed', reason TEXT, retry_at INTEGER
            );
            CREATE TABLE IF NOT EXISTS provider_account_state (
              account_id TEXT PRIMARY KEY REFERENCES provider_accounts(id), state TEXT NOT NULL DEFAULT 'unknown', reason TEXT, retry_at INTEGER
            );
            CREATE TABLE IF NOT EXISTS usage_attempts (
              id TEXT PRIMARY KEY, request_id TEXT NOT NULL, route_id TEXT NOT NULL, outcome TEXT NOT NULL, failure_category TEXT, observed_at INTEGER NOT NULL DEFAULT (unixepoch())
            );
            ",
        ).map_err(|error| error.to_string())
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

#[async_trait]
impl ConfigurationRepository for SqliteStore {
    async fn authenticate(&self, raw_key: &str) -> Result<bool, String> {
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

    async fn candidates(&self, logical_model: &str) -> Result<Vec<RouteCandidate>, String> {
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

    async fn record_attempt(&self, attempt: AttemptRecord) -> Result<(), String> {
        self.connection.lock().map_err(|_| "sqlite mutex poisoned".to_owned())?.execute("INSERT INTO usage_attempts (id, request_id, route_id, outcome, failure_category) VALUES (?1, ?2, ?3, ?4, ?5)", params![Uuid::new_v4().to_string(), attempt.request_id, attempt.route_id, attempt.outcome, attempt.failure_category]).map_err(|error| error.to_string())?;
        Ok(())
    }

    async fn mark_route_success(&self, route_id: &str) -> Result<(), String> {
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

    async fn mark_route_failure(
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

    async fn mark_account_failure(
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

#[cfg(test)]
mod tests {
    use rusqlite::params;

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
}
