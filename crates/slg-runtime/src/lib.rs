//! Process lifecycle helpers and local durability primitives.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use slg_domain::{ProviderFailure, RouteCandidate, candidate_plan};
use slg_ports::{AttemptRecord, ConfigurationRepository, UsageSpool};
use uuid::Uuid;

pub async fn serve(router: axum::Router, address: SocketAddr) -> Result<(), String> {
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .map_err(|error| error.to_string())?;
    axum::serve(listener, router)
        .await
        .map_err(|error| error.to_string())
}

/// Atomic, integrity-checked storage for a sanitized control-plane snapshot.
pub struct LastKnownGoodSnapshot<T> {
    path: PathBuf,
    marker: std::marker::PhantomData<T>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotEnvelope {
    schema: u8,
    payload: serde_json::Value,
    digest: String,
}

impl<T> LastKnownGoodSnapshot<T>
where
    T: Serialize + DeserializeOwned,
{
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            marker: std::marker::PhantomData,
        }
    }

    pub fn save(&self, snapshot: &T) -> Result<(), String> {
        let payload = serde_json::to_value(snapshot).map_err(|error| error.to_string())?;
        reject_sensitive_fields(&payload)?;
        let envelope = SnapshotEnvelope {
            schema: 1,
            digest: digest(&serde_json::to_vec(&payload).map_err(|error| error.to_string())?),
            payload,
        };
        let bytes = serde_json::to_vec(&envelope).map_err(|error| error.to_string())?;
        let parent = self
            .path
            .parent()
            .ok_or_else(|| "snapshot path has no parent".to_owned())?;
        let parent_was_missing = !parent.exists();
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        if parent_was_missing {
            restrict_directory(parent)?;
        }
        let temporary = parent.join(format!(
            ".{}.{}.tmp",
            self.path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("snapshot"),
            Uuid::new_v4()
        ));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|error| error.to_string())?;
        restrict_file(&file)?;
        std::io::Write::write_all(&mut file, &bytes).map_err(|error| error.to_string())?;
        file.sync_all().map_err(|error| error.to_string())?;
        fs::rename(&temporary, &self.path).map_err(|error| error.to_string())?;
        Ok(())
    }

    pub fn load(&self) -> Result<T, String> {
        let bytes = fs::read(&self.path).map_err(|error| error.to_string())?;
        let envelope: SnapshotEnvelope =
            serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
        if envelope.schema != 1 {
            return Err("unsupported snapshot schema".into());
        }
        let payload = serde_json::to_vec(&envelope.payload).map_err(|error| error.to_string())?;
        if digest(&payload) != envelope.digest {
            return Err("snapshot integrity check failed".into());
        }
        reject_sensitive_fields(&envelope.payload)?;
        serde_json::from_value(envelope.payload).map_err(|error| error.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpoolEntry {
    pub id: String,
    pub attempt: AttemptRecord,
}

/// Sanitized control-plane data retained for PostgreSQL outage recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlSnapshot {
    pub gateway_key_hashes: BTreeSet<String>,
    pub routes: Vec<RouteCandidate>,
    pub fallbacks: BTreeMap<String, Vec<String>>,
    pub route_states: BTreeMap<String, SnapshotRouteState>,
    pub account_states: BTreeMap<String, SnapshotAccountState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotRouteState {
    pub state: String,
    pub retry_at_unix: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotAccountState {
    pub state: String,
    pub retry_at_unix: Option<i64>,
}

#[derive(Clone)]
pub struct SnapshotRepository {
    snapshot: std::sync::Arc<ControlSnapshot>,
    runtime_state: std::sync::Arc<Mutex<SnapshotRuntimeState>>,
    usage_spool: DurableUsageSpool,
}

#[derive(Clone)]
struct SnapshotRuntimeState {
    route_states: BTreeMap<String, SnapshotRouteState>,
    account_states: BTreeMap<String, SnapshotAccountState>,
}

impl SnapshotRepository {
    #[must_use]
    pub fn new(snapshot: ControlSnapshot, usage_spool: DurableUsageSpool) -> Self {
        let runtime_state = SnapshotRuntimeState {
            route_states: snapshot.route_states.clone(),
            account_states: snapshot.account_states.clone(),
        };
        Self {
            snapshot: std::sync::Arc::new(snapshot),
            runtime_state: std::sync::Arc::new(Mutex::new(runtime_state)),
            usage_spool,
        }
    }
}

/// Uses the primary control plane while it is reachable, then conservatively
/// continues from the supplied local snapshot.
#[derive(Clone)]
pub struct SnapshotFallback<C> {
    primary: C,
    fallback: SnapshotRepository,
}

impl<C> SnapshotFallback<C> {
    #[must_use]
    pub const fn new(primary: C, fallback: SnapshotRepository) -> Self {
        Self { primary, fallback }
    }
}

/// A bounded local SQLite queue for usage evidence when the primary store fails.
#[derive(Clone)]
pub struct DurableUsageSpool {
    connection: Arc<Mutex<Connection>>,
}

impl DurableUsageSpool {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = path.as_ref();
        let parent = path
            .parent()
            .ok_or_else(|| "usage spool path has no parent".to_owned())?;
        let parent_was_missing = !parent.exists();
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        if parent_was_missing {
            restrict_directory(parent)?;
        }
        let connection = Connection::open(path).map_err(|error| error.to_string())?;
        restrict_path(path)?;
        connection.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS usage_spool (id TEXT PRIMARY KEY, payload TEXT NOT NULL, queued_at INTEGER NOT NULL DEFAULT (unixepoch()));").map_err(|error| error.to_string())?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    pub fn append(&self, attempt: AttemptRecord) -> Result<String, String> {
        let entry = SpoolEntry {
            id: attempt.attempt_id.to_string(),
            attempt,
        };
        let payload = serde_json::to_string(&entry).map_err(|error| error.to_string())?;
        self.connection
            .lock()
            .map_err(|_| "usage spool mutex poisoned".to_owned())?
            .execute(
                "INSERT INTO usage_spool (id, payload) VALUES (?1, ?2) ON CONFLICT(id) DO NOTHING",
                params![entry.id, payload],
            )
            .map_err(|error| error.to_string())?;
        Ok(entry.id)
    }

    pub fn pending(&self, limit: u32) -> Result<Vec<SpoolEntry>, String> {
        let limit = i64::from(limit);
        let connection = self
            .connection
            .lock()
            .map_err(|_| "usage spool mutex poisoned".to_owned())?;
        let mut statement = connection
            .prepare("SELECT payload FROM usage_spool ORDER BY queued_at, id LIMIT ?1")
            .map_err(|error| error.to_string())?;
        statement
            .query_map([limit], |row| row.get::<_, String>(0))
            .map_err(|error| error.to_string())?
            .map(|row| {
                serde_json::from_str(&row.map_err(|error| error.to_string())?)
                    .map_err(|error| error.to_string())
            })
            .collect()
    }

    pub fn acknowledge(&self, entry_ids: &[String]) -> Result<(), String> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| "usage spool mutex poisoned".to_owned())?;
        let transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        for id in entry_ids {
            transaction
                .execute("DELETE FROM usage_spool WHERE id = ?1", [id])
                .map_err(|error| error.to_string())?;
        }
        transaction.commit().map_err(|error| error.to_string())
    }
}

#[async_trait]
impl UsageSpool for DurableUsageSpool {
    async fn append_attempt(&self, attempt: AttemptRecord) -> Result<(), String> {
        self.append(attempt).map(|_| ())
    }
}

#[async_trait]
impl ConfigurationRepository for SnapshotRepository {
    async fn authenticate(&self, raw_key: &str) -> Result<bool, String> {
        Ok(self
            .snapshot
            .gateway_key_hashes
            .contains(&digest(raw_key.as_bytes())))
    }

    async fn candidates(&self, logical_model: &str) -> Result<Vec<RouteCandidate>, String> {
        let now = unix_time()?;
        let mut state = self
            .runtime_state
            .lock()
            .map_err(|_| "snapshot runtime state mutex poisoned".to_owned())?;
        let closed_routes = self
            .snapshot
            .routes
            .iter()
            .filter(|route| {
                state
                    .account_states
                    .get(&route.provider_account_id)
                    .is_none_or(|account| account.state != "blocked")
                    && state
                        .route_states
                        .get(&route.route_id)
                        .is_none_or(|route_state| route_state.state == "closed")
            })
            .cloned()
            .collect::<Vec<_>>();
        if let Ok(routes) = candidate_plan(logical_model, &closed_routes, &self.snapshot.fallbacks)
        {
            return Ok(routes);
        }
        let retryable_routes = self
            .snapshot
            .routes
            .iter()
            .filter(|route| {
                state
                    .account_states
                    .get(&route.provider_account_id)
                    .is_none_or(|account| account.state != "blocked")
                    && state
                        .route_states
                        .get(&route.route_id)
                        .is_some_and(|route_state| {
                            route_state.state == "open"
                                && route_state
                                    .retry_at_unix
                                    .is_some_and(|retry_at| retry_at <= now)
                        })
            })
            .cloned()
            .collect::<Vec<_>>();
        let probe = candidate_plan(logical_model, &retryable_routes, &self.snapshot.fallbacks)
            .map_err(|error| error.to_string())?
            .into_iter()
            .next()
            .ok_or_else(|| "no route eligible for half-open probe".to_owned())?;
        if let Some(route_state) = state.route_states.get_mut(&probe.route_id) {
            route_state.state = "half_open".into();
        }
        Ok(vec![probe])
    }

    async fn record_attempt(&self, attempt: AttemptRecord) -> Result<(), String> {
        self.usage_spool.append(attempt).map(|_| ())
    }

    async fn mark_route_success(&self, route_id: &str) -> Result<(), String> {
        let mut state = self
            .runtime_state
            .lock()
            .map_err(|_| "snapshot runtime state mutex poisoned".to_owned())?;
        if let Some(route_state) = state.route_states.get_mut(route_id) {
            route_state.state = "closed".into();
            route_state.retry_at_unix = None;
        }
        Ok(())
    }

    async fn mark_route_failure(
        &self,
        route_id: &str,
        failure: &ProviderFailure,
    ) -> Result<(), String> {
        if !failure.category.opens_route() {
            return Ok(());
        }
        let mut state = self
            .runtime_state
            .lock()
            .map_err(|_| "snapshot runtime state mutex poisoned".to_owned())?;
        if let Some(route_state) = state.route_states.get_mut(route_id) {
            route_state.state = "open".into();
            route_state.retry_at_unix = failure.retry_at_unix;
        }
        Ok(())
    }

    async fn mark_account_failure(
        &self,
        account_id: &str,
        failure: &ProviderFailure,
    ) -> Result<(), String> {
        if !failure.category.blocks_account() {
            return Ok(());
        }
        let mut state = self
            .runtime_state
            .lock()
            .map_err(|_| "snapshot runtime state mutex poisoned".to_owned())?;
        if let Some(account_state) = state.account_states.get_mut(account_id) {
            account_state.state = "blocked".into();
            account_state.retry_at_unix = failure.retry_at_unix;
        }
        Ok(())
    }
}

#[async_trait]
impl<C> ConfigurationRepository for SnapshotFallback<C>
where
    C: ConfigurationRepository,
{
    async fn authenticate(&self, raw_key: &str) -> Result<bool, String> {
        match self.primary.authenticate(raw_key).await {
            Ok(value) => Ok(value),
            Err(_) => self.fallback.authenticate(raw_key).await,
        }
    }

    async fn candidates(&self, logical_model: &str) -> Result<Vec<RouteCandidate>, String> {
        match self.primary.candidates(logical_model).await {
            Ok(routes) => Ok(routes),
            Err(_) => self.fallback.candidates(logical_model).await,
        }
    }

    async fn record_attempt(&self, attempt: AttemptRecord) -> Result<(), String> {
        match self.primary.record_attempt(attempt.clone()).await {
            Ok(()) => Ok(()),
            Err(_) => self.fallback.record_attempt(attempt).await,
        }
    }

    async fn mark_route_success(&self, route_id: &str) -> Result<(), String> {
        match self.primary.mark_route_success(route_id).await {
            Ok(()) => Ok(()),
            Err(_) => self.fallback.mark_route_success(route_id).await,
        }
    }

    async fn mark_route_failure(
        &self,
        route_id: &str,
        failure: &ProviderFailure,
    ) -> Result<(), String> {
        match self.primary.mark_route_failure(route_id, failure).await {
            Ok(()) => Ok(()),
            Err(_) => self.fallback.mark_route_failure(route_id, failure).await,
        }
    }

    async fn mark_account_failure(
        &self,
        account_id: &str,
        failure: &ProviderFailure,
    ) -> Result<(), String> {
        match self.primary.mark_account_failure(account_id, failure).await {
            Ok(()) => Ok(()),
            Err(_) => {
                self.fallback
                    .mark_account_failure(account_id, failure)
                    .await
            }
        }
    }
}

/// Delivers queued usage attempts and removes only records acknowledged by the
/// primary repository. Delivery intentionally remains at-least-once.
pub async fn flush_usage_spool<C>(
    spool: &DurableUsageSpool,
    primary: &C,
    limit: u32,
) -> Result<usize, String>
where
    C: ConfigurationRepository,
{
    let entries = spool.pending(limit)?;
    let mut acknowledged = Vec::new();
    for entry in entries {
        if primary.record_attempt(entry.attempt).await.is_err() {
            break;
        }
        acknowledged.push(entry.id);
    }
    let delivered = acknowledged.len();
    spool.acknowledge(&acknowledged)?;
    Ok(delivered)
}

pub fn spawn_usage_spool_worker<C>(spool: DurableUsageSpool, primary: C)
where
    C: ConfigurationRepository + Clone + Send + Sync + 'static,
{
    tokio::spawn(async move {
        loop {
            let _ = flush_usage_spool(&spool, &primary, 100).await;
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    });
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn unix_time() -> Result<i64, String> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| error.to_string())
        .and_then(|duration| i64::try_from(duration.as_secs()).map_err(|error| error.to_string()))
}

fn reject_sensitive_fields(value: &serde_json::Value) -> Result<(), String> {
    match value {
        serde_json::Value::Object(values) => {
            for (key, value) in values {
                if matches!(
                    key.as_str(),
                    "api_key" | "credential" | "prompt" | "content" | "response" | "secret"
                ) {
                    return Err(format!(
                        "snapshot contains forbidden sensitive field `{key}`"
                    ));
                }
                reject_sensitive_fields(value)?;
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                reject_sensitive_fields(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(unix)]
fn restrict_directory(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|error| error.to_string())
}
#[cfg(not(unix))]
fn restrict_directory(_: &Path) -> Result<(), String> {
    Ok(())
}
#[cfg(unix)]
fn restrict_file(file: &File) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|error| error.to_string())
}
#[cfg(not(unix))]
fn restrict_file(_: &File) -> Result<(), String> {
    Ok(())
}

#[cfg(unix)]
fn restrict_path(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|error| error.to_string())
}
#[cfg(not(unix))]
fn restrict_path(_: &Path) -> Result<(), String> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct AcknowledgingRepository;

    #[async_trait]
    impl ConfigurationRepository for AcknowledgingRepository {
        async fn authenticate(&self, _: &str) -> Result<bool, String> {
            Ok(false)
        }

        async fn candidates(&self, _: &str) -> Result<Vec<RouteCandidate>, String> {
            Ok(Vec::new())
        }

        async fn record_attempt(&self, _: AttemptRecord) -> Result<(), String> {
            Ok(())
        }

        async fn mark_route_success(&self, _: &str) -> Result<(), String> {
            Ok(())
        }

        async fn mark_route_failure(&self, _: &str, _: &ProviderFailure) -> Result<(), String> {
            Ok(())
        }

        async fn mark_account_failure(&self, _: &str, _: &ProviderFailure) -> Result<(), String> {
            Ok(())
        }
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct Snapshot {
        revision: u64,
        routes: Vec<String>,
    }

    fn path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("slg-runtime-{name}-{}.json", Uuid::new_v4()))
    }

    fn snapshot_path() -> PathBuf {
        std::env::temp_dir()
            .join(format!("slg-runtime-snapshot-{}", Uuid::new_v4()))
            .join("snapshot.json")
    }

    fn control_snapshot(raw_key: &str) -> ControlSnapshot {
        let route = RouteCandidate {
            route_id: "route-a".into(),
            logical_model: "model-a".into(),
            provider_account_id: "account-a".into(),
            provider: "provider-a".into(),
            credential_ref: slg_domain::CredentialReference::parse("env:PROVIDER_API_KEY").unwrap(),
            base_url: "https://provider.example.test".into(),
            upstream_model: "upstream-a".into(),
            priority: 1,
        };
        ControlSnapshot {
            gateway_key_hashes: BTreeSet::from([digest(raw_key.as_bytes())]),
            routes: vec![route],
            fallbacks: BTreeMap::new(),
            route_states: BTreeMap::from([(
                "route-a".into(),
                SnapshotRouteState {
                    state: "closed".into(),
                    retry_at_unix: None,
                },
            )]),
            account_states: BTreeMap::from([(
                "account-a".into(),
                SnapshotAccountState {
                    state: "available".into(),
                    retry_at_unix: None,
                },
            )]),
        }
    }

    #[test]
    fn snapshot_round_trip_checks_integrity() {
        let path = snapshot_path();
        let store = LastKnownGoodSnapshot::new(&path);
        let expected = Snapshot {
            revision: 3,
            routes: vec!["route-a".into()],
        };
        store.save(&expected).unwrap();
        assert_eq!(store.load().unwrap(), expected);
        fs::write(&path, "{}").unwrap();
        assert!(store.load().is_err());
        fs::remove_file(&path).unwrap();
        fs::remove_dir(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn snapshot_rejects_sensitive_content() {
        let path = snapshot_path();
        let store = LastKnownGoodSnapshot::new(&path);
        assert!(
            store
                .save(&serde_json::json!({"credential": "must-not-persist"}))
                .is_err()
        );
        assert!(!path.exists());
    }

    #[test]
    fn spool_delivers_at_least_once_until_acknowledged() {
        let path = path("spool");
        let spool = DurableUsageSpool::open(&path).unwrap();
        let attempt = AttemptRecord {
            attempt_id: slg_domain::AttemptId::new(),
            request_id: "request".into(),
            route_id: "route".into(),
            outcome: "succeeded".into(),
            failure_category: None,
        };
        let id = spool.append(attempt.clone()).unwrap();
        // The same outcome can be re-enqueued after a crash/retry, but must
        // retain one durable item until the primary store acknowledges it.
        assert_eq!(spool.append(attempt).unwrap(), id);
        drop(spool);

        let recovered = DurableUsageSpool::open(&path).unwrap();
        let pending = recovered.pending(10).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, id);
        recovered.acknowledge(&[id]).unwrap();
        assert!(recovered.pending(10).unwrap().is_empty());
        drop(recovered);
        fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn snapshot_repository_authenticates_routes_and_replays_spooled_usage() {
        let spool_path = path("snapshot-spool");
        let spool = DurableUsageSpool::open(&spool_path).unwrap();
        let snapshot = SnapshotRepository::new(control_snapshot("gateway-key"), spool.clone());

        assert!(snapshot.authenticate("gateway-key").await.unwrap());
        assert!(!snapshot.authenticate("different-key").await.unwrap());
        assert_eq!(
            snapshot.candidates("model-a").await.unwrap()[0].route_id,
            "route-a"
        );
        snapshot
            .record_attempt(AttemptRecord {
                attempt_id: slg_domain::AttemptId::new(),
                request_id: "attempt-a".into(),
                route_id: "route-a".into(),
                outcome: "succeeded".into(),
                failure_category: None,
            })
            .await
            .unwrap();
        assert_eq!(spool.pending(10).unwrap().len(), 1);
        assert_eq!(
            flush_usage_spool(&spool, &AcknowledgingRepository, 10)
                .await
                .unwrap(),
            1
        );
        assert!(spool.pending(10).unwrap().is_empty());
        fs::remove_file(spool_path).unwrap();
    }
}
