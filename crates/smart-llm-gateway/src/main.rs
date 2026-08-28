use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Args, Parser, Subcommand};
use slg_adapter_provider::OpenAiCompatibleExecutor;
use slg_adapter_secrets::EnvironmentSecretResolver;
use slg_adapter_storage_postgres::PostgresStore;
use slg_adapter_storage_sqlite::SqliteStore;
use slg_application::Gateway;

#[derive(Debug, Parser)]
#[command(name = "gateway", version, about = "Smart LLM Gateway")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Init(DatabaseArgs),
    #[command(subcommand)]
    Key(KeyCommand),
    #[command(subcommand)]
    Model(ModelCommand),
    #[command(subcommand)]
    Account(AccountCommand),
    #[command(subcommand)]
    Route(RouteCommand),
    #[command(subcommand)]
    Fallback(FallbackCommand),
    Serve(ServeArgs),
}

#[derive(Debug, Args)]
struct DatabaseArgs {
    #[arg(long, default_value = "smart-llm-gateway.sqlite")]
    database: String,
}
#[derive(Debug, Args)]
struct ServeArgs {
    #[arg(long, default_value = "smart-llm-gateway.sqlite")]
    database: String,
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: SocketAddr,
    #[arg(long, default_value = ".smart-llm-gateway-runtime")]
    state_dir: PathBuf,
}
#[derive(Debug, Subcommand)]
enum KeyCommand {
    Create {
        #[command(flatten)]
        database: DatabaseArgs,
        #[arg(long, default_value = "default")]
        description: String,
    },
}
#[derive(Debug, Subcommand)]
enum ModelCommand {
    Create {
        #[command(flatten)]
        database: DatabaseArgs,
        name: String,
    },
}
#[derive(Debug, Subcommand)]
enum AccountCommand {
    Create {
        #[command(flatten)]
        database: DatabaseArgs,
        id: String,
        #[arg(long, default_value = "openai-compatible")]
        provider: String,
        #[arg(long)]
        credential_env: String,
        #[arg(long)]
        base_url: String,
    },
}
#[derive(Debug, Subcommand)]
enum RouteCommand {
    Add {
        #[command(flatten)]
        database: DatabaseArgs,
        id: String,
        #[arg(long)]
        model: String,
        #[arg(long)]
        account: String,
        #[arg(long)]
        upstream_model: String,
        #[arg(long, default_value_t = 1)]
        priority: u32,
    },
}
#[derive(Debug, Subcommand)]
enum FallbackCommand {
    Add {
        #[command(flatten)]
        database: DatabaseArgs,
        #[arg(long)]
        source: String,
        #[arg(long)]
        target: String,
        #[arg(long, default_value_t = 1)]
        priority: u32,
    },
}

#[tokio::main]
async fn main() {
    if let Err(error) = run(Cli::parse()).await {
        eprintln!("gateway: {error}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), String> {
    match cli.command {
        Command::Init(arguments) => {
            let store = open_store(&arguments.database).await?;
            print_json(serde_json::json!({"ok": true, "database": store.kind()}));
        }
        Command::Key(KeyCommand::Create {
            database,
            description,
        }) => {
            let key = open_store(&database.database)
                .await?
                .create_gateway_key(&description)
                .await?;
            print_json(
                serde_json::json!({"key": key, "warning": "Store this value now; it is not recoverable."}),
            );
        }
        Command::Model(ModelCommand::Create { database, name }) => {
            open_store(&database.database)
                .await?
                .create_model(&name)
                .await?;
            print_json(serde_json::json!({"ok": true, "model": name}));
        }
        Command::Account(AccountCommand::Create {
            database,
            id,
            provider,
            credential_env,
            base_url,
        }) => {
            let credential_reference = validate_environment_reference(&credential_env)?;
            open_store(&database.database)
                .await?
                .create_account(&id, &provider, credential_reference, &base_url)
                .await?;
            print_json(serde_json::json!({"ok": true, "account": id}));
        }
        Command::Route(RouteCommand::Add {
            database,
            id,
            model,
            account,
            upstream_model,
            priority,
        }) => {
            open_store(&database.database)
                .await?
                .create_route(&id, &model, &account, &upstream_model, priority)
                .await?;
            print_json(serde_json::json!({"ok": true, "route": id}));
        }
        Command::Fallback(FallbackCommand::Add {
            database,
            source,
            target,
            priority,
        }) => {
            open_store(&database.database)
                .await?
                .add_fallback(&source, &target, priority)
                .await?;
            print_json(serde_json::json!({"ok": true, "source": source, "target": target}));
        }
        Command::Serve(arguments) => serve(arguments).await?,
    }
    Ok(())
}

async fn serve(arguments: ServeArgs) -> Result<(), String> {
    let spool =
        slg_runtime::DurableUsageSpool::open(arguments.state_dir.join("usage-spool.sqlite"))?;
    if is_postgres_database(&arguments.database) {
        serve_postgres(arguments, spool).await
    } else {
        println!("{{\"listening\":\"{}\"}}", arguments.bind);
        match open_store(&arguments.database).await? {
            Store::Sqlite(store) => {
                slg_runtime::serve(
                    slg_adapter_inbound_openai::router(Arc::new(
                        Gateway::new(
                            store,
                            OpenAiCompatibleExecutor::new(),
                            EnvironmentSecretResolver,
                        )
                        .with_usage_spool(spool),
                    )),
                    arguments.bind,
                )
                .await
            }
            Store::Postgres(_) => Err("unexpected PostgreSQL store selection".into()),
        }
    }
}

async fn serve_postgres(
    arguments: ServeArgs,
    spool: slg_runtime::DurableUsageSpool,
) -> Result<(), String> {
    let snapshot_store =
        slg_runtime::LastKnownGoodSnapshot::new(arguments.state_dir.join("last-known-good.json"));
    match PostgresStore::connect(&arguments.database).await {
        Ok(primary) => {
            let snapshot = control_snapshot(primary.control_snapshot_data().await?);
            snapshot_store.save(&snapshot)?;
            let fallback = slg_runtime::SnapshotRepository::new(snapshot, spool.clone());
            let _ = slg_runtime::flush_usage_spool(&spool, &primary, 100).await?;
            slg_runtime::spawn_usage_spool_worker(spool, primary.clone());
            println!(
                "{{\"listening\":\"{}\",\"control_plane\":\"postgres\"}}",
                arguments.bind
            );
            slg_runtime::serve(
                slg_adapter_inbound_openai::router(Arc::new(Gateway::new(
                    slg_runtime::SnapshotFallback::new(primary, fallback),
                    OpenAiCompatibleExecutor::new(),
                    EnvironmentSecretResolver,
                ))),
                arguments.bind,
            )
            .await
        }
        Err(primary_error) => {
            let snapshot = snapshot_store.load().map_err(|snapshot_error| {
                format!(
                    "PostgreSQL control plane is unavailable ({primary_error}); no valid last-known-good snapshot is available ({snapshot_error})"
                )
            })?;
            println!(
                "{{\"listening\":\"{}\",\"control_plane\":\"last_known_good\"}}",
                arguments.bind
            );
            slg_runtime::serve(
                slg_adapter_inbound_openai::router(Arc::new(Gateway::new(
                    slg_runtime::SnapshotRepository::new(snapshot, spool),
                    OpenAiCompatibleExecutor::new(),
                    EnvironmentSecretResolver,
                ))),
                arguments.bind,
            )
            .await
        }
    }
}

fn control_snapshot(
    data: slg_adapter_storage_postgres::ControlSnapshotData,
) -> slg_runtime::ControlSnapshot {
    slg_runtime::ControlSnapshot {
        gateway_key_hashes: data.gateway_key_hashes.into_iter().collect(),
        routes: data.routes,
        fallbacks: data.fallbacks,
        route_states: data
            .route_states
            .into_iter()
            .map(|(route_id, (state, retry_at_unix))| {
                (
                    route_id,
                    slg_runtime::SnapshotRouteState {
                        state,
                        retry_at_unix,
                    },
                )
            })
            .collect(),
        account_states: data
            .account_states
            .into_iter()
            .map(|(account_id, (state, retry_at_unix))| {
                (
                    account_id,
                    slg_runtime::SnapshotAccountState {
                        state,
                        retry_at_unix,
                    },
                )
            })
            .collect(),
    }
}

fn is_postgres_database(database: &str) -> bool {
    database.starts_with("postgres://") || database.starts_with("postgresql://")
}

enum Store {
    Sqlite(SqliteStore),
    Postgres(PostgresStore),
}

impl Store {
    const fn kind(&self) -> &'static str {
        match self {
            Self::Sqlite(_) => "sqlite",
            Self::Postgres(_) => "postgres",
        }
    }
    async fn create_gateway_key(&self, description: &str) -> Result<String, String> {
        match self {
            Self::Sqlite(store) => store.create_gateway_key(description),
            Self::Postgres(store) => store.create_gateway_key(description).await,
        }
    }
    async fn create_model(&self, name: &str) -> Result<(), String> {
        match self {
            Self::Sqlite(store) => store.create_model(name),
            Self::Postgres(store) => store.create_model(name).await,
        }
    }
    async fn create_account(
        &self,
        id: &str,
        provider: &str,
        credential_env: &str,
        base_url: &str,
    ) -> Result<(), String> {
        match self {
            Self::Sqlite(store) => store.create_account(id, provider, credential_env, base_url),
            Self::Postgres(store) => {
                store
                    .create_account(id, provider, credential_env, base_url)
                    .await
            }
        }
    }
    async fn create_route(
        &self,
        id: &str,
        model: &str,
        account: &str,
        upstream_model: &str,
        priority: u32,
    ) -> Result<(), String> {
        match self {
            Self::Sqlite(store) => store.create_route(id, model, account, upstream_model, priority),
            Self::Postgres(store) => {
                store
                    .create_route(id, model, account, upstream_model, priority)
                    .await
            }
        }
    }
    async fn add_fallback(&self, source: &str, target: &str, priority: u32) -> Result<(), String> {
        match self {
            Self::Sqlite(store) => store.add_fallback(source, target, priority),
            Self::Postgres(store) => store.add_fallback(source, target, priority).await,
        }
    }
}

async fn open_store(database: &str) -> Result<Store, String> {
    if is_postgres_database(database) {
        PostgresStore::connect(database).await.map(Store::Postgres)
    } else {
        SqliteStore::open(database).map(Store::Sqlite)
    }
}

fn validate_environment_reference(value: &str) -> Result<&str, String> {
    let Some(name) = value.strip_prefix("env:") else {
        return Err("credential reference must use the explicit env:NAME format".into());
    };
    let mut characters = name.chars();
    let Some(first) = characters.next() else {
        return Err("credential reference must include an environment variable name".into());
    };
    if !(first == '_' || first.is_ascii_alphabetic())
        || !characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
    {
        return Err(
            "credential reference must use env:NAME with a valid environment variable name".into(),
        );
    }
    Ok(value)
}

#[allow(clippy::needless_pass_by_value)]
fn print_json(value: serde_json::Value) {
    println!(
        "{}",
        serde_json::to_string(&value).expect("serializable command result")
    );
}

#[cfg(test)]
mod tests {
    use super::validate_environment_reference;

    #[test]
    fn accepts_only_explicit_environment_references() {
        assert_eq!(
            validate_environment_reference("env:PROVIDER_API_KEY").unwrap(),
            "env:PROVIDER_API_KEY"
        );
        assert!(validate_environment_reference("PROVIDER_API_KEY").is_err());
        assert!(validate_environment_reference("sk-live-secret").is_err());
        assert!(validate_environment_reference("env:").is_err());
        assert!(validate_environment_reference("env:1INVALID").is_err());
        assert!(validate_environment_reference("env:INVALID-NAME").is_err());
    }
}
