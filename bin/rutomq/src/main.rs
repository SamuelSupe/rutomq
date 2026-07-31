use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use rutomq_agent::{AgentConfig, Broker, DEFAULT_LOG_FILTER, Metrics, serve_admin};
use rutomq_control::{MemoryMetadataStore, MetadataStore, PostgresMetadataStore};
use rutomq_storage::{ObjectStore, OpenDalObjectStore, S3Config};
use std::sync::Arc;
use tracing::info;

#[derive(Debug, Parser)]
#[command(
    name = "rutomq",
    version,
    about = "Diskless Kafka-compatible message queue"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Agent,
    Migrate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObjectStoreBackend {
    Memory,
    S3,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| DEFAULT_LOG_FILTER.to_owned()),
        )
        .json()
        .init();

    match Cli::parse().command {
        Command::Agent => run_agent().await,
        Command::Migrate => run_migrate().await,
    }
}

async fn run_agent() -> Result<()> {
    let config = AgentConfig::from_env().context("load agent configuration")?;
    let metadata: Arc<dyn MetadataStore> = if let Ok(database_url) = std::env::var("DATABASE_URL") {
        let store = PostgresMetadataStore::connect(&database_url)
            .await
            .context("connect PostgreSQL")?;
        store.migrate().await.context("run PostgreSQL migrations")?;
        Arc::new(store)
    } else {
        info!("DATABASE_URL is not configured; using in-memory metadata (development only)");
        Arc::new(MemoryMetadataStore::new())
    };
    let objects: Arc<dyn ObjectStore> = match object_store_backend_from_env()? {
        ObjectStoreBackend::S3 => Arc::new(OpenDalObjectStore::s3(s3_config_from_env()?)?),
        ObjectStoreBackend::Memory => {
            info!("using in-memory objects (development only)");
            Arc::new(OpenDalObjectStore::memory()?)
        }
    };
    let metrics = Arc::new(Metrics::new()?);
    metadata.check().await.context("check metadata store")?;
    objects.check().await.context("check object store")?;
    metrics.set_ready(true);
    let broker = Broker::new(metadata, objects, config.clone(), metrics.clone());
    let (shutdown_sender, shutdown) = tokio::sync::watch::channel(false);
    let kafka = broker.serve(shutdown.clone());
    let admin = serve_admin(metrics.clone(), &config, shutdown);
    tokio::pin!(kafka);
    tokio::pin!(admin);
    tokio::select! {
        result = &mut kafka => return result,
        result = &mut admin => return result,
        result = shutdown_signal() => result?,
    }
    metrics.set_ready(false);
    let _ = shutdown_sender.send(true);
    let (kafka_result, admin_result) = tokio::join!(&mut kafka, &mut admin);
    kafka_result?;
    admin_result?;
    info!("agent shutdown completed");
    Ok(())
}

async fn run_migrate() -> Result<()> {
    let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL is required")?;
    let store = PostgresMetadataStore::connect(&database_url).await?;
    store.migrate().await?;
    info!("database migrations completed");
    Ok(())
}

fn object_store_backend_from_env() -> Result<ObjectStoreBackend> {
    match std::env::var("OBJECT_STORE_BACKEND") {
        Ok(value) => parse_object_store_backend(&value),
        Err(std::env::VarError::NotPresent) => Ok(ObjectStoreBackend::Memory),
        Err(error) => Err(error).context("read OBJECT_STORE_BACKEND"),
    }
}

fn parse_object_store_backend(value: &str) -> Result<ObjectStoreBackend> {
    if value.eq_ignore_ascii_case("memory") {
        Ok(ObjectStoreBackend::Memory)
    } else if value.eq_ignore_ascii_case("s3") {
        Ok(ObjectStoreBackend::S3)
    } else {
        bail!("OBJECT_STORE_BACKEND must be memory or s3")
    }
}

fn s3_config_from_env() -> Result<S3Config> {
    Ok(S3Config {
        bucket: env_or("S3_BUCKET", "rutomq"),
        root: env_or("S3_ROOT", "rutomq"),
        region: env_or("S3_REGION", "us-east-1"),
        endpoint: std::env::var("S3_ENDPOINT")
            .ok()
            .filter(|value| !value.is_empty()),
        access_key_id: std::env::var("S3_ACCESS_KEY_ID").ok(),
        secret_access_key: std::env::var("S3_SECRET_ACCESS_KEY").ok(),
        write_chunk_bytes: env_usize(
            "RUTOMQ_OBJECT_WRITE_CHUNK_BYTES",
            rutomq_storage::DEFAULT_WRITE_CHUNK_BYTES,
        )?,
        write_concurrency: env_usize(
            "RUTOMQ_OBJECT_WRITE_CONCURRENCY",
            rutomq_storage::DEFAULT_WRITE_CONCURRENCY,
        )?,
    })
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn env_usize(name: &str, default: usize) -> Result<usize> {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("{name} must be an unsigned integer")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("read {name}")),
    }
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .context("install SIGTERM handler")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("wait for Ctrl-C")?,
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c()
        .await
        .context("wait for shutdown signal")?;

    info!("agent shutdown requested");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ObjectStoreBackend, parse_object_store_backend};

    #[test]
    fn object_store_backend_rejects_unknown_values() {
        assert_eq!(
            parse_object_store_backend("memory").unwrap(),
            ObjectStoreBackend::Memory
        );
        assert_eq!(
            parse_object_store_backend("S3").unwrap(),
            ObjectStoreBackend::S3
        );
        assert!(parse_object_store_backend("s33").is_err());
        assert!(parse_object_store_backend("").is_err());
    }
}
