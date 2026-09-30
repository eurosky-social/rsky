//! Re-verify the handles of specific actors now, bypassing the sweep's
//! cooldowns and queue order.
//!
//! Reads DIDs (one per line; blank lines and `#` comments ignored) from a file
//! or stdin and runs each through the indexer's own `index_handle` with
//! `force = true`: DID document -> handle -> bidirectional check -> actor row
//! (handle set and `handleResolveTries` reset on success, failure recorded
//! otherwise). For fixing reported stale handles without waiting for the sweep,
//! and without nudging the account's PDS, which writes a PLC operation per DID.
//!
//! Prints one `did<TAB>verified|unverified|error: ...` line per DID on stdout
//! and a summary on stderr.

use std::io::{BufRead, BufReader};
use std::sync::Arc;

use clap::Parser;
use color_eyre::Result;
use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use futures::stream::{self, StreamExt};
use rsky_identity::IdResolver;
use rsky_identity::types::IdentityResolverOpts;
use rsky_wintermute::config::IDENTITY_RESOLVER_TIMEOUT;
use rsky_wintermute::indexer::IndexerManager;
use tokio_postgres::NoTls;

#[derive(Debug, Parser)]
#[command(name = "reverify_handles")]
#[command(about = "Re-verify actor handles for a list of DIDs, bypassing the sweep's cooldowns")]
struct Args {
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    /// File with one DID per line; `-` reads stdin.
    #[arg(default_value = "-")]
    input: String,

    #[arg(long, default_value_t = 20)]
    concurrency: usize,
}

fn read_dids(input: &str) -> Result<Vec<String>> {
    let reader: Box<dyn BufRead> = if input == "-" {
        Box::new(BufReader::new(std::io::stdin()))
    } else {
        Box::new(BufReader::new(std::fs::File::open(input)?))
    };
    let mut dids = Vec::new();
    for line in reader.lines() {
        let line = line?;
        let did = line.split('#').next().unwrap_or("").trim();
        if !did.is_empty() {
            dids.push(did.to_owned());
        }
    }
    Ok(dids)
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();
    let dids = read_dids(&args.input)?;

    let mut cfg = Config::new();
    cfg.url = Some(args.database_url.clone());
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    // Connections are only held around the reads and writes, so a small pool
    // serves a high --concurrency.
    cfg.pool = Some(deadpool_postgres::PoolConfig::new(
        args.concurrency.clamp(1, 16),
    ));
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;

    let id_resolver = Arc::new(IdResolver::new(IdentityResolverOpts {
        timeout: Some(IDENTITY_RESOLVER_TIMEOUT),
        plc_url: std::env::var("PLC_URL").ok(),
        did_cache: None,
        backup_nameservers: None,
    }));

    let timestamp = chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string();

    let results: Vec<(String, std::result::Result<bool, String>)> = stream::iter(dids)
        .map(|did| {
            let pool = pool.clone();
            let id_resolver = Arc::clone(&id_resolver);
            let timestamp = timestamp.clone();
            async move {
                let outcome =
                    IndexerManager::index_handle(&pool, &id_resolver, &did, &timestamp, true)
                        .await
                        .map_err(|e| e.to_string());
                (did, outcome)
            }
        })
        .buffer_unordered(args.concurrency.max(1))
        .collect()
        .await;

    let (mut verified, mut unverified, mut errors) = (0usize, 0usize, 0usize);
    for (did, outcome) in &results {
        match outcome {
            Ok(true) => {
                verified += 1;
                println!("{did}\tverified");
            }
            Ok(false) => {
                unverified += 1;
                println!("{did}\tunverified");
            }
            Err(e) => {
                errors += 1;
                println!("{did}\terror: {e}");
            }
        }
    }
    eprintln!(
        "{} DIDs: {verified} verified, {unverified} unverified, {errors} errors",
        results.len()
    );
    Ok(())
}
