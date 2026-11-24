//! Repairs actors left `deactivated` by a move between PDSes (EUR-693).
//!
//! Walks the whole `actor` table in primary-key pages (there is no index on
//! `upstreamStatus`). For every `deactivated` actor it first asks the relay,
//! and only when the relay reports the account active does it ask the
//! account's PDS, through the same check and conditional clear that the
//! live ingester uses. Most deactivated accounts are on Bluesky's PDSes, so
//! asking the relay first keeps tens of thousands of requests off them.
//!
//! Run it with the wintermute environment, so `PLC_URL` points at the same
//! PLC mirror: `--dry-run` first. Every clear is logged with its DID.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use clap::Parser;
use color_eyre::Result;
use deadpool_postgres::{Config, ManagerConfig, Pool, RecyclingMethod, Runtime};
use futures::stream::{self, StreamExt};
use rsky_wintermute::ingester::{IngesterManager, relay_http_endpoint};
use tokio_postgres::NoTls;
use tracing::{info, warn};

#[derive(Debug, Parser)]
#[command(name = "recheck_deactivated")]
#[command(
    about = "Clears upstreamStatus='deactivated' for actors that the relay and their PDS both report active."
)]
struct Args {
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    /// The relay to ask first; the first of a comma-separated list is used.
    #[arg(long, env = "RELAY_HOSTS")]
    relay: String,

    #[arg(long, default_value_t = 8)]
    concurrency: usize,

    /// Actor rows read per page of the table walk.
    #[arg(long, default_value_t = 20_000)]
    page_size: i64,

    /// Pause between pages, to keep the walk gentle on the database.
    #[arg(long, default_value_t = 50)]
    page_pause_ms: u64,

    /// Stop after this many deactivated actors (0 = all).
    #[arg(long, default_value_t = 0)]
    limit: u64,

    /// Report what would be cleared, without writing.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Default)]
struct Counters {
    deactivated: AtomicU64,
    relay_inactive: AtomicU64,
    relay_unknown: AtomicU64,
    pds_not_active: AtomicU64,
    cleared: AtomicU64,
    unchanged: AtomicU64,
    failed: AtomicU64,
}

impl Counters {
    fn summary(&self) -> String {
        format!(
            "deactivated={} relay_inactive={} relay_unknown={} pds_not_active={} cleared={} unchanged={} failed={}",
            self.deactivated.load(Ordering::Relaxed),
            self.relay_inactive.load(Ordering::Relaxed),
            self.relay_unknown.load(Ordering::Relaxed),
            self.pds_not_active.load(Ordering::Relaxed),
            self.cleared.load(Ordering::Relaxed),
            self.unchanged.load(Ordering::Relaxed),
            self.failed.load(Ordering::Relaxed),
        )
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let relay_host = args
        .relay
        .split(',')
        .map(str::trim)
        .find(|h| !h.is_empty())
        .ok_or_else(|| color_eyre::eyre::eyre!("no relay host given"))?;
    let (scheme, host) = relay_http_endpoint(relay_host);
    let relay_base = format!("{scheme}://{host}");

    let mut cfg = Config::new();
    cfg.url = Some(args.database_url.clone());
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;

    let http = rsky_wintermute::outbound::client()?
        .builder()
        .timeout(Duration::from_secs(10))
        .user_agent("rsky-wintermute-recheck/0.1")
        .build()?;

    info!(
        "walking actor for deactivated rows; relay {relay_base}; {}",
        if args.dry_run { "dry run" } else { "WRITING" }
    );

    let counters = Arc::new(Counters::default());
    let mut after = String::new();
    let mut pages: u64 = 0;
    'walk: loop {
        let Some((last, dids)) = next_page(&pool, &after, args.page_size).await? else {
            break;
        };
        after = last;
        pages += 1;

        let mut dids = dids;
        if args.limit > 0 {
            let seen = counters.deactivated.load(Ordering::Relaxed);
            let room = usize::try_from(args.limit.saturating_sub(seen)).unwrap_or(usize::MAX);
            dids.truncate(room);
        }
        counters
            .deactivated
            .fetch_add(dids.len() as u64, Ordering::Relaxed);

        stream::iter(dids)
            .for_each_concurrent(args.concurrency, |did| {
                let pool = pool.clone();
                let http = http.clone();
                let relay_base = relay_base.clone();
                let counters = Arc::clone(&counters);
                async move {
                    recheck_one(&pool, &http, &relay_base, &did, args.dry_run, &counters).await;
                }
            })
            .await;

        if pages % 100 == 0 {
            info!(
                "progress: {pages} pages, at {after}: {}",
                counters.summary()
            );
        }
        if args.limit > 0 && counters.deactivated.load(Ordering::Relaxed) >= args.limit {
            break 'walk;
        }
        tokio::time::sleep(Duration::from_millis(args.page_pause_ms)).await;
    }

    info!("done: {pages} pages: {}", counters.summary());
    Ok(())
}

/// The next page of the table walk: the last DID of the page and the
/// deactivated DIDs on it, or `None` past the end.
async fn next_page(
    pool: &Pool,
    after: &str,
    page_size: i64,
) -> Result<Option<(String, Vec<String>)>> {
    let client = pool.get().await?;
    let row = client
        .query_one(
            "WITH page AS (
                 SELECT did, \"upstreamStatus\" FROM actor
                  WHERE did > $1 ORDER BY did LIMIT $2
             )
             SELECT (SELECT did FROM page ORDER BY did DESC LIMIT 1),
                    COALESCE(array_agg(did) FILTER (WHERE \"upstreamStatus\" = 'deactivated'),
                             '{}')
               FROM page",
            &[&after, &page_size],
        )
        .await?;
    let last: Option<String> = row.get(0);
    Ok(last.map(|last| (last, row.get(1))))
}

async fn recheck_one(
    pool: &Pool,
    http: &reqwest::Client,
    relay_base: &str,
    did: &str,
    dry_run: bool,
    counters: &Counters,
) {
    match relay_says_active(http, relay_base, did).await {
        Some(true) => {}
        Some(false) => {
            counters.relay_inactive.fetch_add(1, Ordering::Relaxed);
            return;
        }
        None => {
            counters.relay_unknown.fetch_add(1, Ordering::Relaxed);
            return;
        }
    }

    if dry_run {
        if IngesterManager::pds_says_active(did).await == Some(true) {
            info!("[dry-run] would clear {did}");
            counters.cleared.fetch_add(1, Ordering::Relaxed);
        } else {
            counters.pds_not_active.fetch_add(1, Ordering::Relaxed);
        }
        return;
    }

    let pds_active = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = Arc::clone(&pds_active);
    let result = IngesterManager::recheck_deactivated(pool, did, |did| async move {
        let active = IngesterManager::pds_says_active(&did).await;
        seen.store(active == Some(true), Ordering::Relaxed);
        active
    })
    .await;
    match result {
        Ok(true) => {
            info!("cleared {did}");
            counters.cleared.fetch_add(1, Ordering::Relaxed);
        }
        Ok(false) if pds_active.load(Ordering::Relaxed) => {
            // The row changed while the PDS was asked.
            counters.unchanged.fetch_add(1, Ordering::Relaxed);
        }
        Ok(false) => {
            counters.pds_not_active.fetch_add(1, Ordering::Relaxed);
        }
        Err(e) => {
            warn!("{did}: recheck failed: {e}");
            counters.failed.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// The relay's `getRepoStatus` answer, or `None` when it can't be had.
async fn relay_says_active(http: &reqwest::Client, relay_base: &str, did: &str) -> Option<bool> {
    let url = rsky_wintermute::outbound::client()
        .ok()?
        .checked(&format!(
            "{relay_base}/xrpc/com.atproto.sync.getRepoStatus?did={did}"
        ))
        .ok()?;
    let resp = http.get(url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let body: serde_json::Value = resp.json().await.ok()?;
    body.get("active").and_then(serde_json::Value::as_bool)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_pool() -> Pool {
        let mut cfg = Config::new();
        cfg.url = Some(std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/bsky_test".to_owned()
        }));
        cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap()
    }

    #[tokio::test]
    async fn walk_finds_every_deactivated_actor_across_pages() {
        let pool = test_pool();
        let client = pool.get().await.unwrap();
        let prefix = "did:plc:recheckwalk";
        client
            .execute(
                "DELETE FROM actor WHERE did LIKE $1",
                &[&format!("{prefix}%")],
            )
            .await
            .unwrap();
        let mut expected = Vec::new();
        for (i, status) in [
            Some("deactivated"),
            None,
            Some("takendown"),
            Some("deactivated"),
            None,
            None,
            Some("deactivated"),
        ]
        .into_iter()
        .enumerate()
        {
            let did = format!("{prefix}{}", char::from(b'a' + u8::try_from(i).unwrap()));
            client
                .execute(
                    "INSERT INTO actor (did, handle, \"indexedAt\", \"upstreamStatus\")
                     VALUES ($1, $2, NOW(), $3)",
                    &[&did, &format!("{}.test", &did[8..]), &status],
                )
                .await
                .unwrap();
            if status == Some("deactivated") {
                expected.push(did);
            }
        }

        // Pages of 2 rows, starting just before the seeded range.
        let mut after = prefix.to_owned();
        let mut found = Vec::new();
        while let Some((last, dids)) = next_page(&pool, &after, 2).await.unwrap() {
            if !last.starts_with(prefix) {
                found.extend(dids.into_iter().filter(|d| d.starts_with(prefix)));
                break;
            }
            found.extend(dids.into_iter().filter(|d| d.starts_with(prefix)));
            after = last;
        }
        assert_eq!(found, expected);
    }

    #[tokio::test]
    async fn walk_past_the_end_is_none() {
        let pool = test_pool();
        assert!(next_page(&pool, "\u{10FFFF}", 10).await.unwrap().is_none());
    }
}
