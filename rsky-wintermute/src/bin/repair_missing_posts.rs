//! Repairs posts the appview is missing although other posts reference them.
//!
//! For each window, finds the reply parents, reply roots and quoted posts
//! referenced by posts sorted in it that are not in the `post` table, fetches
//! each one from its author's PDS with `com.atproto.sync.getRecord`, and
//! indexes it. Posts the PDS proves absent were deleted and are left alone.
//! Unlike `firehose_catchup` this works for gaps of any age, but it only finds
//! posts something else references.
//!
//! Prints one `uri<TAB>outcome<TAB>detail` line per post on stdout and a
//! summary on stderr. The exit status is non-zero when an indexing write
//! failed; a PDS that cannot be reached is reported, not fatal.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use clap::Parser;
use color_eyre::Result;
use color_eyre::eyre::eyre;
use dashmap::DashMap;
use deadpool_postgres::Pool;
use futures::stream::{self, StreamExt};
use rsky_identity::IdResolver;
use rsky_identity::safe_fetch::SafeClient;
use rsky_identity::types::IdentityResolverOpts;
use rsky_wintermute::config::IDENTITY_RESOLVER_TIMEOUT;
use rsky_wintermute::indexer::IndexerManager;
use rsky_wintermute::reconcile::{Provenance, Source, current_generation};
use rsky_wintermute::repair::{Fetched, Target, fetch_post, missing_targets, repaired_indexed_at};
use rsky_wintermute::types::{IndexJob, WriteAction};

/// Referencing posts sorted between two instants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TimeWindow {
    since: DateTime<Utc>,
    until: DateTime<Utc>,
}

fn parse_window(s: &str) -> Result<TimeWindow, String> {
    let (since, until) = s
        .split_once('/')
        .ok_or_else(|| format!("expected SINCE/UNTIL, got '{s}'"))?;
    let parse = |t: &str| {
        DateTime::parse_from_rfc3339(t.trim())
            .map(|t| t.with_timezone(&Utc))
            .map_err(|e| format!("invalid time '{t}': {e}"))
    };
    let (since, until) = (parse(since)?, parse(until)?);
    if since >= until {
        return Err(format!("since ({since}) must be before until ({until})"));
    }
    Ok(TimeWindow { since, until })
}

/// Splits `window` into chunks of at most `chunk`, so that no one scan of
/// the post table runs for long.
fn chunks(window: TimeWindow, chunk: chrono::Duration) -> Vec<(String, String)> {
    let format = |t: DateTime<Utc>| t.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
    let mut out = Vec::new();
    let mut start = window.since;
    while start < window.until {
        let end = (start + chunk).min(window.until);
        out.push((format(start), format(end)));
        start = end;
    }
    out
}

#[derive(Debug, Parser)]
#[command(name = "repair_missing_posts")]
#[command(about = "Fetch and index posts that other posts reference but the appview is missing")]
struct Args {
    /// Referencing posts to scan, as SINCE/UNTIL in RFC 3339, on their
    /// "sortAt". Repeatable, comma-separated. The posts they reference can be
    /// of any age.
    #[arg(long = "window", value_parser = parse_window, value_delimiter = ',', required = true)]
    windows: Vec<TimeWindow>,

    /// Each window is scanned in chunks of this many minutes. Production
    /// connections carry a statement timeout, and an hour of posts after a
    /// large gap exceeded it.
    #[arg(long, default_value = "10")]
    chunk_minutes: u32,

    /// Fetch the posts and report, without indexing anything.
    #[arg(long)]
    dry_run: bool,

    /// Posts fetched and indexed at once.
    #[arg(long, default_value = "16")]
    concurrency: usize,

    /// Seconds allowed for resolving an author and fetching one post.
    #[arg(long, default_value = "30")]
    fetch_timeout_secs: u64,

    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    #[arg(long, default_value = "8")]
    pool_size: usize,
}

/// What happened to one missing post.
enum Outcome {
    Repaired,
    WouldRepair,
    Deleted,
    Unavailable(String),
    Unresolvable(String),
    FetchFailed(String),
    WriteFailed(String),
}

impl Outcome {
    const fn label(&self) -> &'static str {
        match self {
            Self::Repaired => "repaired",
            Self::WouldRepair => "would-repair",
            Self::Deleted => "deleted",
            Self::Unavailable(_) => "unavailable",
            Self::Unresolvable(_) => "unresolvable",
            Self::FetchFailed(_) => "fetch-failed",
            Self::WriteFailed(_) => "write-failed",
        }
    }

    fn detail(&self) -> &str {
        match self {
            Self::Unavailable(d)
            | Self::Unresolvable(d)
            | Self::FetchFailed(d)
            | Self::WriteFailed(d) => d,
            _ => "",
        }
    }
}

struct Repairer {
    pool: Pool,
    http: SafeClient,
    resolver: IdResolver,
    /// The PDS of each author, or why it could not be found.
    pds: DashMap<String, Result<String, String>>,
    dry_run: bool,
    fetch_timeout: Duration,
}

impl Repairer {
    async fn pds_of(&self, did: &str) -> Result<String, String> {
        if let Some(known) = self.pds.get(did) {
            return known.clone();
        }
        let resolved = match self.resolver.did.resolve(did.to_owned(), None).await {
            Ok(Some(doc)) => doc
                .service
                .unwrap_or_default()
                .into_iter()
                .find(|s| s.r#type == "AtprotoPersonalDataServer" || s.id == "#atproto_pds")
                .map(|s| s.service_endpoint)
                .ok_or_else(|| "no PDS in the DID document".to_owned()),
            Ok(None) => Err("DID document not found".to_owned()),
            Err(e) => Err(format!("DID resolution failed: {e}")),
        };
        self.pds.insert(did.to_owned(), resolved.clone());
        resolved
    }

    async fn repair(&self, target: &Target) -> Outcome {
        let fetched = tokio::time::timeout(self.fetch_timeout, async {
            let pds = match self.pds_of(&target.did).await {
                Ok(pds) => pds,
                Err(e) => return Err(Outcome::Unresolvable(e)),
            };
            fetch_post(&self.http, &pds, target)
                .await
                .map_err(|e| Outcome::FetchFailed(format!("{pds}: {e}")))
        })
        .await;
        let found = match fetched {
            Err(_) => {
                return Outcome::FetchFailed(format!("timed out after {:?}", self.fetch_timeout));
            }
            Ok(Err(outcome)) => return outcome,
            Ok(Ok(Fetched::Deleted)) => return Outcome::Deleted,
            Ok(Ok(Fetched::Unavailable(reason))) => return Outcome::Unavailable(reason),
            Ok(Ok(Fetched::Found(found))) => found,
        };
        if self.dry_run {
            return Outcome::WouldRepair;
        }
        let generation = match current_generation(&self.pool, &target.did).await {
            Ok(generation) => generation,
            Err(e) => return Outcome::WriteFailed(format!("generation lookup: {e}")),
        };
        let job = IndexJob {
            uri: target.uri.clone(),
            cid: found.cid,
            action: WriteAction::Create,
            indexed_at: repaired_indexed_at(&found.record, Utc::now()),
            record: Some(found.record),
            rev: found.rev,
            provenance: Some(Provenance {
                generation,
                source: Source::Direct,
            }),
        };
        match IndexerManager::process_job(
            &self.pool,
            &job,
            *rsky_wintermute::config::RECORD_SKIP_BOILERPLATE,
        )
        .await
        {
            Ok(()) => Outcome::Repaired,
            Err(e) => Outcome::WriteFailed(e.to_string()),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();
    if args.concurrency == 0 || args.chunk_minutes == 0 {
        return Err(eyre!(
            "--concurrency and --chunk-minutes must be at least 1"
        ));
    }
    let pool = rsky_wintermute::config::create_pg_pool(
        &args.database_url,
        rsky_wintermute::config::pg_pool_config(args.pool_size),
    )?;

    // Every window first, so a post referenced from several chunks is
    // fetched once.
    let mut targets = BTreeSet::new();
    let chunk = chrono::Duration::minutes(i64::from(args.chunk_minutes));
    for window in &args.windows {
        for (since, until) in chunks(*window, chunk) {
            let client = pool.get().await?;
            let found = missing_targets(&client, &since, &until).await?;
            tracing::info!("{since}/{until}: {} referenced posts missing", found.len());
            targets.extend(found);
        }
    }
    tracing::info!(
        "{} distinct missing posts; {}",
        targets.len(),
        if args.dry_run {
            "fetching without indexing (--dry-run)"
        } else {
            "fetching and indexing"
        }
    );

    let repairer = Arc::new(Repairer {
        pool,
        http: rsky_wintermute::outbound::client()?,
        resolver: IdResolver::new(IdentityResolverOpts {
            timeout: Some(IDENTITY_RESOLVER_TIMEOUT),
            plc_url: std::env::var("PLC_URL").ok(),
            did_cache: None,
            backup_nameservers: None,
        }),
        pds: DashMap::new(),
        dry_run: args.dry_run,
        fetch_timeout: Duration::from_secs(args.fetch_timeout_secs),
    });

    let total = targets.len();
    let mut counts: std::collections::BTreeMap<&'static str, usize> = Default::default();
    let mut done = 0usize;
    let mut outcomes = stream::iter(targets)
        .map(|uri| {
            let repairer = Arc::clone(&repairer);
            async move {
                let outcome = match Target::parse(&uri) {
                    Some(target) => repairer.repair(&target).await,
                    None => Outcome::Unresolvable("not a post named by DID".to_owned()),
                };
                (uri, outcome)
            }
        })
        .buffer_unordered(args.concurrency);
    while let Some((uri, outcome)) = outcomes.next().await {
        println!("{uri}\t{}\t{}", outcome.label(), outcome.detail());
        *counts.entry(outcome.label()).or_default() += 1;
        done += 1;
        if done % 500 == 0 {
            tracing::info!("{done}/{total} posts: {counts:?}");
        }
    }

    let summary = counts
        .iter()
        .map(|(label, n)| format!("{n} {label}"))
        .collect::<Vec<_>>()
        .join(", ");
    eprintln!("{total} missing posts: {summary}");
    let write_failed = counts.get("write-failed").copied().unwrap_or(0);
    if write_failed > 0 {
        return Err(eyre!("{write_failed} indexing write(s) failed"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_split_into_chunks() {
        let window = parse_window("2026-10-02T14:00:00Z/2026-10-02T16:30:00Z").unwrap();
        assert_eq!(
            chunks(window, chrono::Duration::minutes(60)),
            [
                (
                    "2026-10-02T14:00:00.000Z".to_owned(),
                    "2026-10-02T15:00:00.000Z".to_owned()
                ),
                (
                    "2026-10-02T15:00:00.000Z".to_owned(),
                    "2026-10-02T16:00:00.000Z".to_owned()
                ),
                (
                    "2026-10-02T16:00:00.000Z".to_owned(),
                    "2026-10-02T16:30:00.000Z".to_owned()
                ),
            ]
        );
    }

    #[test]
    fn rejects_malformed_windows() {
        assert!(parse_window("2026-10-02T14:00:00Z").is_err());
        assert!(parse_window("2026-10-02T16:00:00Z/2026-10-02T14:00:00Z").is_err());
        assert!(parse_window("yesterday/today").is_err());
    }
}
