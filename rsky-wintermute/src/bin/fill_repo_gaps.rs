//! Fills the gaps in accounts' indexed history (EUR-654).
//!
//! For each account it reads the whole repository from the account's PDS,
//! compares it with the records the AppView already holds (`record`, by URI
//! and CID), and indexes only the records that are missing or held in an
//! older version. Records already present are not written again, so a fill
//! costs writes only for the gaps.
//!
//! Each written record is stamped with its own `createdAt` (capped at now) as
//! `indexedAt`. Its posts, likes and follows then sort where they were made,
//! and the notifications they create carry that old time: they are listed in
//! the past, not as new, and the AppView's push bridge does not push them
//! (`BSKY_NOTIFICATION_PUSH_MAX_AGE_MINUTES`). Run it only against an AppView
//! whose push bridge has that check.
//!
//! Records the AppView holds but the repository no longer has are deletes it
//! missed. They are counted (`extra`), not removed.
//!
//! Prints one tab-separated line per account on stdout and a summary to
//! stderr. Run it with the service's environment (`PLC_URL`), and with
//! `--dry-run` first.

use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use clap::Parser;
use color_eyre::Result;
use color_eyre::eyre::eyre;
use futures::stream::{self, StreamExt};
use iroh_car::CarReader;
use rsky_identity::IdResolver;
use rsky_identity::types::IdentityResolverOpts;
use rsky_repo::parse::get_and_parse_record;
use rsky_repo::readable_repo::ReadableRepo;
use rsky_repo::storage::memory_blockstore::MemoryBlockstore;
use rsky_wintermute::backfiller::convert_record_to_ipld;
use rsky_wintermute::indexer::IndexerManager;
use rsky_wintermute::ingester::pds_endpoint;
use rsky_wintermute::reconcile::{Provenance, Source};
use rsky_wintermute::repair::repaired_indexed_at;
use rsky_wintermute::types::{IndexJob, WriteAction};
use tracing::{info, warn};

#[derive(Debug, Parser)]
#[command(name = "fill_repo_gaps")]
#[command(
    about = "Reads accounts' repositories again and indexes only the records the AppView is missing."
)]
struct Args {
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    /// Accounts to fill (repeatable, or comma-separated).
    #[arg(long = "did", num_args = 1..)]
    dids: Vec<String>,

    /// A file with one DID per line; blank lines and `#` comments are skipped.
    #[arg(long)]
    dids_file: Option<std::path::PathBuf>,

    /// Every active account on this PDS, from its `com.atproto.sync.listRepos`.
    #[arg(long)]
    pds: Option<String>,

    /// Accounts read at the same time.
    #[arg(long, default_value_t = 4)]
    concurrency: usize,

    /// Stop after this many accounts (0 = all).
    #[arg(long, default_value_t = 0)]
    limit: usize,

    /// Skip a repository larger than this.
    #[arg(long, default_value_t = 512)]
    max_car_mb: u64,

    /// Compare and report, without writing.
    #[arg(long)]
    dry_run: bool,
}

/// What one account needed and got.
#[derive(Debug, Default)]
struct Report {
    in_repo: usize,
    missing: usize,
    changed: usize,
    extra: usize,
    written: usize,
    failed: usize,
    car_bytes: usize,
    /// Missing or changed records by collection.
    needed_by_collection: HashMap<String, usize>,
}

#[derive(Debug, PartialEq, Eq)]
enum Need {
    Missing,
    Changed,
}

/// A record of the repository that has to be indexed.
#[derive(Debug, PartialEq, Eq)]
struct Needed {
    uri: String,
    /// `collection/rkey`, the record's key in the repository.
    key: String,
    cid: String,
    need: Need,
}

/// Compares a repository's records (`collection/rkey` → CID) with the ones
/// held for the account (URI → CID). Returns the records to index and the
/// number held that the repository no longer has.
fn plan(
    did: &str,
    repo: &[(String, String)],
    held: &HashMap<String, String>,
    allowed: impl Fn(&str) -> bool,
) -> (Vec<Needed>, usize) {
    let mut needed = Vec::new();
    let mut in_repo = HashSet::with_capacity(repo.len());
    for (key, cid) in repo {
        let Some((collection, _rkey)) = key.split_once('/') else {
            continue;
        };
        if !allowed(collection) {
            continue;
        }
        let uri = format!("at://{did}/{key}");
        let need = match held.get(&uri) {
            None => Some(Need::Missing),
            Some(held_cid) if held_cid != cid => Some(Need::Changed),
            Some(_) => None,
        };
        if let Some(need) = need {
            needed.push(Needed {
                uri: uri.clone(),
                key: key.clone(),
                cid: cid.clone(),
                need,
            });
        }
        in_repo.insert(uri);
    }
    let extra = held.keys().filter(|uri| !in_repo.contains(*uri)).count();
    (needed, extra)
}

/// DIDs from the flags and the file, in order, without repeats.
fn collect_dids(flags: &[String], file: Option<&str>) -> Vec<String> {
    let from_flags = flags.iter().flat_map(|d| d.split(','));
    let from_file = file
        .into_iter()
        .flat_map(str::lines)
        .map(|line| line.split('#').next().unwrap_or(""));
    let mut seen = HashSet::new();
    from_flags
        .chain(from_file)
        .map(str::trim)
        .filter(|d| d.starts_with("did:"))
        .filter(|d| seen.insert((*d).to_owned()))
        .map(str::to_owned)
        .collect()
}

struct Ctx {
    pool: deadpool_postgres::Pool,
    http: reqwest::Client,
    resolver: IdResolver,
    max_car_bytes: usize,
    dry_run: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let http = rsky_wintermute::outbound::client()?
        .builder()
        .timeout(Duration::from_secs(300))
        .user_agent("rsky-wintermute-fill-repo-gaps/0.1")
        .build()?;

    let file = match &args.dids_file {
        Some(path) => Some(std::fs::read_to_string(path)?),
        None => None,
    };
    let mut dids = collect_dids(&args.dids, file.as_deref());
    if let Some(pds) = &args.pds {
        let listed = list_active_repos(&http, pds).await?;
        info!("{} active repositories on {pds}", listed.len());
        let mut seen: HashSet<String> = dids.iter().cloned().collect();
        dids.extend(listed.into_iter().filter(|d| seen.insert(d.clone())));
    }
    if args.limit > 0 {
        dids.truncate(args.limit);
    }
    if dids.is_empty() {
        return Err(eyre!("no accounts given: use --did, --dids-file or --pds"));
    }

    let ctx = Arc::new(Ctx {
        pool: rsky_wintermute::config::create_pg_pool(
            &args.database_url,
            rsky_wintermute::config::pg_pool_config(args.concurrency * 2 + 2),
        )?,
        http,
        resolver: IdResolver::new(IdentityResolverOpts {
            timeout: Some(Duration::from_secs(10)),
            plc_url: std::env::var("PLC_URL").ok(),
            did_cache: None,
            backup_nameservers: None,
        }),
        max_car_bytes: usize::try_from(args.max_car_mb * 1024 * 1024).unwrap_or(usize::MAX),
        dry_run: args.dry_run,
    });
    info!(
        "{} accounts, {} at a time; {}",
        dids.len(),
        args.concurrency,
        if args.dry_run { "dry run" } else { "WRITING" }
    );

    println!("did\tin_repo\tmissing\tchanged\textra\twritten\tfailed\tcar_bytes\terror");
    let totals = Arc::new(Mutex::new((Report::default(), 0usize)));
    stream::iter(dids)
        .map(|did| {
            let ctx = Arc::clone(&ctx);
            async move {
                let result = fill_one(&ctx, &did).await;
                (did, result)
            }
        })
        .buffer_unordered(args.concurrency.max(1))
        .for_each(|(did, result)| {
            let totals = Arc::clone(&totals);
            async move {
                let (report, error) = match result {
                    Ok(report) => (report, String::new()),
                    Err(e) => (Report::default(), e.to_string().replace(['\t', '\n'], " ")),
                };
                println!(
                    "{did}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{error}",
                    report.in_repo,
                    report.missing,
                    report.changed,
                    report.extra,
                    report.written,
                    report.failed,
                    report.car_bytes,
                );
                let mut totals = totals
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let (sum, errors) = &mut *totals;
                if !error.is_empty() {
                    *errors += 1;
                }
                sum.in_repo += report.in_repo;
                sum.missing += report.missing;
                sum.changed += report.changed;
                sum.extra += report.extra;
                sum.written += report.written;
                sum.failed += report.failed;
                sum.car_bytes += report.car_bytes;
                for (collection, n) in report.needed_by_collection {
                    *sum.needed_by_collection.entry(collection).or_default() += n;
                }
            }
        })
        .await;

    let totals = totals
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (sum, errors) = &*totals;
    let mut by_collection: Vec<_> = sum.needed_by_collection.iter().collect();
    by_collection.sort_by(|a, b| b.1.cmp(a.1));
    info!(
        "done: records in repositories {}, missing {}, changed {}, extra {}, written {}, failed {}, \
         downloaded {} MB, accounts with errors {}",
        sum.in_repo,
        sum.missing,
        sum.changed,
        sum.extra,
        sum.written,
        sum.failed,
        sum.car_bytes / (1024 * 1024),
        errors
    );
    info!("missing or changed by collection: {by_collection:?}");
    Ok(())
}

async fn fill_one(ctx: &Ctx, did: &str) -> Result<Report> {
    let doc = ctx
        .resolver
        .did
        .resolve(did.to_owned(), None)
        .await
        .map_err(|e| eyre!("DID resolution: {e}"))?
        .ok_or_else(|| eyre!("DID not found"))?;
    let pds = pds_endpoint(did, &doc).ok_or_else(|| eyre!("no PDS in the DID document"))?;

    let url = rsky_wintermute::outbound::client()?.checked(&format!(
        "{}/xrpc/com.atproto.sync.getRepo?did={did}",
        pds.trim_end_matches('/')
    ))?;
    let resp = ctx.http.get(url).send().await?;
    if !resp.status().is_success() {
        return Err(eyre!("getRepo: HTTP {}", resp.status()));
    }
    if resp
        .content_length()
        .is_some_and(|len| usize::try_from(len).unwrap_or(usize::MAX) > ctx.max_car_bytes)
    {
        return Err(eyre!("repository larger than --max-car-mb"));
    }
    let car = resp.bytes().await?;
    if car.len() > ctx.max_car_bytes {
        return Err(eyre!("repository larger than --max-car-mb"));
    }

    let mut reader = CarReader::new(Cursor::new(car.to_vec()))
        .await
        .map_err(|e| eyre!("CAR header: {e}"))?;
    let root = *reader
        .header()
        .roots()
        .first()
        .ok_or_else(|| eyre!("CAR has no root"))?;
    let mut blocks = rsky_repo::block_map::BlockMap::new();
    while let Some((cid, data)) = reader
        .next_block()
        .await
        .map_err(|e| eyre!("CAR block: {e}"))?
    {
        blocks.set(cid, data);
    }
    let storage = Arc::new(tokio::sync::RwLock::new(
        MemoryBlockstore::new(Some(blocks))
            .await
            .map_err(|e| eyre!("blockstore: {e}"))?,
    ));
    let mut repo = ReadableRepo::load(storage, root)
        .await
        .map_err(|e| eyre!("repository: {e}"))?;
    if repo.did() != did {
        return Err(eyre!("repository belongs to {}", repo.did()));
    }
    let leaves = repo
        .data
        .list(None, None, None)
        .await
        .map_err(|e| eyre!("listing records: {e}"))?;
    let rev = repo.commit.rev.clone();

    let held: HashMap<String, String> = {
        let client = ctx.pool.get().await?;
        client
            .query("SELECT uri, cid FROM record WHERE did = $1", &[&did])
            .await?
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect()
    };
    let repo_records: Vec<(String, String)> = leaves
        .iter()
        .map(|leaf| (leaf.key.clone(), leaf.value.to_string()))
        .collect();
    let (needed, extra) = plan(
        did,
        &repo_records,
        &held,
        rsky_wintermute::config::ingest_collection_allowed,
    );

    let mut report = Report {
        in_repo: repo_records.len(),
        extra,
        car_bytes: car.len(),
        ..Report::default()
    };
    for n in &needed {
        match n.need {
            Need::Missing => report.missing += 1,
            Need::Changed => report.changed += 1,
        }
        let collection = n.key.split_once('/').map_or("", |(c, _)| c);
        *report
            .needed_by_collection
            .entry(collection.to_owned())
            .or_default() += 1;
    }
    if ctx.dry_run || needed.is_empty() {
        return Ok(report);
    }

    let cids: HashMap<&str, _> = leaves
        .iter()
        .map(|leaf| (leaf.key.as_str(), leaf.value))
        .collect();
    let wanted: Vec<_> = needed
        .iter()
        .filter_map(|n| cids.get(n.key.as_str()).copied())
        .collect();
    let found = {
        let storage = repo.storage.read().await;
        storage
            .get_blocks(wanted)
            .await
            .map_err(|e| eyre!("reading records: {e}"))?
    };
    let generation = rsky_wintermute::reconcile::current_generation(&ctx.pool, did).await?;
    let now = Utc::now();
    let mut wrote_counted = false;
    for n in &needed {
        let Some(cid) = cids.get(n.key.as_str()).copied() else {
            report.failed += 1;
            continue;
        };
        let Ok(parsed) = get_and_parse_record(&found.blocks, cid) else {
            report.failed += 1;
            continue;
        };
        let raw = serde_json::to_value(&parsed.record)?;
        let job = IndexJob {
            uri: n.uri.clone(),
            cid: n.cid.clone(),
            action: WriteAction::Create,
            indexed_at: repaired_indexed_at(&raw, now),
            record: Some(convert_record_to_ipld(&raw)),
            rev: rev.clone(),
            provenance: Some(Provenance {
                generation,
                source: Source::Direct,
            }),
        };
        match IndexerManager::process_job(
            &ctx.pool,
            &job,
            *rsky_wintermute::config::RECORD_SKIP_BOILERPLATE,
        )
        .await
        {
            Ok(()) => {
                report.written += 1;
                wrote_counted |= n.key.starts_with("app.bsky.feed.post/")
                    || n.key.starts_with("app.bsky.graph.follow/");
            }
            Err(e) => {
                warn!("{}: {e}", n.uri);
                report.failed += 1;
            }
        }
    }

    // The profile's post and follow counts, as direct_index recomputes them.
    if wrote_counted {
        let client = ctx.pool.get().await?;
        client
            .execute(
                "INSERT INTO profile_agg (did, \"postsCount\")
                 SELECT $1::varchar, COUNT(*) FROM post WHERE creator = $1::varchar
                 ON CONFLICT (did) DO UPDATE SET \"postsCount\" = EXCLUDED.\"postsCount\"",
                &[&did],
            )
            .await?;
        client
            .execute(
                "INSERT INTO profile_agg (did, \"followsCount\")
                 SELECT $1::varchar, COUNT(*) FROM follow WHERE creator = $1::varchar
                 ON CONFLICT (did) DO UPDATE SET \"followsCount\" = EXCLUDED.\"followsCount\"",
                &[&did],
            )
            .await?;
    }
    Ok(report)
}

async fn list_active_repos(http: &reqwest::Client, pds: &str) -> Result<Vec<String>> {
    #[derive(serde::Deserialize)]
    struct Page {
        #[serde(default)]
        cursor: Option<String>,
        repos: Vec<Repo>,
    }
    #[derive(serde::Deserialize)]
    struct Repo {
        did: String,
        #[serde(default = "active_default")]
        active: bool,
    }
    const fn active_default() -> bool {
        true
    }

    let base = pds.trim_end_matches('/');
    let mut dids = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut url = format!("{base}/xrpc/com.atproto.sync.listRepos?limit=1000");
        if let Some(c) = &cursor {
            url.push_str("&cursor=");
            url.push_str(c);
        }
        let url = rsky_wintermute::outbound::client()?.checked(&url)?;
        let resp = http.get(url).send().await?;
        if !resp.status().is_success() {
            return Err(eyre!("listRepos: HTTP {}", resp.status()));
        }
        let page: Page = resp.json().await?;
        let empty = page.repos.is_empty();
        dids.extend(page.repos.into_iter().filter(|r| r.active).map(|r| r.did));
        cursor = page.cursor;
        if empty || cursor.is_none() {
            break;
        }
    }
    Ok(dids)
}

#[cfg(test)]
mod tests {
    use super::*;

    const DID: &str = "did:plc:fillgapstest";

    fn held(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(k, c)| (format!("at://{DID}/{k}"), (*c).to_owned()))
            .collect()
    }

    fn repo(entries: &[(&str, &str)]) -> Vec<(String, String)> {
        entries
            .iter()
            .map(|(k, c)| ((*k).to_owned(), (*c).to_owned()))
            .collect()
    }

    #[test]
    fn plan_writes_only_missing_and_changed_records() {
        let repo = repo(&[
            ("app.bsky.feed.post/a", "cid-a"),
            ("app.bsky.feed.post/b", "cid-b2"),
            ("app.bsky.feed.like/c", "cid-c"),
            ("app.bsky.graph.follow/d", "cid-d"),
        ]);
        let held = held(&[
            ("app.bsky.feed.post/a", "cid-a"),
            ("app.bsky.feed.post/b", "cid-b1"),
            ("app.bsky.feed.post/gone", "cid-gone"),
            ("app.bsky.graph.follow/d", "cid-d"),
        ]);

        let (needed, extra) = plan(DID, &repo, &held, |_| true);

        assert_eq!(
            needed,
            vec![
                Needed {
                    uri: format!("at://{DID}/app.bsky.feed.post/b"),
                    key: "app.bsky.feed.post/b".to_owned(),
                    cid: "cid-b2".to_owned(),
                    need: Need::Changed,
                },
                Needed {
                    uri: format!("at://{DID}/app.bsky.feed.like/c"),
                    key: "app.bsky.feed.like/c".to_owned(),
                    cid: "cid-c".to_owned(),
                    need: Need::Missing,
                },
            ]
        );
        assert_eq!(extra, 1, "the deleted post is counted, not written");
    }

    #[test]
    fn plan_skips_collections_that_are_not_indexed() {
        let repo = repo(&[
            ("app.bsky.feed.post/a", "cid-a"),
            ("chat.bsky.actor.declaration/self", "cid-x"),
        ]);
        let (needed, extra) = plan(DID, &repo, &HashMap::new(), |c| c == "app.bsky.feed.post");
        assert_eq!(needed.len(), 1);
        assert_eq!(needed[0].key, "app.bsky.feed.post/a");
        assert_eq!(extra, 0);
    }

    #[test]
    fn plan_of_a_complete_account_is_empty() {
        let entries = [
            ("app.bsky.feed.post/a", "cid-a"),
            ("app.bsky.feed.like/b", "cid-b"),
        ];
        let (needed, extra) = plan(DID, &repo(&entries), &held(&entries), |_| true);
        assert!(needed.is_empty());
        assert_eq!(extra, 0);
    }

    #[test]
    fn dids_come_from_flags_and_file_once_each() {
        let flags = vec![
            "did:plc:aaa,did:plc:bbb".to_owned(),
            "did:plc:aaa".to_owned(),
        ];
        let file = "did:plc:ccc\n\n# a comment\ndid:plc:bbb  # again\nnot-a-did\n";
        assert_eq!(
            collect_dids(&flags, Some(file)),
            vec!["did:plc:aaa", "did:plc:bbb", "did:plc:ccc"]
        );
    }
}
