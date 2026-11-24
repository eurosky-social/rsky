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
//! missed (`extra`). With `--delete-extra` they are removed through the
//! indexer, exactly as a live delete would remove them (the record, its row
//! in the typed table, and the counts it was part of). Only records
//! indexed from the downloaded revision or an older one are removed: a record
//! written after the download has a newer revision and stays (`kept`), and
//! the indexer checks the revision again when it deletes. An account whose
//! deletes would be more than `--max-delete-percent` of its repository (and
//! more than 100 records) has its deletes refused and is reported, since
//! that looks more like a broken download than missed deletes.
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

    /// Also remove the records the repository no longer has.
    #[arg(long)]
    delete_extra: bool,

    /// Refuse an account's deletes when they are more than this percentage
    /// of the records in its repository (and more than 100).
    #[arg(long, default_value_t = 25)]
    max_delete_percent: usize,
}

/// Deletes below this many are never refused as out of proportion.
const DELETE_CAP_FLOOR: usize = 100;

/// What one account needed and got.
#[derive(Debug, Default)]
struct Report {
    in_repo: usize,
    missing: usize,
    changed: usize,
    /// Held records the repository no longer has, removable.
    extra: usize,
    /// Held records the repository doesn't have but that are newer than the
    /// download or carry no revision; never removed.
    kept: usize,
    written: usize,
    deleted: usize,
    failed: usize,
    car_bytes: usize,
    /// The deletes were refused as out of proportion.
    deletes_refused: bool,
    /// Missing or changed records by collection.
    needed_by_collection: HashMap<String, usize>,
    /// Removable records by collection.
    extra_by_collection: HashMap<String, usize>,
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

/// A record the AppView holds for the account.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Held {
    cid: String,
    /// The repository revision it was indexed from, if one was kept.
    rev: Option<String>,
}

/// How a repository and the records held for its account differ.
#[derive(Debug, Default, PartialEq, Eq)]
struct Plan {
    /// Records of the repository to index.
    needed: Vec<Needed>,
    /// Held records the repository no longer has, indexed from its revision
    /// or an older one: deletes that were missed. Sorted.
    extra: Vec<String>,
    /// Held records the repository doesn't have but that were indexed from a
    /// newer revision (written after the download) or carry no revision.
    kept: usize,
}

/// Compares a repository at revision `rev` (`collection/rkey` → CID) with the
/// records held for the account (URI → CID and revision).
fn plan(
    did: &str,
    rev: &str,
    repo: &[(String, String)],
    held: &HashMap<String, Held>,
    allowed: impl Fn(&str) -> bool,
) -> Plan {
    let mut plan = Plan::default();
    let mut in_repo = HashSet::with_capacity(repo.len());
    for (key, cid) in repo {
        let uri = format!("at://{did}/{key}");
        let indexed = key
            .split_once('/')
            .is_some_and(|(collection, _rkey)| allowed(collection));
        let need = match held.get(&uri) {
            _ if !indexed => None,
            None => Some(Need::Missing),
            Some(h) if h.cid != *cid => Some(Need::Changed),
            Some(_) => None,
        };
        if let Some(need) = need {
            plan.needed.push(Needed {
                uri: uri.clone(),
                key: key.clone(),
                cid: cid.clone(),
                need,
            });
        }
        // Every record of the repository, also in collections not indexed:
        // a held one from a collection dropped later is not a delete.
        in_repo.insert(uri);
    }
    for (uri, h) in held {
        if in_repo.contains(uri) {
            continue;
        }
        match &h.rev {
            Some(held_rev) if held_rev.as_str() <= rev => plan.extra.push(uri.clone()),
            _ => plan.kept += 1,
        }
    }
    plan.extra.sort();
    plan
}

/// Whether `deletes` for a repository of `in_repo` records are out of
/// proportion: more than the floor and more than `max_percent` of it.
const fn deletes_out_of_proportion(deletes: usize, in_repo: usize, max_percent: usize) -> bool {
    deletes > DELETE_CAP_FLOOR && deletes.saturating_mul(100) > in_repo.saturating_mul(max_percent)
}

/// `collection` of an `at://did/collection/rkey` URI.
fn collection_of(uri: &str) -> &str {
    uri.trim_start_matches("at://")
        .split('/')
        .nth(1)
        .unwrap_or("")
}

/// The outcome of [`apply_deletes`].
#[derive(Debug, Default, PartialEq, Eq)]
struct Deletes {
    deleted: usize,
    /// Still held afterwards: indexed again from a newer revision meanwhile.
    kept: usize,
    failed: usize,
    /// A post or follow was deleted, so the profile counts need recomputing.
    counted: bool,
}

/// Removes records through the indexer at revision `rev`: it deletes a
/// record only if that was indexed from `rev` or an older revision, and
/// removes it as a live delete would.
async fn apply_deletes(
    pool: &deadpool_postgres::Pool,
    uris: &[String],
    rev: &str,
    generation: Option<i64>,
    indexed_at: &str,
) -> Deletes {
    let mut out = Deletes::default();
    for uri in uris {
        let job = IndexJob {
            uri: uri.clone(),
            cid: String::new(),
            action: WriteAction::Delete,
            indexed_at: indexed_at.to_owned(),
            record: None,
            rev: rev.to_owned(),
            provenance: Some(Provenance {
                generation,
                source: Source::Direct,
            }),
        };
        let applied = match IndexerManager::process_job(
            pool,
            &job,
            *rsky_wintermute::config::RECORD_SKIP_BOILERPLATE,
        )
        .await
        {
            // The indexer answers Ok also when the revision kept the record.
            Ok(()) => still_held(pool, uri).await.map(|held| !held),
            Err(e) => Err(color_eyre::eyre::Report::from(e)),
        };
        match applied {
            Ok(true) => {
                out.deleted += 1;
                out.counted |= matches!(
                    collection_of(uri),
                    "app.bsky.feed.post" | "app.bsky.graph.follow"
                );
            }
            Ok(false) => out.kept += 1,
            Err(e) => {
                warn!("{uri}: delete: {e}");
                out.failed += 1;
            }
        }
    }
    out
}

async fn still_held(pool: &deadpool_postgres::Pool, uri: &str) -> Result<bool> {
    let client = pool.get().await?;
    Ok(client
        .query_opt("SELECT 1 FROM record WHERE uri = $1", &[&uri])
        .await?
        .is_some())
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
    delete_extra: bool,
    max_delete_percent: usize,
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
        delete_extra: args.delete_extra,
        max_delete_percent: args.max_delete_percent,
    });
    info!(
        "{} accounts, {} at a time; {}{}",
        dids.len(),
        args.concurrency,
        if args.dry_run { "dry run" } else { "WRITING" },
        if args.delete_extra {
            ", deleting extra records"
        } else {
            ""
        }
    );

    println!(
        "did\tin_repo\tmissing\tchanged\textra\tkept\twritten\tdeleted\tfailed\tcar_bytes\tdeletes_refused\terror"
    );
    // The summed report, accounts with errors, accounts with deletes refused.
    let totals = Arc::new(Mutex::new((Report::default(), 0usize, 0usize)));
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
                    "{did}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{error}",
                    report.in_repo,
                    report.missing,
                    report.changed,
                    report.extra,
                    report.kept,
                    report.written,
                    report.deleted,
                    report.failed,
                    report.car_bytes,
                    u8::from(report.deletes_refused),
                );
                let mut totals = totals
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let (sum, errors, refused) = &mut *totals;
                if !error.is_empty() {
                    *errors += 1;
                }
                if report.deletes_refused {
                    *refused += 1;
                }
                sum.in_repo += report.in_repo;
                sum.missing += report.missing;
                sum.changed += report.changed;
                sum.extra += report.extra;
                sum.kept += report.kept;
                sum.written += report.written;
                sum.deleted += report.deleted;
                sum.failed += report.failed;
                sum.car_bytes += report.car_bytes;
                for (collection, n) in report.needed_by_collection {
                    *sum.needed_by_collection.entry(collection).or_default() += n;
                }
                for (collection, n) in report.extra_by_collection {
                    *sum.extra_by_collection.entry(collection).or_default() += n;
                }
            }
        })
        .await;

    let totals = totals
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (sum, errors, refused) = &*totals;
    info!(
        "done: records in repositories {}, missing {}, changed {}, extra {}, kept {}, written {}, \
         deleted {}, failed {}, downloaded {} MB, accounts with errors {}, accounts with deletes \
         refused {}",
        sum.in_repo,
        sum.missing,
        sum.changed,
        sum.extra,
        sum.kept,
        sum.written,
        sum.deleted,
        sum.failed,
        sum.car_bytes / (1024 * 1024),
        errors,
        refused
    );
    info!(
        "missing or changed by collection: {:?}",
        by_count(&sum.needed_by_collection)
    );
    info!(
        "extra by collection: {:?}",
        by_count(&sum.extra_by_collection)
    );
    Ok(())
}

/// Collection counts, largest first.
fn by_count(counts: &HashMap<String, usize>) -> Vec<(&str, usize)> {
    let mut sorted: Vec<_> = counts.iter().map(|(c, n)| (c.as_str(), *n)).collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
    sorted
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

    let held: HashMap<String, Held> = {
        let client = ctx.pool.get().await?;
        client
            .query("SELECT uri, cid, rev FROM record WHERE did = $1", &[&did])
            .await?
            .into_iter()
            .map(|row| {
                (
                    row.get(0),
                    Held {
                        cid: row.get(1),
                        rev: row.get(2),
                    },
                )
            })
            .collect()
    };
    let repo_records: Vec<(String, String)> = leaves
        .iter()
        .map(|leaf| (leaf.key.clone(), leaf.value.to_string()))
        .collect();
    let Plan {
        needed,
        extra,
        kept,
    } = plan(
        did,
        &rev,
        &repo_records,
        &held,
        rsky_wintermute::config::ingest_collection_allowed,
    );

    let mut report = Report {
        in_repo: repo_records.len(),
        extra: extra.len(),
        kept,
        car_bytes: car.len(),
        deletes_refused: ctx.delete_extra
            && deletes_out_of_proportion(extra.len(), repo_records.len(), ctx.max_delete_percent),
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
    for uri in &extra {
        *report
            .extra_by_collection
            .entry(collection_of(uri).to_owned())
            .or_default() += 1;
    }
    let deletes: &[String] = if ctx.delete_extra && !report.deletes_refused {
        &extra
    } else {
        &[]
    };
    if ctx.dry_run || (needed.is_empty() && deletes.is_empty()) {
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

    let deleted = apply_deletes(
        &ctx.pool,
        deletes,
        &rev,
        generation,
        &now.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
    )
    .await;
    report.deleted = deleted.deleted;
    report.kept += deleted.kept;
    report.failed += deleted.failed;

    // The profile's post and follow counts, as direct_index recomputes them.
    if wrote_counted || deleted.counted {
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
    /// The revision of the downloaded repository in the plan tests.
    const REV: &str = "3mmmmmmmmmmm2";

    fn held(entries: &[(&str, &str)]) -> HashMap<String, Held> {
        held_at(
            &entries
                .iter()
                .map(|(k, c)| (*k, *c, Some(REV)))
                .collect::<Vec<_>>(),
        )
    }

    fn held_at(entries: &[(&str, &str, Option<&str>)]) -> HashMap<String, Held> {
        entries
            .iter()
            .map(|(k, c, rev)| {
                (
                    format!("at://{DID}/{k}"),
                    Held {
                        cid: (*c).to_owned(),
                        rev: rev.map(str::to_owned),
                    },
                )
            })
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

        let plan = plan(DID, REV, &repo, &held, |_| true);

        assert_eq!(
            plan.needed,
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
        assert_eq!(
            plan.extra,
            vec![format!("at://{DID}/app.bsky.feed.post/gone")],
            "the deleted post is a missed delete, not written"
        );
        assert_eq!(plan.kept, 0);
    }

    #[test]
    fn plan_skips_collections_that_are_not_indexed() {
        let repo = repo(&[
            ("app.bsky.feed.post/a", "cid-a"),
            ("chat.bsky.actor.declaration/self", "cid-x"),
        ]);
        let plan = plan(DID, REV, &repo, &HashMap::new(), |c| {
            c == "app.bsky.feed.post"
        });
        assert_eq!(plan.needed.len(), 1);
        assert_eq!(plan.needed[0].key, "app.bsky.feed.post/a");
        assert!(plan.extra.is_empty());
    }

    #[test]
    fn plan_never_deletes_a_record_the_repository_still_has() {
        // Held from when its collection was indexed; the repository still
        // has it, so it is no delete even though it is no longer indexed.
        let entries = [
            ("app.bsky.feed.post/a", "cid-a"),
            ("chat.bsky.actor.declaration/self", "cid-x"),
        ];
        let plan = plan(DID, REV, &repo(&entries), &held(&entries), |c| {
            c == "app.bsky.feed.post"
        });
        assert_eq!(plan, Plan::default());
    }

    #[test]
    fn plan_deletes_only_records_indexed_up_to_the_downloaded_revision() {
        let held = held_at(&[
            ("app.bsky.feed.like/older", "c1", Some("3maaaaaaaaaa2")),
            ("app.bsky.feed.like/same", "c2", Some(REV)),
            ("app.bsky.feed.like/newer", "c3", Some("3mzzzzzzzzzz2")),
            ("app.bsky.feed.like/unversioned", "c4", None),
        ]);
        let plan = plan(DID, REV, &[], &held, |_| true);
        assert_eq!(
            plan.extra,
            vec![
                format!("at://{DID}/app.bsky.feed.like/older"),
                format!("at://{DID}/app.bsky.feed.like/same"),
            ]
        );
        assert_eq!(plan.kept, 2, "written after the download, or no revision");
    }

    #[test]
    fn plan_of_a_complete_account_is_empty() {
        let entries = [
            ("app.bsky.feed.post/a", "cid-a"),
            ("app.bsky.feed.like/b", "cid-b"),
        ];
        let plan = plan(DID, REV, &repo(&entries), &held(&entries), |_| true);
        assert_eq!(plan, Plan::default());
    }

    #[test]
    fn deletes_out_of_proportion_need_both_the_floor_and_the_share() {
        assert!(!deletes_out_of_proportion(100, 10, 25), "at the floor");
        assert!(deletes_out_of_proportion(101, 400, 25));
        assert!(!deletes_out_of_proportion(101, 404, 25), "exactly 25%");
        assert!(deletes_out_of_proportion(2265, 1895, 25));
        assert!(!deletes_out_of_proportion(630, 2805, 25));
    }

    #[test]
    fn collection_of_a_uri() {
        assert_eq!(
            collection_of("at://did:plc:a/app.bsky.feed.like/3k"),
            "app.bsky.feed.like"
        );
        assert_eq!(collection_of("at://did:plc:a"), "");
    }

    fn test_pool() -> deadpool_postgres::Pool {
        let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/bsky_test".to_owned()
        });
        rsky_wintermute::config::create_pg_pool(&url, rsky_wintermute::config::pg_pool_config(4))
            .unwrap()
    }

    #[tokio::test]
    async fn apply_deletes_removes_only_records_up_to_the_revision() {
        let did = "did:plc:fillgapsdeletes";
        let pool = test_pool();
        let client = pool.get().await.unwrap();
        for sql in [
            "DELETE FROM \"like\" WHERE creator = $1",
            "DELETE FROM notification WHERE author = $1",
            "DELETE FROM record WHERE did = $1",
        ] {
            client.execute(sql, &[&did]).await.unwrap();
        }

        let older = format!("at://{did}/app.bsky.feed.like/older");
        let newer = format!("at://{did}/app.bsky.feed.like/newer");
        // Each like its own subject: the like table keeps one per subject.
        for (uri, rev, post) in [
            (&older, "3maaaaaaaaaa2", "p1"),
            (&newer, "3mzzzzzzzzzz2", "p2"),
        ] {
            let job = IndexJob {
                uri: uri.clone(),
                cid: format!("bafy{rev}"),
                action: WriteAction::Create,
                record: Some(serde_json::json!({
                    "$type": "app.bsky.feed.like",
                    "subject": {
                        "uri": format!("at://did:plc:fillgapsauthor/app.bsky.feed.post/{post}"),
                        "cid": "bafypost",
                    },
                    "createdAt": "2026-07-01T00:00:00.000Z",
                })),
                indexed_at: "2026-07-01T00:00:00.000Z".to_owned(),
                rev: rev.to_owned(),
                provenance: None,
            };
            IndexerManager::process_job(&pool, &job, false)
                .await
                .unwrap();
        }

        let out = apply_deletes(
            &pool,
            &[older.clone(), newer.clone()],
            "3mmmmmmmmmmm2",
            None,
            "2026-11-24T00:00:00.000Z",
        )
        .await;

        assert_eq!(
            out,
            Deletes {
                deleted: 1,
                kept: 1,
                failed: 0,
                counted: false,
            }
        );
        let held: Vec<String> = client
            .query("SELECT uri FROM record WHERE did = $1", &[&did])
            .await
            .unwrap()
            .iter()
            .map(|row| row.get(0))
            .collect();
        assert_eq!(held, vec![newer.clone()]);
        let likes: Vec<String> = client
            .query("SELECT uri FROM \"like\" WHERE creator = $1", &[&did])
            .await
            .unwrap()
            .iter()
            .map(|row| row.get(0))
            .collect();
        assert_eq!(likes, vec![newer]);
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
