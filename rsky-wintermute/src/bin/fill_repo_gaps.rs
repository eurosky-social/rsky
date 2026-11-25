//! Fills the gaps in accounts' indexed history (EUR-654).
//!
//! For each account it reads the whole repository from the account's PDS,
//! compares it with the records the AppView already holds (by URI and CID,
//! from `record` and, for likes, reposts, follows and blocks written with
//! `RECORD_SKIP_BOILERPLATE`, their typed tables), and indexes only the
//! records that are missing or held in an older version. Records already
//! present are not written again, so a fill costs writes only for the gaps.
//! A like, repost, follow or block of a subject the account already has one
//! of is a `duplicate`: the typed tables keep one per subject, so it is
//! counted and not written.
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
//! the indexer checks the revision again when it deletes. A record held only
//! in its typed table has no revision; it is removed only if it was indexed
//! more than a minute before the download started, so one created after the
//! download stays. Don't run two fills over the same account at once: a
//! record the other one writes from a newer download carries its `createdAt`
//! as the time it was indexed. An account whose
//! deletes would be more than `--max-delete-percent` of its repository (and
//! more than 100 records) has its deletes refused and is reported, since
//! that looks more like a broken download than missed deletes.
//!
//! `--repo-host` reads every repository from one host instead of each
//! account's PDS, for example a full-network mirror such as Hubble; no DID
//! is resolved then. `--pds` lists the accounts of any host that serves
//! `com.atproto.sync.listRepos`, and `--committed-since` keeps only those
//! whose latest commit is on or after a date: an account with no commit
//! since a gap began can have nothing missing from it. `--max-rps` spaces
//! the requests to those hosts, and a 429 or 5xx answer is tried again
//! after its `Retry-After` (or a growing pause). `--list-only` prints the
//! accounts it would read and stops.
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
use rsky_wintermute::held::{self, Held, HeldIn, Holdings, collection_of};
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

    /// Every active account on this host, from its
    /// `com.atproto.sync.listRepos`: a PDS, or a mirror such as Hubble.
    #[arg(long)]
    pds: Option<String>,

    /// With `--pds`, only accounts whose latest commit is on or after this
    /// date (YYYY-MM-DD, UTC). Accounts without a readable revision stay in.
    #[arg(long)]
    committed_since: Option<chrono::NaiveDate>,

    /// Read every repository from this host instead of the account's PDS,
    /// without resolving the DID.
    #[arg(long)]
    repo_host: Option<String>,

    /// Requests per second to the hosts read from, evenly spaced (0 = no limit).
    #[arg(long, default_value_t = 0.0)]
    max_rps: f64,

    /// Print the accounts that would be read, one per line, and stop.
    #[arg(long)]
    list_only: bool,

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

/// Tries per request, the first included.
const MAX_ATTEMPTS: u32 = 5;

/// The longest pause before trying a request again.
const MAX_RETRY_PAUSE: Duration = Duration::from_secs(300);

const USER_AGENT: &str = "rsky-wintermute-fill-repo-gaps/0.2";

/// Spaces requests evenly, at most `per_second`, across every task.
struct RateLimiter {
    interval: Duration,
    next: tokio::sync::Mutex<tokio::time::Instant>,
}

impl RateLimiter {
    /// `None` for no limit (`per_second` of 0 or less).
    fn new(per_second: f64) -> Option<Self> {
        (per_second > 0.0).then(|| Self {
            interval: Duration::from_secs_f64(1.0 / per_second),
            next: tokio::sync::Mutex::new(tokio::time::Instant::now()),
        })
    }

    /// Waits for this caller's slot.
    async fn wait(&self) {
        let slot = {
            let mut next = self.next.lock().await;
            let slot = (*next).max(tokio::time::Instant::now());
            *next = slot + self.interval;
            slot
        };
        tokio::time::sleep_until(slot).await;
    }
}

/// How long to wait before trying again after an answer with `status`, or
/// `None` when trying again won't help. `Retry-After` in seconds wins over
/// the doubling pause; both are capped at [`MAX_RETRY_PAUSE`].
fn retry_pause(
    status: reqwest::StatusCode,
    retry_after: Option<&str>,
    attempt: u32,
) -> Option<Duration> {
    use reqwest::StatusCode;
    let worth_it = status == StatusCode::TOO_MANY_REQUESTS
        || (status.is_server_error() && status != StatusCode::NOT_IMPLEMENTED);
    if !worth_it {
        return None;
    }
    let doubling = Duration::from_secs(2u64.saturating_pow(attempt));
    let pause = retry_after
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map_or(doubling, Duration::from_secs);
    Some(pause.min(MAX_RETRY_PAUSE))
}

/// A GET that waits for its rate-limit slot, and tries again after a 429,
/// a 5xx or a connection error, up to [`MAX_ATTEMPTS`] in all. Returns the
/// last answer, whatever its status.
async fn get_patiently(
    http: &reqwest::Client,
    limiter: Option<&RateLimiter>,
    url: reqwest::Url,
) -> Result<reqwest::Response> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        if let Some(limiter) = limiter {
            limiter.wait().await;
        }
        match http.get(url.clone()).send().await {
            Ok(resp) => {
                let retry_after = resp
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok());
                match retry_pause(resp.status(), retry_after, attempt) {
                    Some(pause) if attempt < MAX_ATTEMPTS => {
                        warn!(
                            "{}: HTTP {}, trying again in {} s",
                            url.path(),
                            resp.status(),
                            pause.as_secs()
                        );
                        tokio::time::sleep(pause).await;
                    }
                    _ => return Ok(resp),
                }
            }
            Err(e) if attempt < MAX_ATTEMPTS && (e.is_connect() || e.is_timeout()) => {
                tokio::time::sleep(Duration::from_secs(2u64.saturating_pow(attempt))).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// The TID of the first moment of `date` (UTC). Revisions are TIDs, which
/// sort as strings, so a revision at or above it was made on `date` or later.
fn tid_at_start_of(date: chrono::NaiveDate) -> String {
    let micros = date
        .and_time(chrono::NaiveTime::MIN)
        .and_utc()
        .timestamp_micros();
    rsky_common::tid::TID::from_time(usize::try_from(micros).unwrap_or(0), 0).0
}

/// Whether an account whose latest commit is `rev` may have committed on or
/// after `since` (a TID). A missing or malformed revision keeps the account.
fn committed_since(rev: Option<&str>, since: &str) -> bool {
    match rev {
        Some(rev) if rev.len() == since.len() => rev >= since,
        _ => true,
    }
}

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
    /// Likes, reposts, follows and blocks of a subject already held under
    /// another URI; not written.
    duplicates: usize,
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

/// How long before the download a record held only in its typed table must
/// have been indexed to count as a missed delete.
const TYPED_DELETE_MARGIN: chrono::TimeDelta = chrono::TimeDelta::minutes(1);

/// Whether a record held only in its typed table, indexed at `indexed_at`,
/// was there before a download that started at `download_started`.
fn indexed_before(indexed_at: &str, download_started: chrono::DateTime<Utc>) -> bool {
    chrono::DateTime::parse_from_rfc3339(indexed_at)
        .is_ok_and(|at| at.with_timezone(&Utc) < download_started - TYPED_DELETE_MARGIN)
}

/// Compares a repository at revision `rev`, downloaded from
/// `download_started` on (`collection/rkey` → CID), with the records held for
/// the account (URI → CID and where it is held). A like, repost, follow or
/// block in `untyped` is held in `record` but not in its typed table, so it
/// counts as missing.
fn plan(
    did: &str,
    rev: &str,
    download_started: chrono::DateTime<Utc>,
    repo: &[(String, String)],
    held: &HashMap<String, Held>,
    untyped: &HashSet<String>,
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
            Some(_) if untyped.contains(&uri) => Some(Need::Missing),
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
        let removable = match &h.held_in {
            HeldIn::Record {
                rev: Some(held_rev),
            } => held_rev.as_str() <= rev,
            HeldIn::Record { rev: None } => false,
            HeldIn::Typed { indexed_at } => indexed_before(indexed_at, download_started),
        };
        if removable {
            plan.extra.push(uri.clone());
        } else {
            plan.kept += 1;
        }
    }
    plan.extra.sort();
    plan
}

/// Whether a needed record of `collection` is a like, repost, follow or
/// block of a subject the account already has one of (`holds`), or that an
/// earlier record of this repository is being written for (`writing`, which
/// it joins otherwise).
fn is_duplicate(
    need: &Need,
    collection: &str,
    record: &serde_json::Value,
    holds: impl Fn(&str, &str) -> bool,
    writing: &mut HashSet<(String, String)>,
) -> bool {
    *need == Need::Missing
        && held::subject_of(collection, record).is_some_and(|subject| {
            holds(collection, &subject) || !writing.insert((collection.to_owned(), subject))
        })
}

/// Whether `deletes` for a repository of `in_repo` records are out of
/// proportion: more than the floor and more than `max_percent` of it.
const fn deletes_out_of_proportion(deletes: usize, in_repo: usize, max_percent: usize) -> bool {
    deletes > DELETE_CAP_FLOOR && deletes.saturating_mul(100) > in_repo.saturating_mul(max_percent)
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

/// Removes records through the indexer at revision `rev`, as a live delete
/// would. A record in `record` is deleted only if it was indexed from `rev`
/// or an older revision; with `skip_boilerplate`, likes, reposts, follows and
/// blocks are deleted from their typed tables whatever their revision, as the
/// indexer does then.
async fn apply_deletes(
    pool: &deadpool_postgres::Pool,
    uris: &[String],
    rev: &str,
    generation: Option<i64>,
    indexed_at: &str,
    skip_boilerplate: bool,
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
        let applied = match IndexerManager::process_job(pool, &job, skip_boilerplate).await {
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
    Ok(held::still_held(&client, uri).await?)
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
    /// For hosts named by DID documents: the guarded outbound transport.
    http: reqwest::Client,
    /// For the hosts given on the command line (`--repo-host`, `--pds`).
    host_http: reqwest::Client,
    repo_host: Option<String>,
    limiter: Option<RateLimiter>,
    resolver: IdResolver,
    max_car_bytes: usize,
    dry_run: bool,
    delete_extra: bool,
    max_delete_percent: usize,
    /// `RECORD_SKIP_BOILERPLATE`, as the indexer runs with it.
    skip_boilerplate: bool,
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
        .user_agent(USER_AGENT)
        .build()?;
    let host_http = reqwest::Client::builder()
        .timeout(Duration::from_secs(300))
        .user_agent(USER_AGENT)
        .build()?;
    let limiter = RateLimiter::new(args.max_rps);

    let file = match &args.dids_file {
        Some(path) => Some(std::fs::read_to_string(path)?),
        None => None,
    };
    let mut dids = collect_dids(&args.dids, file.as_deref());
    if let Some(pds) = &args.pds {
        let since = args.committed_since.map(tid_at_start_of);
        let listed = list_active_repos(
            &host_http,
            limiter.as_ref(),
            pds,
            since.as_deref(),
            args.limit,
        )
        .await?;
        info!(
            "{} active repositories on {pds}{}",
            listed.len(),
            args.committed_since
                .map_or_else(String::new, |d| format!(" with a commit since {d}"))
        );
        let mut seen: HashSet<String> = dids.iter().cloned().collect();
        dids.extend(listed.into_iter().filter(|d| seen.insert(d.clone())));
    }
    if args.limit > 0 {
        dids.truncate(args.limit);
    }
    if dids.is_empty() {
        return Err(eyre!("no accounts given: use --did, --dids-file or --pds"));
    }
    if args.list_only {
        for did in &dids {
            println!("{did}");
        }
        return Ok(());
    }

    let ctx = Arc::new(Ctx {
        pool: rsky_wintermute::config::create_pg_pool(
            &args.database_url,
            rsky_wintermute::config::pg_pool_config(args.concurrency * 2 + 2),
        )?,
        http,
        host_http,
        repo_host: args.repo_host.clone(),
        limiter,
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
        skip_boilerplate: *rsky_wintermute::config::RECORD_SKIP_BOILERPLATE,
    });
    info!(
        "{} accounts, {} at a time; {}{}{}",
        dids.len(),
        args.concurrency,
        if args.dry_run { "dry run" } else { "WRITING" },
        if args.delete_extra {
            ", deleting extra records"
        } else {
            ""
        },
        if ctx.skip_boilerplate {
            "; likes, reposts, follows and blocks go to their tables only"
        } else {
            ""
        }
    );

    println!(
        "did\tin_repo\tmissing\tchanged\textra\tkept\tduplicate\twritten\tdeleted\tfailed\tcar_bytes\tdeletes_refused\terror"
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
                    "{did}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{error}",
                    report.in_repo,
                    report.missing,
                    report.changed,
                    report.extra,
                    report.kept,
                    report.duplicates,
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
                sum.duplicates += report.duplicates;
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
        "done: records in repositories {}, missing {}, changed {}, extra {}, kept {}, duplicates {}, \
         written {}, deleted {}, failed {}, downloaded {} MB, accounts with errors {}, accounts \
         with deletes refused {}",
        sum.in_repo,
        sum.missing,
        sum.changed,
        sum.extra,
        sum.kept,
        sum.duplicates,
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
    let download_started = Utc::now();
    let resp = if let Some(host) = &ctx.repo_host {
        let url = reqwest::Url::parse_with_params(
            &format!(
                "{}/xrpc/com.atproto.sync.getRepo",
                host.trim_end_matches('/')
            ),
            &[("did", did)],
        )?;
        get_patiently(&ctx.host_http, ctx.limiter.as_ref(), url).await?
    } else {
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
        get_patiently(&ctx.http, ctx.limiter.as_ref(), url).await?
    };
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

    let holdings: Holdings = {
        let client = ctx.pool.get().await?;
        held::holdings(&client, did).await?
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
        download_started,
        &repo_records,
        &holdings.records,
        &holdings.untyped,
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
    let deleting: HashSet<&str> = deletes.iter().map(String::as_str).collect();

    // Read the needed records from the repository. A like, repost, follow or
    // block of a subject the account already has one of is a duplicate: its
    // typed table keeps one per subject, so writing it would change nothing.
    // One whose subject is held only by a record deleted now (an unlike and
    // a like again, both missed) is written, after that delete.
    let cids: HashMap<&str, _> = leaves
        .iter()
        .map(|leaf| (leaf.key.as_str(), leaf.value))
        .collect();
    let found = {
        let wanted: Vec<_> = needed
            .iter()
            .filter_map(|n| cids.get(n.key.as_str()).copied())
            .collect();
        let storage = repo.storage.read().await;
        storage
            .get_blocks(wanted)
            .await
            .map_err(|e| eyre!("reading records: {e}"))?
    };
    let mut to_write: Vec<(&Needed, serde_json::Value)> = Vec::with_capacity(needed.len());
    let mut subjects_written = HashSet::new();
    for n in &needed {
        let Some(parsed) = cids
            .get(n.key.as_str())
            .and_then(|cid| get_and_parse_record(&found.blocks, *cid).ok())
        else {
            report.failed += 1;
            continue;
        };
        let raw = serde_json::to_value(&parsed.record)?;
        let collection = n.key.split_once('/').map_or("", |(c, _)| c);
        if is_duplicate(
            &n.need,
            collection,
            &raw,
            |c, s| {
                holdings
                    .subject_held_by(c, s)
                    .is_some_and(|uri| !deleting.contains(uri))
            },
            &mut subjects_written,
        ) {
            report.duplicates += 1;
            continue;
        }
        match n.need {
            Need::Missing => report.missing += 1,
            Need::Changed => report.changed += 1,
        }
        *report
            .needed_by_collection
            .entry(collection.to_owned())
            .or_default() += 1;
        to_write.push((n, raw));
    }
    if ctx.dry_run || (to_write.is_empty() && deletes.is_empty()) {
        return Ok(report);
    }

    let generation = rsky_wintermute::reconcile::current_generation(&ctx.pool, did).await?;
    let now = Utc::now();
    // Deletes first: a record written for a subject a deleted one held is
    // refused by the typed table while that one is still there.
    let deleted = apply_deletes(
        &ctx.pool,
        deletes,
        &rev,
        generation,
        &now.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
        ctx.skip_boilerplate,
    )
    .await;
    report.deleted = deleted.deleted;
    report.kept += deleted.kept;
    report.failed += deleted.failed;

    let mut wrote_counted = false;
    for (n, raw) in &to_write {
        let job = IndexJob {
            uri: n.uri.clone(),
            cid: n.cid.clone(),
            action: WriteAction::Create,
            indexed_at: repaired_indexed_at(raw, now),
            record: Some(convert_record_to_ipld(raw)),
            rev: rev.clone(),
            provenance: Some(Provenance {
                generation,
                source: Source::Direct,
            }),
        };
        match IndexerManager::process_job(&ctx.pool, &job, ctx.skip_boilerplate).await {
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

/// The active accounts `host` lists with `com.atproto.sync.listRepos`, in
/// its order; with `since` (a TID), only those committed on or after it.
/// Stops once it has `stop_after` accounts (0 = never).
async fn list_active_repos(
    http: &reqwest::Client,
    limiter: Option<&RateLimiter>,
    host: &str,
    since: Option<&str>,
    stop_after: usize,
) -> Result<Vec<String>> {
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
        #[serde(default)]
        rev: Option<String>,
    }
    const fn active_default() -> bool {
        true
    }

    let endpoint = format!(
        "{}/xrpc/com.atproto.sync.listRepos",
        host.trim_end_matches('/')
    );
    let (mut dids, mut seen, mut pages) = (Vec::new(), 0usize, 0usize);
    let mut cursor: Option<String> = None;
    loop {
        let mut url = reqwest::Url::parse_with_params(&endpoint, &[("limit", "1000")])?;
        if let Some(c) = &cursor {
            url.query_pairs_mut().append_pair("cursor", c);
        }
        let resp = get_patiently(http, limiter, url).await?;
        if !resp.status().is_success() {
            return Err(eyre!("listRepos: HTTP {}", resp.status()));
        }
        let page: Page = resp.json().await?;
        let empty = page.repos.is_empty();
        seen += page.repos.len();
        dids.extend(
            page.repos
                .into_iter()
                .filter(|r| r.active)
                .filter(|r| since.is_none_or(|s| committed_since(r.rev.as_deref(), s)))
                .map(|r| r.did),
        );
        cursor = page.cursor;
        pages += 1;
        if pages % 500 == 0 {
            info!(
                "listRepos: {pages} pages, {seen} repositories, {} kept",
                dids.len()
            );
        }
        if stop_after > 0 && dids.len() >= stop_after {
            dids.truncate(stop_after);
            break;
        }
        if empty || cursor.is_none() {
            break;
        }
    }
    Ok(dids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsky_repo::types::{RecordCreateOrUpdateOp, WriteOpAction};

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
                        held_in: HeldIn::Record {
                            rev: rev.map(str::to_owned),
                        },
                    },
                )
            })
            .collect()
    }

    /// When the download of the plan tests started.
    fn started() -> chrono::DateTime<Utc> {
        "2026-11-25T12:00:00Z".parse().unwrap()
    }

    fn typed(entries: &[(&str, &str, &str)]) -> HashMap<String, Held> {
        entries
            .iter()
            .map(|(k, c, indexed_at)| {
                (
                    format!("at://{DID}/{k}"),
                    Held {
                        cid: (*c).to_owned(),
                        held_in: HeldIn::Typed {
                            indexed_at: (*indexed_at).to_owned(),
                        },
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

        let plan = plan(DID, REV, started(), &repo, &held, &HashSet::new(), |_| true);

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
        let plan = plan(
            DID,
            REV,
            started(),
            &repo,
            &HashMap::new(),
            &HashSet::new(),
            |c| c == "app.bsky.feed.post",
        );
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
        let plan = plan(
            DID,
            REV,
            started(),
            &repo(&entries),
            &held(&entries),
            &HashSet::new(),
            |c| c == "app.bsky.feed.post",
        );
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
        let plan = plan(DID, REV, started(), &[], &held, &HashSet::new(), |_| true);
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
        let plan = plan(
            DID,
            REV,
            started(),
            &repo(&entries),
            &held(&entries),
            &HashSet::new(),
            |_| true,
        );
        assert_eq!(plan, Plan::default());
    }

    #[test]
    fn plan_removes_a_typed_only_record_only_if_indexed_before_the_download() {
        let held = typed(&[
            (
                "app.bsky.feed.like/hour-before",
                "c1",
                "2026-11-25T11:00:00.000Z",
            ),
            (
                "app.bsky.feed.like/seconds-before",
                "c2",
                "2026-11-25T11:59:30.000Z",
            ),
            ("app.bsky.feed.like/after", "c3", "2026-11-25T12:00:05.000Z"),
            ("app.bsky.feed.like/unreadable", "c4", "yesterday"),
        ]);
        let plan = plan(DID, REV, started(), &[], &held, &HashSet::new(), |_| true);
        assert_eq!(
            plan.extra,
            vec![format!("at://{DID}/app.bsky.feed.like/hour-before")]
        );
        assert_eq!(
            plan.kept, 3,
            "within the margin, after the download started, or no readable time"
        );
    }

    #[test]
    fn plan_compares_typed_only_records_by_cid() {
        let repo = repo(&[
            ("app.bsky.feed.like/same", "c1"),
            ("app.bsky.graph.follow/changed", "c2-new"),
        ]);
        let held = typed(&[
            ("app.bsky.feed.like/same", "c1", "2026-11-25T11:00:00.000Z"),
            (
                "app.bsky.graph.follow/changed",
                "c2-old",
                "2026-11-25T11:00:00.000Z",
            ),
        ]);
        let plan = plan(DID, REV, started(), &repo, &held, &HashSet::new(), |_| true);
        assert_eq!(plan.needed.len(), 1);
        assert_eq!(plan.needed[0].key, "app.bsky.graph.follow/changed");
        assert_eq!(plan.needed[0].need, Need::Changed);
        assert!(plan.extra.is_empty());
    }

    #[test]
    fn duplicates_are_second_likes_of_a_held_or_written_subject() {
        let post = "at://did:plc:b/app.bsky.feed.post/p";
        let like = serde_json::json!({"subject": {"uri": post, "cid": "c"}});
        let follow = serde_json::json!({"subject": "did:plc:c"});
        let held_post = |c: &str, s: &str| c == "app.bsky.feed.like" && s == post;
        let nothing = |_: &str, _: &str| false;
        let mut writing = HashSet::new();

        assert!(is_duplicate(
            &Need::Missing,
            "app.bsky.feed.like",
            &like,
            held_post,
            &mut writing
        ));
        assert!(
            !is_duplicate(
                &Need::Changed,
                "app.bsky.feed.like",
                &like,
                held_post,
                &mut writing
            ),
            "a held record in a new version is no duplicate"
        );
        assert!(!is_duplicate(
            &Need::Missing,
            "app.bsky.graph.follow",
            &follow,
            nothing,
            &mut writing
        ));
        assert!(
            is_duplicate(
                &Need::Missing,
                "app.bsky.graph.follow",
                &follow,
                nothing,
                &mut writing
            ),
            "a second follow of the same account in one repository"
        );
        let post_record = serde_json::json!({"text": "hi"});
        assert!(!is_duplicate(
            &Need::Missing,
            "app.bsky.feed.post",
            &post_record,
            nothing,
            &mut writing
        ));
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
    fn retry_pause_honours_retry_after_and_skips_hopeless_answers() {
        use reqwest::StatusCode;
        let pause = |status, retry_after, attempt| retry_pause(status, retry_after, attempt);
        assert_eq!(
            pause(StatusCode::TOO_MANY_REQUESTS, Some("7"), 1),
            Some(Duration::from_secs(7))
        );
        assert_eq!(
            pause(StatusCode::TOO_MANY_REQUESTS, None, 3),
            Some(Duration::from_secs(8)),
            "doubling without Retry-After"
        );
        assert_eq!(
            pause(
                StatusCode::TOO_MANY_REQUESTS,
                Some("Wed, 21 Oct 2026 07:28:00 GMT"),
                1
            ),
            Some(Duration::from_secs(2)),
            "a date falls back to the doubling pause"
        );
        assert_eq!(
            pause(StatusCode::SERVICE_UNAVAILABLE, Some("86400"), 1),
            Some(MAX_RETRY_PAUSE)
        );
        assert!(pause(StatusCode::INTERNAL_SERVER_ERROR, None, 1).is_some());
        assert_eq!(pause(StatusCode::NOT_IMPLEMENTED, None, 1), None);
        assert_eq!(pause(StatusCode::BAD_REQUEST, None, 1), None);
        assert_eq!(pause(StatusCode::NOT_FOUND, None, 1), None);
        assert_eq!(pause(StatusCode::OK, None, 1), None);
    }

    #[test]
    fn committed_since_compares_revisions_with_the_day_they_start() {
        // 3mq7bf4y36n2e is a revision from 2026-07-09 08:59 UTC.
        let day = |d: &str| tid_at_start_of(d.parse().unwrap());
        assert!(day("2026-07-09").as_str() < "3mq7bf4y36n2e");
        assert!(day("2026-07-10").as_str() > "3mq7bf4y36n2e");
        let since = day("2026-06-20");
        assert!(committed_since(Some("3mq7bf4y36n2e"), &since));
        assert!(
            committed_since(Some(since.as_str()), &since),
            "the first moment counts"
        );
        assert!(
            !committed_since(Some("3lido2jgaih2k"), &since),
            "2024 stays out"
        );
        assert!(committed_since(None, &since), "no revision stays in");
        assert!(
            committed_since(Some("legacy-rev"), &since),
            "malformed stays in"
        );
    }

    #[tokio::test]
    async fn rate_limiter_spaces_requests_evenly() {
        assert!(RateLimiter::new(0.0).is_none());
        let limiter = RateLimiter::new(50.0).unwrap();
        let start = tokio::time::Instant::now();
        for _ in 0..5 {
            limiter.wait().await;
        }
        // The first slot is now; four more follow 20 ms apart.
        assert!(start.elapsed() >= Duration::from_millis(80));
    }

    #[tokio::test]
    async fn get_patiently_tries_a_429_again_and_gives_up_after_the_last_try() {
        let mut server = mockito::Server::new_async().await;
        let throttled = server
            .mock("GET", "/xrpc/com.atproto.sync.getRepo")
            .match_query(mockito::Matcher::Any)
            .with_status(429)
            .with_header("retry-after", "0")
            .expect(usize::try_from(MAX_ATTEMPTS).unwrap())
            .create_async()
            .await;
        let url = reqwest::Url::parse(&format!(
            "{}/xrpc/com.atproto.sync.getRepo?did=did:plc:a",
            server.url()
        ))
        .unwrap();
        let resp = get_patiently(&reqwest::Client::new(), None, url)
            .await
            .unwrap();
        assert_eq!(resp.status(), 429);
        throttled.assert_async().await;
    }

    #[tokio::test]
    async fn get_patiently_returns_a_404_at_once() {
        let mut server = mockito::Server::new_async().await;
        let missing = server
            .mock("GET", "/xrpc/com.atproto.sync.getRepo")
            .match_query(mockito::Matcher::Any)
            .with_status(404)
            .expect(1)
            .create_async()
            .await;
        let url = reqwest::Url::parse(&format!(
            "{}/xrpc/com.atproto.sync.getRepo?did=did:plc:a",
            server.url()
        ))
        .unwrap();
        let resp = get_patiently(&reqwest::Client::new(), None, url)
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);
        missing.assert_async().await;
    }

    #[tokio::test]
    async fn list_active_repos_follows_the_cursor_and_keeps_recent_active_accounts() {
        use mockito::Matcher;
        let mut server = mockito::Server::new_async().await;
        let first = server
            .mock("GET", "/xrpc/com.atproto.sync.listRepos")
            .match_query(Matcher::Exact("limit=1000".to_owned()))
            .with_body(
                serde_json::json!({
                    "cursor": "did:plc:c",
                    "repos": [
                        {"did": "did:plc:a", "rev": "3mq7bf4y36n2e", "active": true},
                        {"did": "did:plc:b", "rev": "3lido2jgaih2k", "active": true},
                        {"did": "did:plc:c", "rev": "3mq7bf4y36n2e", "active": false},
                    ],
                })
                .to_string(),
            )
            .create_async()
            .await;
        let second = server
            .mock("GET", "/xrpc/com.atproto.sync.listRepos")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("limit".to_owned(), "1000".to_owned()),
                Matcher::UrlEncoded("cursor".to_owned(), "did:plc:c".to_owned()),
            ]))
            .with_body(
                serde_json::json!({"repos": [{"did": "did:plc:d", "active": true}]}).to_string(),
            )
            .create_async()
            .await;

        let since = tid_at_start_of("2026-06-20".parse().unwrap());
        let dids = list_active_repos(
            &reqwest::Client::new(),
            None,
            &server.url(),
            Some(&since),
            0,
        )
        .await
        .unwrap();

        assert_eq!(dids, vec!["did:plc:a", "did:plc:d"]);
        first.assert_async().await;
        second.assert_async().await;
    }

    #[tokio::test]
    async fn list_active_repos_stops_once_it_has_enough() {
        let mut server = mockito::Server::new_async().await;
        let first = server
            .mock("GET", "/xrpc/com.atproto.sync.listRepos")
            .match_query(mockito::Matcher::Exact("limit=1000".to_owned()))
            .with_body(
                serde_json::json!({
                    "cursor": "did:plc:b",
                    "repos": [{"did": "did:plc:a"}, {"did": "did:plc:b"}],
                })
                .to_string(),
            )
            .expect(1)
            .create_async()
            .await;
        let rest = server
            .mock("GET", "/xrpc/com.atproto.sync.listRepos")
            .match_query(mockito::Matcher::UrlEncoded(
                "cursor".to_owned(),
                "did:plc:b".to_owned(),
            ))
            .expect(0)
            .create_async()
            .await;

        let dids = list_active_repos(&reqwest::Client::new(), None, &server.url(), None, 1)
            .await
            .unwrap();

        assert_eq!(dids, vec!["did:plc:a"]);
        first.assert_async().await;
        rest.assert_async().await;
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
            false,
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

    fn keypair() -> secp256k1::Keypair {
        let seed: [u8; 32] = rand::random();
        secp256k1::Keypair::from_seckey_slice(secp256k1::SECP256K1, &seed).unwrap()
    }

    fn like(rkey: &str, post: &str) -> RecordCreateOrUpdateOp {
        RecordCreateOrUpdateOp {
            action: WriteOpAction::Create,
            collection: "app.bsky.feed.like".to_owned(),
            rkey: rkey.to_owned(),
            record: serde_json::from_value(serde_json::json!({
                "$type": "app.bsky.feed.like",
                "subject": {"uri": post, "cid": "bafyreihhl5mpvjkrhnnagen2fomozzhnhhdq2jr6cego2nzbvmwewv5rd4"},
                "createdAt": "2026-07-01T00:00:00.000Z",
            }))
            .unwrap(),
        }
    }

    /// A repository of `did` holding `writes`: the CAR `getRepo` serves, its
    /// revision, and its records' CIDs by `collection/rkey`.
    async fn repo_car(
        did: &str,
        writes: Vec<RecordCreateOrUpdateOp>,
    ) -> (Vec<u8>, String, HashMap<String, String>) {
        let storage = Arc::new(tokio::sync::RwLock::new(
            MemoryBlockstore::new(None).await.unwrap(),
        ));
        let mut repo = rsky_repo::repo::Repo::create(
            storage.clone(),
            did.to_owned(),
            &keypair(),
            Some(writes),
        )
        .await
        .unwrap();
        let cids = repo
            .data
            .list(None, None, None)
            .await
            .unwrap()
            .into_iter()
            .map(|leaf| (leaf.key, leaf.value.to_string()))
            .collect();
        let car: Vec<u8> = rsky_repo::sync::provider::get_full_repo(storage, repo.cid)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .flat_map(Result::unwrap)
            .collect();
        (car, repo.commit.rev.clone(), cids)
    }

    fn test_ctx(pool: deadpool_postgres::Pool, repo_host: String, skip_boilerplate: bool) -> Ctx {
        Ctx {
            pool,
            http: reqwest::Client::new(),
            host_http: reqwest::Client::new(),
            repo_host: Some(repo_host),
            limiter: None,
            resolver: IdResolver::new(IdentityResolverOpts {
                timeout: None,
                plc_url: None,
                did_cache: None,
                backup_nameservers: None,
            }),
            max_car_bytes: usize::MAX,
            dry_run: false,
            delete_extra: true,
            max_delete_percent: 25,
            skip_boilerplate,
        }
    }

    async fn clean(pool: &deadpool_postgres::Pool, did: &str) {
        let client = pool.get().await.unwrap();
        for sql in [
            "DELETE FROM \"like\" WHERE creator = $1",
            "DELETE FROM post WHERE creator = $1",
            "DELETE FROM feed_item WHERE \"originatorDid\" = $1",
            "DELETE FROM notification WHERE author = $1",
            "DELETE FROM record WHERE did = $1",
        ] {
            client.execute(sql, &[&did]).await.unwrap();
        }
    }

    /// Indexes a like as the indexer would have, long ago.
    async fn index_like(
        pool: &deadpool_postgres::Pool,
        uri: &str,
        cid: &str,
        post: &str,
        indexed_at: &str,
        skip_boilerplate: bool,
    ) {
        let job = IndexJob {
            uri: uri.to_owned(),
            cid: cid.to_owned(),
            action: WriteAction::Create,
            record: Some(serde_json::json!({
                "$type": "app.bsky.feed.like",
                "subject": {"uri": post, "cid": "bafypost"},
                "createdAt": "2026-07-01T00:00:00.000Z",
            })),
            indexed_at: indexed_at.to_owned(),
            rev: "3aaaaaaaaaaa2".to_owned(),
            provenance: None,
        };
        IndexerManager::process_job(pool, &job, skip_boilerplate)
            .await
            .unwrap();
    }

    async fn uris(pool: &deadpool_postgres::Pool, sql: &str, did: &str) -> Vec<String> {
        let client = pool.get().await.unwrap();
        let mut uris: Vec<String> = client
            .query(sql, &[&did])
            .await
            .unwrap()
            .iter()
            .map(|row| row.get(0))
            .collect();
        uris.sort();
        uris
    }

    #[tokio::test]
    async fn an_unlike_and_a_like_again_both_missed_end_with_the_new_like() {
        for skip in [false, true] {
            let did = "did:plc:fillgapsrelike";
            let post = "at://did:plc:fillgapsauthor/app.bsky.feed.post/p";
            let pool = test_pool();
            clean(&pool, did).await;
            // Held: the first like. The repository: only the second one.
            let first = format!("at://{did}/app.bsky.feed.like/3maaaaaaaaaa2");
            let second = format!("at://{did}/app.bsky.feed.like/3mbbbbbbbbbb2");
            index_like(
                &pool,
                &first,
                "bafyfirst",
                post,
                "2026-07-01T00:00:00.000Z",
                skip,
            )
            .await;
            let (car, _, _) = repo_car(did, vec![like("3mbbbbbbbbbb2", post)]).await;
            let mut server = mockito::Server::new_async().await;
            let get_repo = server
                .mock("GET", "/xrpc/com.atproto.sync.getRepo")
                .match_query(mockito::Matcher::UrlEncoded("did".into(), did.into()))
                .with_body(car)
                .create_async()
                .await;

            let ctx = test_ctx(pool.clone(), server.url(), skip);
            let report = fill_one(&ctx, did).await.unwrap();

            get_repo.assert_async().await;
            assert_eq!(
                (
                    report.extra,
                    report.deleted,
                    report.missing,
                    report.written,
                    report.duplicates
                ),
                (1, 1, 1, 1, 0),
                "skip {skip}: {report:?}"
            );
            assert_eq!(
                uris(&pool, "SELECT uri FROM \"like\" WHERE creator = $1", did).await,
                vec![second.clone()],
                "skip {skip}: the account still likes the post"
            );
            let in_record = uris(&pool, "SELECT uri FROM record WHERE did = $1", did).await;
            if skip {
                assert!(in_record.is_empty(), "{in_record:?}");
            } else {
                assert_eq!(in_record, vec![second]);
            }
            clean(&pool, did).await;
        }
    }

    #[tokio::test]
    async fn a_like_held_only_in_record_gets_its_row_back() {
        let did = "did:plc:fillgapsuntyped";
        let lost_post = "at://did:plc:fillgapsauthor/app.bsky.feed.post/lost";
        let twice_post = "at://did:plc:fillgapsauthor/app.bsky.feed.post/twice";
        let pool = test_pool();
        clean(&pool, did).await;
        let (car, _, cids) = repo_car(
            did,
            vec![
                like("3mlostlostlo2", lost_post),
                like("3mfirstfirst2", twice_post),
                like("3msecondseco2", twice_post),
            ],
        )
        .await;
        // The first like of `twice_post` is held as usual. The other two have
        // a record row only: the lost one, and the second like of a post.
        let first = format!("at://{did}/app.bsky.feed.like/3mfirstfirst2");
        index_like(
            &pool,
            &first,
            &cids["app.bsky.feed.like/3mfirstfirst2"],
            twice_post,
            "2026-07-01T00:00:00.000Z",
            false,
        )
        .await;
        let client = pool.get().await.unwrap();
        for rkey in ["3mlostlostlo2", "3msecondseco2"] {
            client
                .execute(
                    "INSERT INTO record (uri, cid, did, json, rev, \"indexedAt\") \
                     VALUES ($1, $2, $3, '{}', '3aaaaaaaaaaa2', '2026-07-01T00:00:00.000Z')",
                    &[
                        &format!("at://{did}/app.bsky.feed.like/{rkey}"),
                        &cids[&format!("app.bsky.feed.like/{rkey}")],
                        &did,
                    ],
                )
                .await
                .unwrap();
        }
        drop(client);
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/xrpc/com.atproto.sync.getRepo")
            .match_query(mockito::Matcher::Any)
            .with_body(car)
            .create_async()
            .await;

        let report = fill_one(&test_ctx(pool.clone(), server.url(), false), did)
            .await
            .unwrap();

        assert_eq!(
            (
                report.missing,
                report.duplicates,
                report.written,
                report.extra
            ),
            (1, 1, 1, 0),
            "{report:?}"
        );
        assert_eq!(
            uris(&pool, "SELECT uri FROM \"like\" WHERE creator = $1", did).await,
            vec![
                first,
                format!("at://{did}/app.bsky.feed.like/3mlostlostlo2")
            ]
        );
        clean(&pool, did).await;
    }

    #[tokio::test]
    async fn with_the_switch_on_likes_are_compared_with_their_table() {
        let did = "did:plc:fillgapstyped";
        let liked = "at://did:plc:fillgapsauthor/app.bsky.feed.post/liked";
        let pool = test_pool();
        clean(&pool, did).await;
        let post_write = RecordCreateOrUpdateOp {
            action: WriteOpAction::Create,
            collection: "app.bsky.feed.post".to_owned(),
            rkey: "3mpostpostpo2".to_owned(),
            record: serde_json::from_value(serde_json::json!({
                "$type": "app.bsky.feed.post",
                "text": "missed",
                "createdAt": "2026-07-01T00:00:00.000Z",
            }))
            .unwrap(),
        };
        // The repository: a held like, a second like of the same post, and a
        // post the AppView missed.
        let (car, _, cids) = repo_car(
            did,
            vec![
                like("3mheldheldhe2", liked),
                like("3mtwicetwice2", liked),
                post_write,
            ],
        )
        .await;
        let held = format!("at://{did}/app.bsky.feed.like/3mheldheldhe2");
        let unliked = format!("at://{did}/app.bsky.feed.like/3mgonegonego2");
        let recent = format!("at://{did}/app.bsky.feed.like/3mnewnewnewn2");
        let later = Utc::now() + chrono::TimeDelta::hours(1);
        let later = later.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
        index_like(
            &pool,
            &held,
            &cids["app.bsky.feed.like/3mheldheldhe2"],
            liked,
            "2026-07-01T00:00:00.000Z",
            true,
        )
        .await;
        index_like(
            &pool,
            &unliked,
            "bafygone",
            "at://did:plc:fillgapsauthor/app.bsky.feed.post/other",
            "2026-07-02T00:00:00.000Z",
            true,
        )
        .await;
        index_like(
            &pool,
            &recent,
            "bafynew",
            "at://did:plc:fillgapsauthor/app.bsky.feed.post/new",
            &later,
            true,
        )
        .await;
        assert!(
            uris(&pool, "SELECT uri FROM record WHERE did = $1", did)
                .await
                .is_empty(),
            "with the switch on, likes have no record row"
        );
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/xrpc/com.atproto.sync.getRepo")
            .match_query(mockito::Matcher::Any)
            .with_body(car)
            .create_async()
            .await;

        let ctx = test_ctx(pool.clone(), server.url(), true);
        let report = fill_one(&ctx, did).await.unwrap();

        assert_eq!(report.in_repo, 3);
        assert_eq!(report.missing, 1, "only the post: {report:?}");
        assert_eq!(
            report.needed_by_collection.keys().collect::<Vec<_>>(),
            vec!["app.bsky.feed.post"]
        );
        assert_eq!(report.duplicates, 1, "the second like of the same post");
        assert_eq!(
            (report.extra, report.deleted),
            (1, 1),
            "the like no longer in the repository"
        );
        assert_eq!(
            report.kept, 1,
            "the like indexed after the download started"
        );
        assert_eq!(report.written, 1);
        assert_eq!(
            uris(&pool, "SELECT uri FROM \"like\" WHERE creator = $1", did).await,
            vec![held, recent]
        );
        assert_eq!(
            uris(&pool, "SELECT uri FROM record WHERE did = $1", did).await,
            vec![format!("at://{did}/app.bsky.feed.post/3mpostpostpo2")]
        );
        clean(&pool, did).await;
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
