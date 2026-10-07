//! Replays ranges of relay sequence numbers, or windows of time, straight into
//! the indexer, to fill gaps the live subscription missed. It never touches the live cursor
//! or the queues, and every write is rev-guarded, so it can run beside the
//! live instance.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use clap::Parser;
use color_eyre::Result;
use color_eyre::eyre::eyre;
use deadpool_postgres::Pool;
use futures::StreamExt;
use tokio::sync::Semaphore;
use tokio_tungstenite::tungstenite::Message;

use rsky_wintermute::indexer::IndexerManager;
use rsky_wintermute::ingester::{IngesterManager, ParseResult, subscribe_url};
use rsky_wintermute::reconcile::{Provenance, Source, current_generation};
use rsky_wintermute::types::{FirehoseEvent, WriteAction};

/// A gap to replay: every event with `start < seq < end`. `start` is the
/// last sequence number that was received before the gap and `end` the first
/// one received after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SeqRange {
    start: i64,
    end: i64,
}

impl SeqRange {
    fn new(start: i64, end: i64) -> Result<Self, String> {
        if start < 0 {
            return Err(format!("start ({start}) must not be negative"));
        }
        if start >= end {
            return Err(format!("start ({start}) must be less than end ({end})"));
        }
        Ok(Self { start, end })
    }
}

impl std::fmt::Display for SeqRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.start, self.end)
    }
}

fn parse_range(s: &str) -> Result<SeqRange, String> {
    let (start, end) = s
        .split_once(':')
        .ok_or_else(|| format!("expected START:END, got '{s}'"))?;
    let start = start
        .trim()
        .parse()
        .map_err(|e| format!("invalid start '{start}': {e}"))?;
    let end = end
        .trim()
        .parse()
        .map_err(|e| format!("invalid end '{end}': {e}"))?;
    SeqRange::new(start, end)
}

/// A gap to replay by time, for a relay whose sequence numbers for it are
/// not known: every event the relay sequenced between the two instants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TimeWindow {
    since: DateTime<Utc>,
    until: DateTime<Utc>,
}

impl std::fmt::Display for TimeWindow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}",
            self.since.format("%Y-%m-%dT%H:%M:%SZ"),
            self.until.format("%Y-%m-%dT%H:%M:%SZ")
        )
    }
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

/// What to replay, as given on the command line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    Seq(SeqRange),
    Window(TimeWindow),
}

impl std::fmt::Display for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Seq(range) => write!(f, "range {range}"),
            Self::Window(window) => write!(f, "window {window}"),
        }
    }
}

/// The time of an event that was replayed, in the form `indexedAt` is
/// stored in. Never later than `now`: a PDS clock can run ahead.
fn replayed_indexed_at(event_time: &str, now: DateTime<Utc>) -> Option<String> {
    let event_at = DateTime::parse_from_rfc3339(event_time)
        .ok()?
        .with_timezone(&Utc);
    Some(
        event_at
            .min(now)
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string(),
    )
}

/// Events sampled to date a cursor. The median is taken because the time on
/// an event comes from its PDS, and a few clocks are far off.
const PROBE_EVENTS: usize = 25;

/// Cursor resolution stops once the bounds are this close; the events in
/// between are replayed, which costs nothing but their writes.
const RESOLVE_PRECISION: i64 = 500;

/// What the relay delivers from a cursor.
#[derive(Debug, Clone, Copy)]
struct Probe {
    first_seq: i64,
    /// The median time of the first events.
    time: DateTime<Utc>,
}

fn median_time(times: &mut [DateTime<Utc>]) -> Option<DateTime<Utc>> {
    times.sort_unstable();
    times.get(times.len() / 2).copied()
}

/// Finds the cursor that splits the relay's log at `target`: events up to it
/// are dated before `target`. `probe` dates the events that follow a cursor.
/// `lo` must be dated before `target` and `hi` at or after it.
async fn bisect_cursor<F, Fut>(
    mut lo: i64,
    mut hi: i64,
    target: DateTime<Utc>,
    probe: F,
) -> Result<(i64, i64)>
where
    F: Fn(i64) -> Fut,
    Fut: std::future::Future<Output = Result<Probe>>,
{
    while hi - lo > RESOLVE_PRECISION {
        let mid = lo + (hi - lo) / 2;
        if probe(mid).await?.time < target {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    Ok((lo, hi))
}

#[derive(Debug, Parser)]
#[command(name = "firehose_catchup")]
#[command(about = "Replay ranges of firehose events to fill indexing gaps")]
struct Args {
    /// A gap to replay as START:END, both exclusive: the last sequence number
    /// received before the gap and the first one received after it. Repeat
    /// the flag or separate ranges with commas; they run in the order given.
    #[arg(long = "range", value_parser = parse_range, value_delimiter = ',')]
    ranges: Vec<SeqRange>,

    /// A gap to replay as SINCE/UNTIL, two RFC 3339 times, for a relay whose
    /// sequence numbers for the gap are not known. The cursors are found by
    /// probing --relay-host. Repeatable, comma-separated.
    #[arg(long = "window", value_parser = parse_window, value_delimiter = ',')]
    windows: Vec<TimeWindow>,

    /// Seconds added on both sides of every --window. Relays order events
    /// differently and event times are their PDS's, so the same events sit
    /// at slightly different times on another relay.
    #[arg(long, default_value = "300")]
    window_padding_secs: u32,

    /// Print the sequence range of every --window and exit, without
    /// connecting to the database or replaying anything.
    #[arg(long)]
    resolve_only: bool,

    /// Starting cursor of a single range (same as --range START:END)
    #[arg(long, requires = "end_cursor")]
    start_cursor: Option<i64>,

    /// Ending cursor of a single range (same as --range START:END)
    #[arg(long, requires = "start_cursor")]
    end_cursor: Option<i64>,

    /// PostgreSQL connection URL
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    /// Firehose relay host. Sequence numbers belong to one relay: the ranges
    /// must come from this host.
    #[arg(long, default_value = "bsky.network")]
    relay_host: String,

    /// Maximum concurrent indexing tasks
    #[arg(long, default_value = "200")]
    concurrency: usize,

    /// Database pool size
    #[arg(long, default_value = "40")]
    pool_size: usize,

    /// Replay what the relay still holds when a range starts before its
    /// replay window, instead of failing the range.
    #[arg(long)]
    allow_partial: bool,

    /// Reconnects allowed in a row without receiving a new event
    #[arg(long, default_value = "5")]
    max_reconnects: u32,

    /// Seconds without a message before the connection is considered dead
    #[arg(long, default_value = "30")]
    idle_timeout_secs: u64,
}

impl Args {
    /// Everything to replay: the ranges, then the windows with their padding.
    fn targets(&self) -> Result<Vec<Target>> {
        let mut targets: Vec<Target> = self.ranges.iter().copied().map(Target::Seq).collect();
        if let (Some(start), Some(end)) = (self.start_cursor, self.end_cursor) {
            targets.push(Target::Seq(
                SeqRange::new(start, end).map_err(|e| eyre!(e))?,
            ));
        }
        let padding = chrono::Duration::seconds(i64::from(self.window_padding_secs));
        targets.extend(self.windows.iter().map(|window| {
            Target::Window(TimeWindow {
                since: window.since - padding,
                until: window.until + padding,
            })
        }));
        if targets.is_empty() {
            return Err(eyre!(
                "nothing to replay: pass --range START:END, --window SINCE/UNTIL \
                 or --start-cursor/--end-cursor"
            ));
        }
        Ok(targets)
    }
}

#[derive(Debug, Default)]
struct Counters {
    /// Record writes and identity/account updates that were applied.
    processed: AtomicU64,
    /// Work that could not be applied; any makes the run fail.
    failed: AtomicU64,
}

/// How one connection ended.
enum StreamEnd {
    /// An event at or past the end of the range arrived.
    ReachedEnd,
    /// The connection dropped or went quiet; worth resuming from `last_seq`.
    Disconnected(String),
}

/// What a range replayed.
#[derive(Debug)]
struct RangeReport {
    range: SeqRange,
    /// The first sequence number the relay delivered.
    first_seq: Option<i64>,
    last_seq: i64,
    events: u64,
    /// The relay no longer held the start of the range.
    truncated: bool,
}

struct Replay {
    pool: Arc<Pool>,
    semaphore: Arc<Semaphore>,
    counters: Arc<Counters>,
    relay_host: String,
    concurrency: usize,
    allow_partial: bool,
    max_reconnects: u32,
    idle_timeout: Duration,
}

impl Replay {
    /// Dates the events the relay delivers from `cursor`; `None` is the
    /// live stream.
    async fn probe(&self, cursor: Option<i64>) -> Result<Probe> {
        let mut url = subscribe_url(&self.relay_host)
            .map_err(|e| eyre!("invalid relay host '{}': {e}", self.relay_host))?;
        if let Some(cursor) = cursor {
            url.query_pairs_mut()
                .append_pair("cursor", &cursor.to_string());
        }
        let (ws_stream, _) = tokio_tungstenite::connect_async(url.as_str()).await?;
        let (_write, mut read) = ws_stream.split();

        let mut first_seq = None;
        let mut times = Vec::with_capacity(PROBE_EVENTS);
        while times.len() < PROBE_EVENTS {
            let msg = tokio::time::timeout(self.idle_timeout, read.next())
                .await
                .map_err(|_| eyre!("no event from cursor {cursor:?} in {:?}", self.idle_timeout))?
                .ok_or_else(|| eyre!("stream ended while probing cursor {cursor:?}"))??;
            let Message::Binary(data) = msg else {
                continue;
            };
            match IngesterManager::parse_message(&data) {
                Ok(ParseResult::Event(event)) => {
                    first_seq.get_or_insert(event.seq);
                    if let Ok(time) = DateTime::parse_from_rfc3339(&event.time) {
                        times.push(time.with_timezone(&Utc));
                    }
                }
                Ok(ParseResult::FutureCursor) => {
                    return Err(eyre!("cursor {cursor:?} is ahead of {}", self.relay_host));
                }
                // an outdated cursor is answered from the oldest event held
                Ok(_) | Err(_) => {}
            }
        }
        let first_seq = first_seq.ok_or_else(|| eyre!("no event from cursor {cursor:?}"))?;
        let time = median_time(&mut times).ok_or_else(|| eyre!("no dated event"))?;
        Ok(Probe { first_seq, time })
    }

    /// Finds the sequence numbers of this relay that bound `window`.
    async fn resolve_window(&self, window: TimeWindow) -> Result<SeqRange> {
        let oldest = self.probe(Some(0)).await?;
        let live = self.probe(None).await?;
        tracing::info!(
            "window {window}: {} holds seq {} ({}) to {} ({})",
            self.relay_host,
            oldest.first_seq,
            oldest.time.format("%Y-%m-%dT%H:%M:%SZ"),
            live.first_seq,
            live.time.format("%Y-%m-%dT%H:%M:%SZ")
        );
        if window.until <= oldest.time {
            return Err(eyre!(
                "window {window} ended before the oldest event {} holds ({})",
                self.relay_host,
                oldest.time
            ));
        }
        if window.since >= live.time {
            return Err(eyre!("window {window} has not started yet"));
        }
        let floor = oldest.first_seq - 1;
        let start = if window.since <= oldest.time {
            if !self.allow_partial {
                return Err(eyre!(
                    "window {window} starts before the oldest event {} holds ({}); \
                     pass --allow-partial to replay what is left",
                    self.relay_host,
                    oldest.time
                ));
            }
            tracing::warn!(
                "window {window}: starts before the relay's replay window, \
                 replaying from the oldest event it holds"
            );
            floor
        } else {
            bisect_cursor(floor, live.first_seq, window.since, |cursor| {
                self.probe(Some(cursor))
            })
            .await?
            .0
        };
        let end = if window.until >= live.time {
            live.first_seq
        } else {
            bisect_cursor(start, live.first_seq, window.until, |cursor| {
                self.probe(Some(cursor))
            })
            .await?
            .1
        };
        SeqRange::new(start, end).map_err(|e| eyre!("window {window}: {e}"))
    }

    async fn run_range(&self, range: SeqRange) -> Result<RangeReport> {
        let mut report = RangeReport {
            range,
            first_seq: None,
            last_seq: range.start,
            events: 0,
            truncated: false,
        };
        let mut stalled_attempts = 0u32;
        loop {
            let resumed_from = report.last_seq;
            let outcome = self.stream(&mut report).await;
            // let every spawned write land before judging or reconnecting
            drop(
                self.semaphore
                    .acquire_many(u32::try_from(self.concurrency)?)
                    .await?,
            );
            let reason = match outcome? {
                StreamEnd::ReachedEnd => return Ok(report),
                StreamEnd::Disconnected(reason) => reason,
            };
            if report.last_seq > resumed_from {
                stalled_attempts = 0;
            } else {
                stalled_attempts += 1;
            }
            if stalled_attempts > self.max_reconnects {
                return Err(eyre!(
                    "range {range}: gave up at seq {} after {} reconnects without progress: {reason}",
                    report.last_seq,
                    self.max_reconnects
                ));
            }
            let delay = Duration::from_secs(u64::from(stalled_attempts.min(5)) * 2);
            tracing::warn!(
                "range {range}: {reason}; resuming from seq {} in {delay:?}",
                report.last_seq
            );
            tokio::time::sleep(delay).await;
        }
    }

    /// Streams from `report.last_seq` until the end of the range or the
    /// connection ends. Errors are final for the range.
    async fn stream(&self, report: &mut RangeReport) -> Result<StreamEnd> {
        let range = report.range;
        let mut url = subscribe_url(&self.relay_host)
            .map_err(|e| eyre!("invalid relay host '{}': {e}", self.relay_host))?;
        url.query_pairs_mut()
            .append_pair("cursor", &report.last_seq.to_string());
        tracing::info!("connecting to {url}");

        let (ws_stream, _) = match tokio_tungstenite::connect_async(url.as_str()).await {
            Ok(connected) => connected,
            Err(e) => return Ok(StreamEnd::Disconnected(format!("connect failed: {e}"))),
        };
        let (_write, mut read) = ws_stream.split();

        let mut last_log_time = Instant::now();
        let mut last_log_events = report.events;

        loop {
            let msg = match tokio::time::timeout(self.idle_timeout, read.next()).await {
                Ok(Some(Ok(msg))) => msg,
                Ok(Some(Err(e))) => {
                    return Ok(StreamEnd::Disconnected(format!("websocket error: {e}")));
                }
                Ok(None) => return Ok(StreamEnd::Disconnected("stream ended".to_owned())),
                Err(_) => {
                    return Ok(StreamEnd::Disconnected(format!(
                        "no message in {:?}",
                        self.idle_timeout
                    )));
                }
            };
            let Message::Binary(data) = msg else {
                continue;
            };

            let event = match IngesterManager::parse_message(&data) {
                Ok(ParseResult::Event(event)) => event,
                Ok(ParseResult::OutdatedCursor) => {
                    if !self.allow_partial {
                        return Err(eyre!(
                            "range {range}: cursor {} is older than the replay window of {}; \
                             pass --allow-partial to replay what is left",
                            report.last_seq,
                            self.relay_host
                        ));
                    }
                    tracing::warn!(
                        "range {range}: cursor {} is older than the relay's replay window, \
                         replaying from the oldest event it holds",
                        report.last_seq
                    );
                    report.truncated = true;
                    continue;
                }
                Ok(ParseResult::FutureCursor) => {
                    return Err(eyre!(
                        "range {range}: cursor {} is ahead of {}; these sequence numbers \
                         are not this relay's",
                        report.last_seq,
                        self.relay_host
                    ));
                }
                Ok(ParseResult::Skip) => continue,
                Err(e) => {
                    // the frame's seq is unknown, so it cannot be skipped by name
                    tracing::warn!(
                        "range {range}: unparseable frame after seq {}: {e}",
                        report.last_seq
                    );
                    self.counters.failed.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };

            let seq = event.seq;
            if seq >= range.end {
                tracing::info!("range {range}: reached the end at seq {seq}");
                return Ok(StreamEnd::ReachedEnd);
            }
            if seq <= report.last_seq {
                continue;
            }
            if report.first_seq.is_none() {
                report.first_seq = Some(seq);
            }

            self.dispatch(event).await?;
            report.last_seq = seq;
            report.events += 1;

            if last_log_time.elapsed() > Duration::from_secs(10) {
                #[allow(clippy::cast_precision_loss)]
                let rate = (report.events - last_log_events) as f64
                    / last_log_time.elapsed().as_secs_f64();
                #[allow(clippy::cast_precision_loss)]
                let pct = (seq - range.start) as f64 / (range.end - range.start) as f64 * 100.0;
                tracing::info!(
                    "range {range}: {pct:.1}% | seq={seq} | events={} | rate={rate:.0}/s | failed={} | remaining={}",
                    report.events,
                    self.counters.failed.load(Ordering::Relaxed),
                    range.end - seq
                );
                last_log_events = report.events;
                last_log_time = Instant::now();
            }
        }
    }

    /// Hands one event to the writers the live instance uses for its kind.
    async fn dispatch(&self, event: FirehoseEvent) -> Result<()> {
        match event.kind.as_str() {
            // The handle a replayed event carries may since have changed, so
            // none is passed and the current one is resolved.
            "identity" | "sync" => {
                let permit = Arc::clone(&self.semaphore).acquire_owned().await?;
                let pool = Arc::clone(&self.pool);
                let counters = Arc::clone(&self.counters);
                tokio::spawn(async move {
                    match IngesterManager::process_identity_event(
                        &pool,
                        &event.did,
                        &event.time,
                        None,
                    )
                    .await
                    {
                        Ok(()) => counters.processed.fetch_add(1, Ordering::Relaxed),
                        Err(e) => {
                            tracing::warn!(
                                "{} event seq={} failed for {}: {e}",
                                event.kind,
                                event.seq,
                                event.did
                            );
                            counters.failed.fetch_add(1, Ordering::Relaxed)
                        }
                    };
                    drop(permit);
                });
            }
            // The update is guarded by the event's time, so an account event
            // older than the actor's current status changes nothing.
            "account" => {
                let Some(account) = event.account else {
                    return Ok(());
                };
                let permit = Arc::clone(&self.semaphore).acquire_owned().await?;
                let pool = Arc::clone(&self.pool);
                let counters = Arc::clone(&self.counters);
                tokio::spawn(async move {
                    if !account.active
                        && IngesterManager::pds_says_active(&event.did).await == Some(true)
                    {
                        tracing::debug!(
                            "skipped account event seq={} for {}: its PDS reports active",
                            event.seq,
                            event.did
                        );
                        drop(permit);
                        return;
                    }
                    match IngesterManager::process_account_event(
                        &pool,
                        &event.did,
                        &event.time,
                        account.active,
                        account.status.as_deref(),
                    )
                    .await
                    {
                        Ok(()) => counters.processed.fetch_add(1, Ordering::Relaxed),
                        Err(e) => {
                            tracing::warn!(
                                "account event seq={} failed for {}: {e}",
                                event.seq,
                                event.did
                            );
                            counters.failed.fetch_add(1, Ordering::Relaxed)
                        }
                    };
                    drop(permit);
                });
            }
            "commit" => self.dispatch_commit(&event).await?,
            _ => {}
        }
        Ok(())
    }

    async fn dispatch_commit(&self, event: &FirehoseEvent) -> Result<()> {
        let jobs = match IngesterManager::parse_event_to_jobs(event).await {
            Ok(jobs) => jobs,
            Err(e) => {
                tracing::warn!("failed to parse commit seq={}: {e}", event.seq);
                self.counters.failed.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
        };
        // An actor that has been reconciled only admits work stamped with its
        // current generation.
        let generation = match current_generation(&self.pool, &event.did).await {
            Ok(generation) => generation,
            Err(e) => {
                tracing::warn!(
                    "generation lookup failed for {} at seq={}: {e}",
                    event.did,
                    event.seq
                );
                self.counters.failed.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
        };
        for mut job in jobs {
            // The commit job records the actor's latest commit, which a
            // replayed one is not: applying it would move that pointer back.
            if matches!(job.action, WriteAction::Commit) {
                continue;
            }
            // Stamped with the time the event was sequenced, not the time of
            // the replay, so a post lost then is not presented as new now.
            if let Some(indexed_at) = replayed_indexed_at(&event.time, Utc::now()) {
                job.indexed_at = indexed_at;
            }
            job.provenance = Some(Provenance {
                generation,
                source: Source::Firehose { seq: event.seq },
            });
            let permit = Arc::clone(&self.semaphore).acquire_owned().await?;
            let pool = Arc::clone(&self.pool);
            let counters = Arc::clone(&self.counters);
            let seq = event.seq;
            tokio::spawn(async move {
                match IndexerManager::process_job(
                    &pool,
                    &job,
                    *rsky_wintermute::config::RECORD_SKIP_BOILERPLATE,
                )
                .await
                {
                    Ok(()) => counters.processed.fetch_add(1, Ordering::Relaxed),
                    Err(e) => {
                        tracing::warn!("indexing failed for {} at seq={seq}: {e}", job.uri);
                        counters.failed.fetch_add(1, Ordering::Relaxed)
                    }
                };
                drop(permit);
            });
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Install rustls crypto provider before any TLS operations
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let targets = args.targets()?;
    if args.concurrency == 0 {
        return Err(eyre!("--concurrency must be at least 1"));
    }

    tracing::info!(
        "firehose catchup: replaying {} range(s) from {}",
        targets.len(),
        args.relay_host
    );

    // Setup database pool
    let pool = Arc::new(rsky_wintermute::config::create_pg_pool(
        &args.database_url,
        deadpool_postgres::PoolConfig {
            max_size: args.pool_size,
            ..Default::default()
        },
    )?);

    // Test DB connection
    if !args.resolve_only {
        let test_client = pool.get().await?;
        drop(test_client);
        tracing::info!("database connection OK, pool_size={}", args.pool_size);
    }

    let replay = Replay {
        pool,
        semaphore: Arc::new(Semaphore::new(args.concurrency)),
        counters: Arc::new(Counters::default()),
        relay_host: args.relay_host.clone(),
        concurrency: args.concurrency,
        allow_partial: args.allow_partial,
        max_reconnects: args.max_reconnects,
        idle_timeout: Duration::from_secs(args.idle_timeout_secs),
    };

    let start_time = Instant::now();
    let mut incomplete = 0usize;
    for target in targets {
        // A window's cursors are found right before it is replayed, and a
        // range that fails does not stop the ones after it: the relay's
        // replay window keeps moving while this runs.
        let range = match target {
            Target::Seq(range) => range,
            Target::Window(window) => match replay.resolve_window(window).await {
                Ok(range) => {
                    tracing::info!("window {window}: is range {range}");
                    if args.resolve_only {
                        println!("--range {range}");
                        continue;
                    }
                    range
                }
                Err(e) => {
                    incomplete += 1;
                    tracing::error!("{target}: FAILED: {e}");
                    continue;
                }
            },
        };
        if args.resolve_only {
            continue;
        }
        match replay.run_range(range).await {
            Ok(report) => {
                // A source that holds the gap delivers something inside it;
                // one with the same gap jumps straight past its end.
                let empty = report.events == 0;
                if report.truncated || empty {
                    incomplete += 1;
                }
                tracing::info!(
                    "range {range}: {} | events={} first_seq={} last_seq={}",
                    if empty {
                        "EMPTY, the relay delivered nothing inside it"
                    } else if report.truncated {
                        "PARTIAL, the relay no longer held its start"
                    } else {
                        "complete"
                    },
                    report.events,
                    report
                        .first_seq
                        .map_or_else(|| "none".to_owned(), |seq| seq.to_string()),
                    report.last_seq
                );
            }
            Err(e) => {
                incomplete += 1;
                tracing::error!("range {range}: FAILED: {e}");
            }
        }
    }

    let processed = replay.counters.processed.load(Ordering::Relaxed);
    let failed = replay.counters.failed.load(Ordering::Relaxed);
    tracing::info!(
        "catchup finished: processed={processed} failed={failed} incomplete_ranges={incomplete} elapsed={:.1}s",
        start_time.elapsed().as_secs_f64()
    );

    if incomplete > 0 || failed > 0 {
        return Err(eyre!(
            "{incomplete} range(s) not fully replayed, {failed} write(s) failed"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_range() {
        assert_eq!(
            parse_range("100:200"),
            Ok(SeqRange {
                start: 100,
                end: 200
            })
        );
    }

    #[test]
    fn rejects_malformed_ranges() {
        assert!(parse_range("100").is_err());
        assert!(parse_range("a:200").is_err());
        assert!(parse_range("200:100").is_err());
        assert!(parse_range("100:100").is_err());
        assert!(parse_range("-1:100").is_err());
    }

    #[test]
    fn parses_a_window() {
        let window = parse_window("2026-10-05T15:40:00Z/2026-10-05T18:00:00+02:00").unwrap();
        assert_eq!(
            window.to_string(),
            "2026-10-05T15:40:00Z/2026-10-05T16:00:00Z"
        );
        assert!(parse_window("2026-10-05T15:40:00Z").is_err());
        assert!(parse_window("2026-10-05T16:00:00Z/2026-10-05T15:40:00Z").is_err());
        assert!(parse_window("yesterday/today").is_err());
    }

    #[test]
    fn windows_are_padded_and_follow_the_ranges() {
        let args = Args::parse_from([
            "firehose_catchup",
            "--database-url",
            "postgres://x",
            "--window",
            "2026-10-05T15:40:00Z/2026-10-05T16:00:00Z",
            "--window-padding-secs",
            "60",
            "--range",
            "1:5",
        ]);
        let targets: Vec<String> = args
            .targets()
            .unwrap()
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            targets,
            [
                "range 1:5",
                "window 2026-10-05T15:39:00Z/2026-10-05T16:01:00Z"
            ]
        );
    }

    #[test]
    fn replayed_events_keep_their_time_unless_it_is_in_the_future() {
        let now = DateTime::parse_from_rfc3339("2026-10-05T20:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            replayed_indexed_at("2026-10-03T16:00:00.123456Z", now).as_deref(),
            Some("2026-10-03T16:00:00.123Z")
        );
        assert_eq!(
            replayed_indexed_at("2027-01-01T00:00:00Z", now).as_deref(),
            Some("2026-10-05T20:00:00.000Z")
        );
        assert_eq!(replayed_indexed_at("not a time", now), None);
    }

    #[test]
    fn median_ignores_outlying_clocks() {
        let at = |s: &str| DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc);
        let mut times = vec![
            at("2020-01-01T00:00:00Z"),
            at("2026-10-05T15:00:01Z"),
            at("2026-10-05T15:00:00Z"),
            at("2026-10-05T15:00:02Z"),
            at("2030-01-01T00:00:00Z"),
        ];
        assert_eq!(median_time(&mut times), Some(at("2026-10-05T15:00:01Z")));
        assert_eq!(median_time(&mut []), None);
    }

    /// A relay that sequences one event a second from `origin`.
    #[tokio::test]
    async fn bisection_brackets_the_target_time() {
        let origin = DateTime::parse_from_rfc3339("2026-10-05T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let probe = |cursor: i64| async move {
            Ok(Probe {
                first_seq: cursor + 1,
                time: origin + chrono::Duration::seconds(cursor + 1),
            })
        };
        let target = origin + chrono::Duration::seconds(40_000);
        let (lo, hi) = bisect_cursor(0, 86_400, target, probe).await.unwrap();
        assert!(lo < 40_000 && 40_000 <= hi + 1, "{lo}..{hi}");
        assert!(hi - lo <= RESOLVE_PRECISION);
    }

    #[test]
    fn ranges_accumulate_from_both_forms() {
        let args = Args::parse_from([
            "firehose_catchup",
            "--database-url",
            "postgres://x",
            "--range",
            "1:5,10:20",
            "--range",
            "30:40",
            "--start-cursor",
            "50",
            "--end-cursor",
            "60",
        ]);
        let ranges: Vec<String> = args
            .targets()
            .unwrap()
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            ranges,
            ["range 1:5", "range 10:20", "range 30:40", "range 50:60"]
        );
    }

    #[test]
    fn requires_at_least_one_range() {
        let args = Args::parse_from(["firehose_catchup", "--database-url", "postgres://x"]);
        assert!(args.targets().is_err());
    }
}
