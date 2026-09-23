//! Durable on-disk queues and small key-value state for wintermute.
//!
//! Every queue here is written once and read once in order, so each one is
//! a segmented append-only log (see [`crate::queue_log`]) rather than a
//! sorted map: no tombstones, no compaction, dequeue cost independent of how
//! much has been drained. The public API is unchanged from the Fjall/LMDB
//! implementation it replaces, including the partitioned backfill dequeue.
//!
//! Layout under the storage directory:
//!
//! ```text
//! firehose_live/                 FIFO log of live IndexJobs
//! label_live/                    FIFO log of LabelEvents
//! repo_backfill/{immediate,priority,normal}/
//!                                three FIFO logs drained in that order
//! repo_backfill/cancelled.cbor   DIDs removed by operators, with counts
//! firehose_backfill/priority/    FIFO log all indexer workers drain first
//! firehose_backfill/shard_NNN/   240 FIFO logs, sliced across workers
//! cursors.cbor                   name -> i64 map, rewritten atomically
//! firehose_events/<seq>.cbor     one file per event (no production caller)
//! ```

use crate::config::{QUEUE_LOG_FSYNC_MS, QUEUE_LOG_SEGMENT_BYTES};
use crate::queue_log::SegmentedLog;
use crate::types::{BackfillJob, FirehoseEvent, IndexJob, LabelEvent, WintermuteError};
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

/// Normal-priority `firehose_backfill` jobs are spread over this many shard
/// logs; a worker owns a contiguous slice of them. Matches the 240 random
/// key prefixes (0x10..=0xff) of the previous key-space partitioning, so the
/// worker-to-slice arithmetic is identical.
const BACKFILL_SHARDS: usize = 240;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn open_log(dir: PathBuf) -> Result<SegmentedLog, WintermuteError> {
    Ok(SegmentedLog::open(
        dir,
        QUEUE_LOG_SEGMENT_BYTES,
        Duration::from_millis(QUEUE_LOG_FSYNC_MS),
    )?)
}

fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, WintermuteError> {
    let mut out = Vec::new();
    ciborium::into_writer(value, &mut out)
        .map_err(|e| WintermuteError::Serialization(format!("failed to serialize: {e}")))?;
    Ok(out)
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, WintermuteError> {
    ciborium::from_reader(bytes)
        .map_err(|e| WintermuteError::Serialization(format!("failed to deserialize: {e}")))
}

/// Writes `bytes` to `path` through a temporary file and a rename, so a
/// crash leaves either the old or the new contents, never a mix.
fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)
}

/// Small name -> value map persisted as one CBOR file.
struct CursorFile {
    path: PathBuf,
    map: Mutex<HashMap<String, i64>>,
}

impl CursorFile {
    fn open(path: PathBuf) -> Result<Self, WintermuteError> {
        let map = match std::fs::read(&path) {
            Ok(bytes) => decode(&bytes)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => HashMap::new(),
            Err(e) => return Err(e.into()),
        };
        Ok(Self {
            path,
            map: Mutex::new(map),
        })
    }

    fn get(&self, name: &str) -> Option<i64> {
        lock(&self.map).get(name).copied()
    }

    #[allow(
        clippy::significant_drop_tightening,
        reason = "held across the write so concurrent updates reach disk in memory order"
    )]
    fn update(&self, f: impl FnOnce(&mut HashMap<String, i64>)) -> Result<(), WintermuteError> {
        let mut map = lock(&self.map);
        f(&mut map);
        let bytes = encode(&*map)?;
        write_atomic(&self.path, &bytes)?;
        Ok(())
    }
}

type BackfillBatch = Vec<(Vec<u8>, BackfillJob)>;

/// The `repo_backfill` queue: three priority levels, each its own log, plus
/// a map of DIDs an operator asked to remove. A log cannot delete from the
/// middle, so removal is recorded and applied when the job reaches the head.
struct RepoBackfillQueue {
    immediate: SegmentedLog,
    priority: SegmentedLog,
    normal: SegmentedLog,
    cancelled_path: PathBuf,
    cancelled: Mutex<HashMap<String, usize>>,
}

impl RepoBackfillQueue {
    fn open(dir: &Path) -> Result<Self, WintermuteError> {
        let cancelled_path = dir.join("cancelled.cbor");
        let cancelled = match std::fs::read(&cancelled_path) {
            Ok(bytes) => decode(&bytes)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => HashMap::new(),
            Err(e) => return Err(e.into()),
        };
        Ok(Self {
            immediate: open_log(dir.join("immediate"))?,
            priority: open_log(dir.join("priority"))?,
            normal: open_log(dir.join("normal"))?,
            cancelled_path,
            cancelled: Mutex::new(cancelled),
        })
    }

    const fn logs(&self) -> [&SegmentedLog; 3] {
        [&self.immediate, &self.priority, &self.normal]
    }

    fn len(&self) -> usize {
        let queued: usize = self.logs().iter().map(|l| l.len()).sum();
        let cancelled: usize = lock(&self.cancelled).values().sum();
        queued.saturating_sub(cancelled)
    }

    fn persist_cancelled(&self, map: &HashMap<String, usize>) -> Result<(), WintermuteError> {
        write_atomic(&self.cancelled_path, &encode(map)?)?;
        Ok(())
    }

    /// Consumes up to `count` jobs across the three levels, dropping any
    /// that an operator cancelled. Returns the jobs and how many were
    /// dropped (so the caller can settle the queue gauge).
    fn dequeue(&self, count: usize) -> Result<(BackfillBatch, usize), WintermuteError> {
        let mut out = Vec::with_capacity(count);
        let mut dropped = 0usize;
        for (level, log) in self.logs().into_iter().enumerate() {
            while out.len() < count {
                let batch = log.read_batch(count - out.len())?;
                if batch.is_empty() {
                    break;
                }
                let mut cancelled = lock(&self.cancelled);
                let mut changed = false;
                for (key, bytes) in batch {
                    let job: BackfillJob = match decode(&bytes) {
                        Ok(job) => job,
                        Err(e) => {
                            tracing::error!("dropping undeserializable repo_backfill entry: {e}");
                            dropped += 1;
                            continue;
                        }
                    };
                    if let Some(remaining) = cancelled.get_mut(&job.did) {
                        *remaining -= 1;
                        if *remaining == 0 {
                            cancelled.remove(&job.did);
                        }
                        changed = true;
                        dropped += 1;
                        continue;
                    }
                    let mut full_key = Vec::with_capacity(1 + key.len());
                    #[allow(clippy::cast_possible_truncation)]
                    full_key.push(level as u8);
                    full_key.extend_from_slice(&key);
                    out.push((full_key, job));
                }
                if changed {
                    self.persist_cancelled(&cancelled)?;
                }
                drop(cancelled);
            }
        }
        Ok((out, dropped))
    }

    /// Reads up to `limit` jobs in dequeue order without consuming them.
    fn peek(&self, limit: usize) -> Result<BackfillBatch, WintermuteError> {
        let mut out = Vec::with_capacity(limit.min(4096));
        let mut cancelled = lock(&self.cancelled).clone();
        for (level, log) in self.logs().into_iter().enumerate() {
            if out.len() >= limit {
                break;
            }
            // Cancelled jobs are still physically queued, so over-read to
            // fill the limit after filtering them out.
            let extra: usize = cancelled.values().sum();
            for (key, bytes) in log.peek(limit - out.len() + extra)? {
                if out.len() >= limit {
                    break;
                }
                let job: BackfillJob = match decode(&bytes) {
                    Ok(job) => job,
                    Err(_) => continue,
                };
                if let Some(remaining) = cancelled.get_mut(&job.did) {
                    *remaining -= 1;
                    if *remaining == 0 {
                        cancelled.remove(&job.did);
                    }
                    continue;
                }
                let mut full_key = Vec::with_capacity(1 + key.len());
                #[allow(clippy::cast_possible_truncation)]
                full_key.push(level as u8);
                full_key.extend_from_slice(&key);
                out.push((full_key, job));
            }
        }
        Ok(out)
    }

    /// Marks every queued job for `did` as removed. Returns how many.
    fn remove_by_did(&self, did: &str) -> Result<usize, WintermuteError> {
        let mut found = 0usize;
        for log in self.logs() {
            log.for_each_unread(|bytes| {
                if decode::<BackfillJob>(bytes).is_ok_and(|job| job.did == did) {
                    found += 1;
                }
            })?;
        }
        let mut cancelled = lock(&self.cancelled);
        let already = cancelled.get(did).copied().unwrap_or(0);
        let newly = found.saturating_sub(already);
        if newly > 0 {
            cancelled.insert(did.to_owned(), found);
            self.persist_cancelled(&cancelled)?;
        }
        drop(cancelled);
        Ok(newly)
    }

    /// Drops every queued job. Returns how many were live (not cancelled).
    fn clear(&self) -> Result<usize, WintermuteError> {
        let live = self.len();
        for log in self.logs() {
            while !log.read_batch(4096)?.is_empty() {}
        }
        let mut cancelled = lock(&self.cancelled);
        cancelled.clear();
        self.persist_cancelled(&cancelled)?;
        drop(cancelled);
        Ok(live)
    }
}

pub struct Storage {
    firehose_events_dir: PathBuf,
    repo_backfill: RepoBackfillQueue,
    firehose_live: SegmentedLog,
    label_live: SegmentedLog,
    firehose_backfill_priority: SegmentedLog,
    firehose_backfill_shards: Vec<SegmentedLog>,
    cursors: CursorFile,
    live_notify: tokio::sync::Notify,
}

impl Storage {
    pub fn new(db_path: Option<PathBuf>) -> Result<Self, WintermuteError> {
        let path = db_path.unwrap_or_else(|| "backfill_cache".into());

        match Self::open_db(&path) {
            Ok(storage) => Ok(storage),
            Err(e) if e.is_storage_corrupted() => {
                tracing::warn!(
                    "detected corrupted storage at {}, deleting and recreating: {e}",
                    path.display()
                );
                crate::metrics::STORAGE_RECOVERY_TOTAL.inc();
                if let Err(rm_err) = std::fs::remove_dir_all(&path) {
                    tracing::warn!("failed to remove corrupted db directory: {rm_err}");
                }
                Self::open_db(&path)
            }
            Err(e) => Err(e),
        }
    }

    fn open_db(path: &Path) -> Result<Self, WintermuteError> {
        std::fs::create_dir_all(path)?;
        tracing::info!(
            "opening queue logs at {} (segment={}MB, fsync={}ms, backfill shards={})",
            path.display(),
            QUEUE_LOG_SEGMENT_BYTES / (1024 * 1024),
            QUEUE_LOG_FSYNC_MS,
            BACKFILL_SHARDS
        );

        let firehose_events_dir = path.join("firehose_events");
        std::fs::create_dir_all(&firehose_events_dir)?;

        let backfill_dir = path.join("firehose_backfill");
        let firehose_backfill_shards = (0..BACKFILL_SHARDS)
            .map(|i| open_log(backfill_dir.join(format!("shard_{i:03}"))))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            firehose_events_dir,
            repo_backfill: RepoBackfillQueue::open(&path.join("repo_backfill"))?,
            firehose_live: open_log(path.join("firehose_live"))?,
            label_live: open_log(path.join("label_live"))?,
            firehose_backfill_priority: open_log(backfill_dir.join("priority"))?,
            firehose_backfill_shards,
            cursors: CursorFile::open(path.join("cursors.cbor"))?,
            live_notify: tokio::sync::Notify::new(),
        })
    }

    // ---- firehose_events ---------------------------------------------------

    fn event_path(&self, seq: i64) -> PathBuf {
        self.firehose_events_dir.join(format!("{seq}.cbor"))
    }

    pub fn write_firehose_event(
        &self,
        seq: i64,
        event: &FirehoseEvent,
    ) -> Result<(), WintermuteError> {
        write_atomic(&self.event_path(seq), &encode(event)?)?;
        Ok(())
    }

    pub fn read_firehose_event(&self, seq: i64) -> Result<Option<FirehoseEvent>, WintermuteError> {
        match std::fs::read(self.event_path(seq)) {
            Ok(bytes) => Ok(Some(decode(&bytes)?)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    // ---- repo_backfill -----------------------------------------------------

    /// Enqueue a backfill job with normal priority.
    /// Normal priority items are processed after all priority items.
    pub fn enqueue_backfill(&self, job: &BackfillJob) -> Result<(), WintermuteError> {
        self.repo_backfill.normal.append(&encode(job)?)?;
        crate::metrics::INGESTER_REPO_BACKFILL_LENGTH.inc();
        Ok(())
    }

    /// Enqueue a backfill job with HIGH priority.
    /// Priority items are processed BEFORE all normal items.
    /// Use this for manual/on-demand backfill requests.
    pub fn enqueue_backfill_priority(&self, job: &BackfillJob) -> Result<(), WintermuteError> {
        self.repo_backfill.priority.append(&encode(job)?)?;
        crate::metrics::INGESTER_REPO_BACKFILL_LENGTH.inc();
        Ok(())
    }

    /// Enqueue a backfill job with IMMEDIATE priority.
    /// These items are processed FIRST, before all other priority items.
    pub fn enqueue_backfill_immediate(&self, job: &BackfillJob) -> Result<(), WintermuteError> {
        self.repo_backfill.immediate.append(&encode(job)?)?;
        crate::metrics::INGESTER_REPO_BACKFILL_LENGTH.inc();
        Ok(())
    }

    pub fn dequeue_backfill(&self) -> Result<Option<(Vec<u8>, BackfillJob)>, WintermuteError> {
        let mut batch = self.dequeue_backfill_batch(1)?;
        if batch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(batch.remove(0)))
        }
    }

    /// Dequeue up to `count` jobs: immediate, then priority, then normal,
    /// each in arrival order.
    pub fn dequeue_backfill_batch(
        &self,
        count: usize,
    ) -> Result<Vec<(Vec<u8>, BackfillJob)>, WintermuteError> {
        let (jobs, dropped) = self.repo_backfill.dequeue(count)?;
        #[allow(clippy::cast_possible_wrap)]
        crate::metrics::INGESTER_REPO_BACKFILL_LENGTH.sub((jobs.len() + dropped) as i64);
        Ok(jobs)
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn remove_backfill(&self, _key: &[u8]) -> Result<(), WintermuteError> {
        // Item already removed in dequeue - this is now a no-op for compatibility
        Ok(())
    }

    // ---- firehose_live -----------------------------------------------------

    /// Enqueue a live index job. Keys are log positions, so dequeue order is
    /// arrival order.
    pub fn enqueue_firehose_live(&self, job: &IndexJob) -> Result<(), WintermuteError> {
        self.firehose_live.append(&encode(job)?)?;
        crate::metrics::INGESTER_FIREHOSE_LIVE_LENGTH.inc();
        self.live_notify.notify_one();
        Ok(())
    }

    /// Block until an enqueue signals the live queue, or `timeout` elapses.
    /// A permit stored by a `notify_one` that raced ahead completes immediately.
    pub async fn wait_for_live_enqueue(&self, timeout: Duration) {
        drop(tokio::time::timeout(timeout, self.live_notify.notified()).await);
    }

    pub fn dequeue_firehose_live(&self) -> Result<Option<(Vec<u8>, IndexJob)>, WintermuteError> {
        let mut batch = self.dequeue_firehose_live_batch(1)?;
        if batch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(batch.remove(0)))
        }
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn remove_firehose_live(&self, _key: &[u8]) -> Result<(), WintermuteError> {
        // Item already removed in dequeue - this is now a no-op for compatibility
        Ok(())
    }

    /// Decodes a batch of log records, dropping any that fail to
    /// deserialize so a poison entry cannot wedge the queue. Returns the
    /// jobs and the number dropped.
    fn decode_jobs<T: serde::de::DeserializeOwned>(
        queue: &str,
        records: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> (Vec<(Vec<u8>, T)>, usize) {
        let mut out = Vec::with_capacity(records.len());
        let mut dropped = 0usize;
        for (key, bytes) in records {
            match decode(&bytes) {
                Ok(job) => out.push((key, job)),
                Err(e) => {
                    tracing::error!("dropping undeserializable {queue} entry: {e}");
                    dropped += 1;
                }
            }
        }
        (out, dropped)
    }

    /// Dequeue up to `limit` live jobs in arrival order.
    pub fn dequeue_firehose_live_batch(
        &self,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, IndexJob)>, WintermuteError> {
        let start = std::time::Instant::now();
        let records = self.firehose_live.read_batch(limit)?;
        let (jobs, dropped) = Self::decode_jobs("firehose_live", records);
        #[allow(clippy::cast_possible_wrap)]
        crate::metrics::INGESTER_FIREHOSE_LIVE_LENGTH.sub((jobs.len() + dropped) as i64);
        let elapsed_ms = start.elapsed().as_millis();
        if elapsed_ms > 1000 {
            tracing::warn!("SLOW live dequeue: {elapsed_ms}ms for {} jobs", jobs.len());
        }
        Ok(jobs)
    }

    // ---- firehose_backfill -------------------------------------------------
    //
    // Normal items are spread over BACKFILL_SHARDS logs so that N indexer
    // workers each own a disjoint slice and never contend on a head lock.
    // Priority items go to one shared log that every worker drains first.

    fn backfill_key(shard: usize, key: &[u8]) -> Vec<u8> {
        let mut full = Vec::with_capacity(2 + key.len());
        #[allow(clippy::cast_possible_truncation)]
        full.extend_from_slice(&(shard as u16).to_be_bytes());
        full.extend_from_slice(key);
        full
    }

    fn random_shard() -> usize {
        rand::random::<usize>() % BACKFILL_SHARDS
    }

    /// Enqueue a firehose backfill job with normal priority into a random shard.
    pub fn enqueue_firehose_backfill(&self, job: &IndexJob) -> Result<(), WintermuteError> {
        self.firehose_backfill_shards[Self::random_shard()].append(&encode(job)?)?;
        crate::metrics::INGESTER_FIREHOSE_BACKFILL_LENGTH.inc();
        Ok(())
    }

    /// Batch enqueue multiple firehose backfill jobs.
    pub fn enqueue_firehose_backfill_batch(
        &self,
        jobs: &[IndexJob],
    ) -> Result<(), WintermuteError> {
        for job in jobs {
            self.firehose_backfill_shards[Self::random_shard()].append(&encode(job)?)?;
        }
        #[allow(clippy::cast_possible_wrap)]
        crate::metrics::INGESTER_FIREHOSE_BACKFILL_LENGTH.add(jobs.len() as i64);
        Ok(())
    }

    /// Enqueue a firehose backfill job with HIGH priority.
    /// Priority items are indexed BEFORE all normal backfill items.
    pub fn enqueue_firehose_backfill_priority(
        &self,
        job: &IndexJob,
    ) -> Result<(), WintermuteError> {
        self.firehose_backfill_priority.append(&encode(job)?)?;
        crate::metrics::INGESTER_FIREHOSE_BACKFILL_LENGTH.inc();
        Ok(())
    }

    /// Batch enqueue multiple firehose backfill jobs with HIGH priority.
    pub fn enqueue_firehose_backfill_priority_batch(
        &self,
        jobs: &[IndexJob],
    ) -> Result<(), WintermuteError> {
        for job in jobs {
            self.firehose_backfill_priority.append(&encode(job)?)?;
        }
        #[allow(clippy::cast_possible_wrap)]
        crate::metrics::INGESTER_FIREHOSE_BACKFILL_LENGTH.add(jobs.len() as i64);
        Ok(())
    }

    /// Drains up to `limit` jobs from the priority log, then from the given
    /// shard range in order.
    fn drain_backfill(
        &self,
        shards: std::ops::Range<usize>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, IndexJob)>, WintermuteError> {
        let mut results = Vec::with_capacity(limit);
        let mut dropped = 0usize;

        let (jobs, d) = Self::decode_jobs::<IndexJob>(
            "firehose_backfill",
            self.firehose_backfill_priority.read_batch(limit)?,
        );
        dropped += d;
        results.extend(
            jobs.into_iter()
                .map(|(k, j)| (Self::backfill_key(BACKFILL_SHARDS, &k), j)),
        );

        for shard in shards {
            if results.len() >= limit {
                break;
            }
            let (jobs, d) = Self::decode_jobs::<IndexJob>(
                "firehose_backfill",
                self.firehose_backfill_shards[shard].read_batch(limit - results.len())?,
            );
            dropped += d;
            results.extend(
                jobs.into_iter()
                    .map(|(k, j)| (Self::backfill_key(shard, &k), j)),
            );
        }

        #[allow(clippy::cast_possible_wrap)]
        crate::metrics::INGESTER_FIREHOSE_BACKFILL_LENGTH.sub((results.len() + dropped) as i64);
        Ok(results)
    }

    pub fn dequeue_firehose_backfill(
        &self,
    ) -> Result<Option<(Vec<u8>, IndexJob)>, WintermuteError> {
        let mut batch = self.dequeue_firehose_backfill_batch(1)?;
        if batch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(batch.remove(0)))
        }
    }

    /// Batch dequeue up to `limit` jobs from `firehose_backfill`: priority
    /// first, then every shard in order.
    pub fn dequeue_firehose_backfill_batch(
        &self,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, IndexJob)>, WintermuteError> {
        self.drain_backfill(0..BACKFILL_SHARDS, limit)
    }

    /// Partitioned dequeue for parallel workers - each worker owns a slice of the shards.
    ///
    /// - Priority items are checked first by ALL workers
    /// - Normal items are partitioned among workers
    ///
    /// With N workers, worker i owns shards in range [start, end) where:
    /// - start = i * 240 / N
    /// - end = (i + 1) * 240 / N (the last worker takes the remainder)
    ///
    /// Each shard is its own log with its own head, so workers never
    /// contend except on the shared priority log.
    pub fn dequeue_firehose_backfill_partitioned(
        &self,
        worker_id: usize,
        num_workers: usize,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, IndexJob)>, WintermuteError> {
        let num_workers = num_workers.max(1);
        let partition_size = BACKFILL_SHARDS / num_workers;
        let start = (worker_id * partition_size).min(BACKFILL_SHARDS);
        let end = if worker_id + 1 == num_workers {
            BACKFILL_SHARDS
        } else {
            ((worker_id + 1) * partition_size).min(BACKFILL_SHARDS)
        };
        self.drain_backfill(start..end, limit)
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn remove_firehose_backfill(&self, _key: &[u8]) -> Result<(), WintermuteError> {
        // Item already removed in dequeue - this is now a no-op for compatibility
        Ok(())
    }

    // ---- label_live --------------------------------------------------------

    pub fn enqueue_label_live(&self, event: &LabelEvent) -> Result<(), WintermuteError> {
        self.label_live.append(&encode(event)?)?;
        crate::metrics::INGESTER_LABEL_LIVE_LENGTH.inc();
        Ok(())
    }

    pub fn dequeue_label_live(&self) -> Result<Option<(Vec<u8>, LabelEvent)>, WintermuteError> {
        let records = self.label_live.read_batch(1)?;
        let (mut events, dropped) = Self::decode_jobs::<LabelEvent>("label_live", records);
        #[allow(clippy::cast_possible_wrap)]
        crate::metrics::INGESTER_LABEL_LIVE_LENGTH.sub((events.len() + dropped) as i64);
        if events.is_empty() {
            Ok(None)
        } else {
            Ok(Some(events.remove(0)))
        }
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn remove_label_live(&self, _key: &[u8]) -> Result<(), WintermuteError> {
        // Item already removed in dequeue - this is now a no-op for compatibility
        Ok(())
    }

    // ---- cursors -----------------------------------------------------------

    pub fn get_cursor(&self, name: &str) -> Result<Option<i64>, WintermuteError> {
        Ok(self.cursors.get(name))
    }

    pub fn set_cursor(&self, name: &str, value: i64) -> Result<(), WintermuteError> {
        self.cursors.update(|m| {
            m.insert(name.to_owned(), value);
        })
    }

    pub fn delete_cursor(&self, name: &str) -> Result<(), WintermuteError> {
        self.cursors.update(|m| {
            m.remove(name);
        })
    }

    // ---- lengths -----------------------------------------------------------
    //
    // Every log tracks its unread count, so exact and approximate lengths
    // are the same O(1) read. Both names are kept for callers.

    pub fn repo_backfill_len(&self) -> Result<usize, WintermuteError> {
        Ok(self.repo_backfill.len())
    }

    #[must_use]
    pub fn repo_backfill_approx_len(&self) -> usize {
        self.repo_backfill.len()
    }

    pub fn firehose_live_len(&self) -> Result<usize, WintermuteError> {
        Ok(self.firehose_live.len())
    }

    #[must_use]
    pub fn firehose_live_approx_len(&self) -> usize {
        self.firehose_live.len()
    }

    pub fn firehose_backfill_len(&self) -> Result<usize, WintermuteError> {
        Ok(self.firehose_backfill_priority.len()
            + self
                .firehose_backfill_shards
                .iter()
                .map(SegmentedLog::len)
                .sum::<usize>())
    }

    pub fn label_live_len(&self) -> Result<usize, WintermuteError> {
        Ok(self.label_live.len())
    }

    #[must_use]
    pub fn label_live_approx_len(&self) -> usize {
        self.label_live.len()
    }

    // ---- operator helpers --------------------------------------------------

    /// Peek at the first N items in `repo_backfill` without removing them
    pub fn peek_backfill(
        &self,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, BackfillJob)>, WintermuteError> {
        self.repo_backfill.peek(limit)
    }

    /// Remove all entries for a specific DID from `repo_backfill`
    /// Returns the number of entries removed
    pub fn remove_backfill_by_did(&self, did: &str) -> Result<usize, WintermuteError> {
        let removed = self.repo_backfill.remove_by_did(did)?;
        #[allow(clippy::cast_possible_wrap)]
        crate::metrics::INGESTER_REPO_BACKFILL_LENGTH.sub(removed as i64);
        Ok(removed)
    }

    /// Clear all items from `repo_backfill`
    pub fn clear_repo_backfill(&self) -> Result<(), WintermuteError> {
        let removed = self.repo_backfill.clear()?;
        #[allow(clippy::cast_possible_wrap)]
        crate::metrics::INGESTER_REPO_BACKFILL_LENGTH.sub(removed as i64);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CommitData, Label, WriteAction};
    use tempfile::TempDir;

    fn setup_test_storage() -> (Storage, TempDir) {
        let temp_dir = TempDir::with_prefix("wintermute_test_").unwrap();
        let db_path = temp_dir.path().join("test_db");
        let storage = Storage::new(Some(db_path)).unwrap();
        (storage, temp_dir)
    }

    fn index_job(uri: &str) -> IndexJob {
        IndexJob {
            uri: uri.to_owned(),
            cid: "cid".to_owned(),
            action: WriteAction::Create,
            record: Some(serde_json::json!({"test": "data"})),
            indexed_at: "2025-01-01T00:00:00Z".to_owned(),
            rev: "rev".to_owned(),
            provenance: None,
        }
    }

    fn backfill_job(did: &str, priority: bool) -> BackfillJob {
        BackfillJob {
            did: did.to_owned(),
            retry_count: 0,
            priority,
        }
    }

    #[test]
    fn test_firehose_event_roundtrip() {
        let (storage, _dir) = setup_test_storage();

        let event = FirehoseEvent {
            seq: 12345,
            did: "did:plc:test123".to_owned(),
            time: "2025-01-01T00:00:00Z".to_owned(),
            kind: "commit".to_owned(),
            commit: Some(CommitData {
                rev: "rev123".to_owned(),
                ops: vec![],
                blocks: vec![],
                cid: None,
            }),
            identity: None,
            account: None,
        };

        storage.write_firehose_event(12345, &event).unwrap();
        let retrieved = storage.read_firehose_event(12345).unwrap().unwrap();
        assert_eq!(retrieved.seq, event.seq);
        assert_eq!(retrieved.did, event.did);
        assert!(storage.read_firehose_event(99999).unwrap().is_none());
    }

    #[test]
    fn test_backfill_queue() {
        let (storage, _dir) = setup_test_storage();
        let job = backfill_job("did:plc:test456", false);

        storage.enqueue_backfill(&job).unwrap();
        let (key, retrieved) = storage.dequeue_backfill().unwrap().unwrap();
        assert_eq!(retrieved.did, job.did);
        assert_eq!(retrieved.retry_count, job.retry_count);

        storage.remove_backfill(&key).unwrap();
        assert!(storage.dequeue_backfill().unwrap().is_none());
    }

    #[test]
    fn test_backfill_priority_queue() {
        let (storage, _dir) = setup_test_storage();

        storage
            .enqueue_backfill(&backfill_job("did:plc:normal1", false))
            .unwrap();
        storage
            .enqueue_backfill(&backfill_job("did:plc:normal2", false))
            .unwrap();
        storage
            .enqueue_backfill_priority(&backfill_job("did:plc:priority1", true))
            .unwrap();
        storage
            .enqueue_backfill_priority(&backfill_job("did:plc:priority2", true))
            .unwrap();
        storage
            .enqueue_backfill_immediate(&backfill_job("did:plc:immediate", true))
            .unwrap();

        let order: Vec<String> = std::iter::from_fn(|| storage.dequeue_backfill().unwrap())
            .map(|(_, j)| j.did)
            .collect();
        assert_eq!(
            order,
            [
                "did:plc:immediate",
                "did:plc:priority1",
                "did:plc:priority2",
                "did:plc:normal1",
                "did:plc:normal2",
            ]
        );
    }

    #[test]
    fn test_firehose_live_queue() {
        let (storage, _dir) = setup_test_storage();
        let job = index_job("at://did:plc:test/app.bsky.feed.post/123");

        storage.enqueue_firehose_live(&job).unwrap();
        let (key, retrieved) = storage.dequeue_firehose_live().unwrap().unwrap();
        assert_eq!(retrieved.uri, job.uri);
        assert_eq!(retrieved.cid, job.cid);

        storage.remove_firehose_live(&key).unwrap();
        assert!(storage.dequeue_firehose_live().unwrap().is_none());
    }

    #[test]
    fn test_firehose_backfill_queue() {
        let (storage, _dir) = setup_test_storage();
        let job = index_job("at://did:plc:test/app.bsky.feed.post/456");

        storage.enqueue_firehose_backfill(&job).unwrap();
        let (key, retrieved) = storage.dequeue_firehose_backfill().unwrap().unwrap();
        assert_eq!(retrieved.uri, job.uri);

        storage.remove_firehose_backfill(&key).unwrap();
        assert!(storage.dequeue_firehose_backfill().unwrap().is_none());
    }

    #[test]
    fn test_firehose_backfill_priority_queue() {
        let (storage, _dir) = setup_test_storage();

        storage
            .enqueue_firehose_backfill(&index_job("at://did:plc:normal/app.bsky.feed.post/1"))
            .unwrap();
        storage
            .enqueue_firehose_backfill(&index_job("at://did:plc:normal/app.bsky.feed.post/2"))
            .unwrap();
        storage
            .enqueue_firehose_backfill_priority(&index_job(
                "at://did:plc:priority/app.bsky.feed.post/1",
            ))
            .unwrap();
        storage
            .enqueue_firehose_backfill_priority(&index_job(
                "at://did:plc:priority/app.bsky.feed.post/2",
            ))
            .unwrap();

        let (_, first) = storage.dequeue_firehose_backfill().unwrap().unwrap();
        assert_eq!(first.uri, "at://did:plc:priority/app.bsky.feed.post/1");
        let (_, second) = storage.dequeue_firehose_backfill().unwrap().unwrap();
        assert_eq!(second.uri, "at://did:plc:priority/app.bsky.feed.post/2");

        let mut rest: Vec<String> =
            std::iter::from_fn(|| storage.dequeue_firehose_backfill().unwrap())
                .map(|(_, j)| j.uri)
                .collect();
        rest.sort();
        assert_eq!(
            rest,
            [
                "at://did:plc:normal/app.bsky.feed.post/1",
                "at://did:plc:normal/app.bsky.feed.post/2",
            ]
        );
    }

    #[test]
    fn test_firehose_backfill_batch_dequeue() {
        let (storage, _dir) = setup_test_storage();
        for i in 0..10 {
            storage
                .enqueue_firehose_backfill(&index_job(&format!(
                    "at://did:plc:test/app.bsky.feed.post/{i}"
                )))
                .unwrap();
        }
        assert_eq!(storage.firehose_backfill_len().unwrap(), 10);
        let batch = storage.dequeue_firehose_backfill_batch(4).unwrap();
        assert_eq!(batch.len(), 4);
        assert_eq!(storage.firehose_backfill_len().unwrap(), 6);
        let batch = storage.dequeue_firehose_backfill_batch(100).unwrap();
        assert_eq!(batch.len(), 6);
        assert!(
            storage
                .dequeue_firehose_backfill_batch(100)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn test_firehose_backfill_partitioned_dequeue() {
        let (storage, _dir) = setup_test_storage();
        for i in 0..200 {
            storage
                .enqueue_firehose_backfill(&index_job(&format!(
                    "at://did:plc:test/app.bsky.feed.post/{i}"
                )))
                .unwrap();
        }

        // Every worker's slice is disjoint, and the union is everything.
        let num_workers = 4;
        let mut seen = std::collections::HashSet::new();
        for worker in 0..num_workers {
            for (key, _) in storage
                .dequeue_firehose_backfill_partitioned(worker, num_workers, 1000)
                .unwrap()
            {
                assert!(seen.insert(key), "job dequeued by two workers");
            }
        }
        assert_eq!(seen.len(), 200);
        assert_eq!(storage.firehose_backfill_len().unwrap(), 0);
    }

    #[test]
    fn test_firehose_backfill_partitioned_priority() {
        let (storage, _dir) = setup_test_storage();
        for i in 0..20 {
            storage
                .enqueue_firehose_backfill(&index_job(&format!(
                    "at://did:plc:normal/app.bsky.feed.post/{i}"
                )))
                .unwrap();
        }
        storage
            .enqueue_firehose_backfill_priority(&index_job(
                "at://did:plc:priority/app.bsky.feed.post/1",
            ))
            .unwrap();

        // Any worker sees the priority item first.
        let batch = storage
            .dequeue_firehose_backfill_partitioned(3, 4, 5)
            .unwrap();
        assert_eq!(batch[0].1.uri, "at://did:plc:priority/app.bsky.feed.post/1");
    }

    #[test]
    fn test_label_live_queue() {
        let (storage, _dir) = setup_test_storage();
        let event = LabelEvent {
            seq: 1,
            labels: vec![Label {
                src: "did:plc:labeler".to_owned(),
                uri: "at://did:plc:test/app.bsky.feed.post/1".to_owned(),
                cid: None,
                val: "spam".to_owned(),
                neg: false,
                cts: "2025-01-01T00:00:00Z".to_owned(),
                exp: None,
            }],
        };
        storage.enqueue_label_live(&event).unwrap();
        assert_eq!(storage.label_live_len().unwrap(), 1);
        let (_, retrieved) = storage.dequeue_label_live().unwrap().unwrap();
        assert_eq!(retrieved.labels[0].val, "spam");
        assert!(storage.dequeue_label_live().unwrap().is_none());
    }

    #[test]
    fn test_cursor() {
        let (storage, _dir) = setup_test_storage();
        assert_eq!(storage.get_cursor("firehose").unwrap(), None);
        storage.set_cursor("firehose", 42).unwrap();
        assert_eq!(storage.get_cursor("firehose").unwrap(), Some(42));
        storage.set_cursor("firehose", -7).unwrap();
        assert_eq!(storage.get_cursor("firehose").unwrap(), Some(-7));
        storage.set_cursor("labels", 1).unwrap();
        assert_eq!(storage.get_cursor("labels").unwrap(), Some(1));
    }

    #[test]
    fn test_delete_cursor() {
        let (storage, _dir) = setup_test_storage();
        storage.set_cursor("c", 5).unwrap();
        assert_eq!(storage.get_cursor("c").unwrap(), Some(5));
        storage.delete_cursor("c").unwrap();
        assert_eq!(storage.get_cursor("c").unwrap(), None);
        storage.delete_cursor("missing").unwrap();
    }

    #[test]
    fn test_is_storage_corrupted() {
        let corrupt = WintermuteError::Storage("segment corrupt beyond repair".to_owned());
        assert!(corrupt.is_storage_corrupted());
        let io: WintermuteError = std::io::Error::other("disk").into();
        assert!(!io.is_storage_corrupted());
        assert!(!WintermuteError::Other("x".to_owned()).is_storage_corrupted());
        assert!(!WintermuteError::Serialization("x".to_owned()).is_storage_corrupted());
    }

    #[test]
    fn test_storage_reopen_preserves_state() {
        let temp_dir = TempDir::with_prefix("storage_recovery_test_").unwrap();
        let db_path = temp_dir.path().join("test_db");

        let storage = Storage::new(Some(db_path.clone())).unwrap();
        storage.set_cursor("test", 42).unwrap();
        storage
            .enqueue_backfill(&backfill_job("did:plc:queued", false))
            .unwrap();
        storage
            .enqueue_firehose_backfill(&index_job("at://did:plc:a/app.bsky.feed.post/1"))
            .unwrap();
        drop(storage);

        let storage = Storage::new(Some(db_path)).unwrap();
        assert_eq!(storage.get_cursor("test").unwrap(), Some(42));
        assert_eq!(storage.repo_backfill_len().unwrap(), 1);
        assert_eq!(storage.firehose_backfill_len().unwrap(), 1);
        assert_eq!(
            storage.dequeue_backfill().unwrap().unwrap().1.did,
            "did:plc:queued"
        );
    }

    #[test]
    fn test_remove_backfill_by_did() {
        let (storage, _dir) = setup_test_storage();
        storage
            .enqueue_backfill(&backfill_job("did:plc:keep1", false))
            .unwrap();
        storage
            .enqueue_backfill(&backfill_job("did:plc:remove", false))
            .unwrap();
        storage
            .enqueue_backfill_priority(&backfill_job("did:plc:remove", true))
            .unwrap();
        storage
            .enqueue_backfill(&backfill_job("did:plc:keep2", false))
            .unwrap();
        assert_eq!(storage.repo_backfill_len().unwrap(), 4);

        assert_eq!(storage.remove_backfill_by_did("did:plc:remove").unwrap(), 2);
        assert_eq!(storage.repo_backfill_len().unwrap(), 2);
        // Removing again finds nothing new.
        assert_eq!(storage.remove_backfill_by_did("did:plc:remove").unwrap(), 0);

        let peeked: Vec<String> = storage
            .peek_backfill(10)
            .unwrap()
            .into_iter()
            .map(|(_, j)| j.did)
            .collect();
        assert_eq!(peeked, ["did:plc:keep1", "did:plc:keep2"]);

        let order: Vec<String> = std::iter::from_fn(|| storage.dequeue_backfill().unwrap())
            .map(|(_, j)| j.did)
            .collect();
        assert_eq!(order, ["did:plc:keep1", "did:plc:keep2"]);

        // A DID re-enqueued after removal is not swallowed by a stale mark.
        storage
            .enqueue_backfill(&backfill_job("did:plc:remove", false))
            .unwrap();
        assert_eq!(
            storage.dequeue_backfill().unwrap().unwrap().1.did,
            "did:plc:remove"
        );
    }

    #[test]
    fn test_remove_backfill_by_did_not_found() {
        let (storage, _dir) = setup_test_storage();
        storage
            .enqueue_backfill(&backfill_job("did:plc:present", false))
            .unwrap();
        assert_eq!(storage.remove_backfill_by_did("did:plc:absent").unwrap(), 0);
        assert_eq!(storage.repo_backfill_len().unwrap(), 1);
    }

    #[test]
    fn test_clear_repo_backfill() {
        let (storage, _dir) = setup_test_storage();
        storage
            .enqueue_backfill_priority(&backfill_job("did:plc:p", true))
            .unwrap();
        storage
            .enqueue_backfill(&backfill_job("did:plc:n", false))
            .unwrap();
        assert_eq!(storage.repo_backfill_len().unwrap(), 2);
        storage.clear_repo_backfill().unwrap();
        assert_eq!(storage.repo_backfill_len().unwrap(), 0);
        assert!(storage.dequeue_backfill().unwrap().is_none());
    }

    #[test]
    fn test_clear_empty_repo_backfill() {
        let (storage, _dir) = setup_test_storage();
        storage.clear_repo_backfill().unwrap();
        assert_eq!(storage.repo_backfill_len().unwrap(), 0);
    }

    #[test]
    fn live_queue_resumes_after_reopen() {
        let dir = TempDir::with_prefix("wintermute_test_").unwrap();
        let db_path = dir.path().join("test_db");
        let storage = Storage::new(Some(db_path.clone())).unwrap();
        for i in 0..6 {
            storage
                .enqueue_firehose_live(&index_job(&format!(
                    "at://did:plc:a/app.bsky.feed.post/p{i}"
                )))
                .unwrap();
        }
        assert_eq!(storage.dequeue_firehose_live_batch(4).unwrap().len(), 4);
        drop(storage);

        let storage = Storage::new(Some(db_path)).unwrap();
        assert_eq!(storage.firehose_live_len().unwrap(), 2);
        let resumed = storage.dequeue_firehose_live_batch(10).unwrap();
        assert_eq!(
            resumed.len(),
            2,
            "reopen must resume after the persisted head"
        );
        assert_eq!(resumed[0].1.uri, "at://did:plc:a/app.bsky.feed.post/p4");
        assert_eq!(resumed[1].1.uri, "at://did:plc:a/app.bsky.feed.post/p5");

        // A fully drained queue accepts and delivers new work after reopen.
        storage
            .enqueue_firehose_live(&index_job("at://did:plc:a/app.bsky.feed.post/fresh"))
            .unwrap();
        let fresh = storage.dequeue_firehose_live_batch(10).unwrap();
        assert_eq!(fresh.len(), 1);
        assert_eq!(fresh[0].1.uri, "at://did:plc:a/app.bsky.feed.post/fresh");
    }

    #[test]
    fn live_queue_dequeues_in_arrival_order() {
        let (storage, _dir) = setup_test_storage();
        for i in 0..5 {
            storage
                .enqueue_firehose_live(&index_job(&format!(
                    "at://did:plc:a/app.bsky.feed.post/r{i}"
                )))
                .unwrap();
        }
        let batch = storage.dequeue_firehose_live_batch(10).unwrap();
        assert_eq!(batch.len(), 5);
        for (i, (key, job)) in batch.iter().enumerate() {
            assert_eq!(job.uri, format!("at://did:plc:a/app.bsky.feed.post/r{i}"));
            assert_eq!(key.len(), 16);
        }
        assert!(batch.windows(2).all(|w| w[0].0 < w[1].0));
        storage
            .enqueue_firehose_live(&index_job("at://did:plc:a/app.bsky.feed.post/r5"))
            .unwrap();
        let next = storage.dequeue_firehose_live_batch(10).unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].1.uri, "at://did:plc:a/app.bsky.feed.post/r5");
        assert!(storage.dequeue_firehose_live_batch(10).unwrap().is_empty());
    }

    #[test]
    fn live_queue_keys_stay_monotonic_across_reopen() {
        let temp_dir = TempDir::with_prefix("wintermute_test_").unwrap();
        let db_path = temp_dir.path().join("test_db");
        {
            let storage = Storage::new(Some(db_path.clone())).unwrap();
            storage
                .enqueue_firehose_live(&index_job("at://did:plc:a/app.bsky.feed.post/first"))
                .unwrap();
        }
        let storage = Storage::new(Some(db_path)).unwrap();
        storage
            .enqueue_firehose_live(&index_job("at://did:plc:a/app.bsky.feed.post/second"))
            .unwrap();
        let batch = storage.dequeue_firehose_live_batch(10).unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].1.uri, "at://did:plc:a/app.bsky.feed.post/first");
        assert_eq!(batch[1].1.uri, "at://did:plc:a/app.bsky.feed.post/second");
        assert!(batch[0].0 < batch[1].0);
    }

    #[test]
    fn drained_live_queue_holds_no_dead_data() {
        // The property the Fjall generation swap existed to restore: after a
        // full drain, nothing of the drained jobs remains on disk.
        let dir = TempDir::with_prefix("wintermute_test_").unwrap();
        let db_path = dir.path().join("test_db");
        let storage = Storage::new(Some(db_path.clone())).unwrap();
        for i in 0..500 {
            storage
                .enqueue_firehose_live(&index_job(&format!(
                    "at://did:plc:a/app.bsky.feed.post/t{i}"
                )))
                .unwrap();
        }
        assert_eq!(storage.dequeue_firehose_live_batch(500).unwrap().len(), 500);
        assert_eq!(storage.firehose_live_approx_len(), 0);
        drop(storage);

        let live_dir = db_path.join("firehose_live");
        let bytes: u64 = std::fs::read_dir(&live_dir)
            .unwrap()
            .map(|e| e.unwrap().metadata().unwrap().len())
            .sum();
        // Only the head file and the (empty or near-empty) current segment.
        assert!(bytes < 1024, "drained queue still holds {bytes} bytes");

        let storage = Storage::new(Some(db_path)).unwrap();
        assert!(
            storage
                .dequeue_firehose_live_batch(1000)
                .unwrap()
                .is_empty()
        );
    }
}
