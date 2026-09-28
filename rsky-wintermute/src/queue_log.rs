//! Segmented append-only log: a durable FIFO of opaque byte records.
//!
//! A queue is written once and read once, in order. The on-disk shape for
//! that is a log, not a sorted map: records are appended to the current
//! segment file, a consumer reads forward from a persisted head position,
//! and a segment is unlinked as soon as the head has moved past it. Nothing
//! is rewritten in place, so there are no tombstones and no compaction, and
//! the cost of a dequeue does not depend on how much has been drained.
//!
//! Layout inside the log directory:
//!
//! ```text
//! <segment id, 20 digits>.seg   records: [u32 len][u32 crc32][payload]
//! head                          mark of the next record to read
//! checkpoint                    mark of the end of the last fsynced append
//! ```
//!
//! A mark is `[u64 seg][u64 off][u64 ordinal][u64 epoch][u32 crc32][u32 0]`.
//! The ordinal counts the records before that position; two marks with the
//! same epoch share a base, so their difference is a record count. That is
//! how open knows the unread count without walking the backlog: checkpoint
//! minus head, plus a scan of the records appended since the checkpoint.
//! Whenever the marks cannot be trusted the log rescans and starts a new
//! epoch, so a stale or mismatched mark costs time, never correctness.
//!
//! Durability: appends are fsynced at most once per `fsync_interval`, on
//! segment roll and on drop. The periodic fsync runs outside the writer
//! lock, so appends and drains keep going while the disk flushes. The head is rewritten after every read batch
//! but not synced, so a crash re-delivers at most the batches since the last
//! head write (at-least-once). A torn record at the tail of the last segment
//! is truncated on open; a corrupt record elsewhere skips the rest of its
//! segment rather than wedging the queue.

use crc32fast::Hasher;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, TryLockError};
use std::time::{Duration, Instant};

const RECORD_HEADER: usize = 8;
/// Anything larger is treated as corruption rather than allocated.
const MAX_RECORD: usize = 64 * 1024 * 1024;
const HEAD_FILE: &str = "head";
const CHECKPOINT_FILE: &str = "checkpoint";
const MARK_LEN: usize = 40;
/// Head files written before marks carried an ordinal: `[seg][off][crc][0]`.
const LEGACY_HEAD_LEN: usize = 24;
const READ_CHUNK: u64 = 1 << 20;

/// A record's position: segment id, then byte offset within the segment.
/// Ordering is arrival order, and the 16-byte big-endian encoding returned
/// as a record key preserves it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Pos {
    seg: u64,
    off: u64,
}

impl Pos {
    fn to_key(self) -> Vec<u8> {
        let mut key = Vec::with_capacity(16);
        key.extend_from_slice(&self.seg.to_be_bytes());
        key.extend_from_slice(&self.off.to_be_bytes());
        key
    }
}

/// A position plus the number of records before it (see the module docs).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Mark {
    pos: Pos,
    ordinal: u64,
    epoch: u64,
}

impl Mark {
    fn encode(self) -> [u8; MARK_LEN] {
        let mut buf = [0u8; MARK_LEN];
        buf[..8].copy_from_slice(&self.pos.seg.to_be_bytes());
        buf[8..16].copy_from_slice(&self.pos.off.to_be_bytes());
        buf[16..24].copy_from_slice(&self.ordinal.to_be_bytes());
        buf[24..32].copy_from_slice(&self.epoch.to_be_bytes());
        let check = crc32(&buf[..32]);
        buf[32..36].copy_from_slice(&check.to_le_bytes());
        buf
    }
}

fn be_u64(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes.try_into().unwrap_or([0; 8]))
}

fn le_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().unwrap_or([0; 4]))
}

/// What a mark file held: a full mark, a legacy head with a position only,
/// or nothing usable (missing, short or failing its checksum).
enum Loaded {
    Mark(Mark),
    PosOnly(Pos),
    Missing,
}

fn load_mark(file: &File) -> io::Result<Loaded> {
    let len = file.metadata()?.len();
    if len == LEGACY_HEAD_LEN as u64 {
        let mut buf = [0u8; LEGACY_HEAD_LEN];
        file.read_exact_at(&mut buf, 0)?;
        if crc32(&buf[..16]) != le_u32(&buf[16..20]) {
            return Ok(Loaded::Missing);
        }
        return Ok(Loaded::PosOnly(Pos {
            seg: be_u64(&buf[..8]),
            off: be_u64(&buf[8..16]),
        }));
    }
    if len < MARK_LEN as u64 {
        return Ok(Loaded::Missing);
    }
    let mut buf = [0u8; MARK_LEN];
    file.read_exact_at(&mut buf, 0)?;
    if crc32(&buf[..32]) != le_u32(&buf[32..36]) {
        return Ok(Loaded::Missing);
    }
    Ok(Loaded::Mark(Mark {
        pos: Pos {
            seg: be_u64(&buf[..8]),
            off: be_u64(&buf[8..16]),
        },
        ordinal: be_u64(&buf[16..24]),
        epoch: be_u64(&buf[24..32]),
    }))
}

fn open_mark_file(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
}

struct Writer {
    /// Shared so a periodic fsync can run after the lock is released.
    file: Arc<File>,
    /// Next append position.
    pos: Pos,
    /// Records appended before `pos`, in the log's epoch.
    ordinal: u64,
    dirty: bool,
    last_sync: Instant,
}

/// An fsync claimed under the writer lock, to run after releasing it.
struct PendingSync {
    file: Arc<File>,
    pos: Pos,
    ordinal: u64,
}

/// The consumer's position; `ordinal` counts the records before it.
struct Head {
    pos: Pos,
    ordinal: u64,
}

pub struct SegmentedLog {
    dir: PathBuf,
    segment_bytes: u64,
    fsync_interval: Duration,
    /// Base shared by every mark this process writes.
    epoch: u64,
    writer: Mutex<Writer>,
    /// Everything before this position is fully written and readable.
    committed: Mutex<Pos>,
    /// Next record to read. Held for the whole of a read batch.
    head: Mutex<Head>,
    head_file: File,
    checkpoint_file: File,
    /// Position in the checkpoint file. Syncs run concurrently and can
    /// finish out of order; this keeps the checkpoint from moving back.
    checkpointed: Mutex<Pos>,
    unread: AtomicU64,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn crc32(data: &[u8]) -> u32 {
    let mut h = Hasher::new();
    h.update(data);
    h.finalize()
}

fn segment_path(dir: &Path, seg: u64) -> PathBuf {
    dir.join(format!("{seg:020}.seg"))
}

/// Unlinks a consumed segment. Failure is logged, not propagated: the head
/// has already moved past it, so a leftover file is only wasted space and
/// is cleaned up on the next open.
fn remove_segment(dir: &Path, seg: u64) {
    if let Err(e) = fs::remove_file(segment_path(dir, seg)) {
        tracing::warn!(
            "queue log {}: failed to unlink segment {seg}: {e}",
            dir.display()
        );
    }
}

fn list_segments(dir: &Path) -> io::Result<Vec<u64>> {
    let mut segs = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("seg") {
            continue;
        }
        if let Some(id) = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u64>().ok())
        {
            segs.push(id);
        }
    }
    segs.sort_unstable();
    Ok(segs)
}

fn encode_record(out: &mut Vec<u8>, payload: &[u8]) {
    #[allow(clippy::cast_possible_truncation)]
    let len = payload.len() as u32;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&crc32(payload).to_le_bytes());
    out.extend_from_slice(payload);
}

enum Parsed<'a> {
    Record {
        payload: &'a [u8],
        total: usize,
    },
    /// The buffer ends before the record does; `need` bytes would hold it.
    Incomplete {
        need: usize,
    },
    Corrupt,
}

fn parse_record(buf: &[u8]) -> Parsed<'_> {
    if buf.len() < RECORD_HEADER {
        return Parsed::Incomplete {
            need: RECORD_HEADER,
        };
    }
    let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    let want = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
    // A zero length with a zero checksum is what a zero-filled region parses
    // as, so empty records are rejected rather than trusted.
    if len == 0 || len > MAX_RECORD {
        return Parsed::Corrupt;
    }
    let total = RECORD_HEADER + len;
    if buf.len() < total {
        return Parsed::Incomplete { need: total };
    }
    let payload = &buf[RECORD_HEADER..total];
    if crc32(payload) != want {
        return Parsed::Corrupt;
    }
    Parsed::Record { payload, total }
}

/// Walks records in `[from, to)` of one segment. Returns the offset just
/// past the last valid record and how many valid records were seen.
fn scan_segment(file: &File, from: u64, to: u64) -> io::Result<(u64, u64)> {
    let mut off = from;
    let mut count = 0u64;
    let mut chunk = READ_CHUNK;
    while off < to {
        let take = (to - off).min(chunk);
        #[allow(clippy::cast_possible_truncation)]
        let mut buf = vec![0u8; take as usize];
        file.read_exact_at(&mut buf, off)?;
        let mut cursor = 0usize;
        loop {
            match parse_record(&buf[cursor..]) {
                Parsed::Record { total, .. } => {
                    cursor += total;
                    count += 1;
                    off += total as u64;
                }
                Parsed::Incomplete { need } => {
                    if off + need as u64 <= to {
                        // Record straddles the chunk boundary: re-read larger.
                        chunk = (need as u64).max(READ_CHUNK);
                        break;
                    }
                    return Ok((off, count));
                }
                Parsed::Corrupt => return Ok((off, count)),
            }
        }
    }
    Ok((off, count))
}

/// Counts the valid records in `[from, tail)`. Every segment in between
/// must exist.
fn count_records(dir: &Path, from: Pos, tail: Pos) -> io::Result<u64> {
    let mut count = 0u64;
    for seg in from.seg..=tail.seg {
        let file = File::open(segment_path(dir, seg))?;
        let start = if seg == from.seg { from.off } else { 0 };
        let end = if seg == tail.seg {
            tail.off
        } else {
            file.metadata()?.len()
        };
        count += scan_segment(&file, start, end)?.1;
    }
    Ok(count)
}

impl SegmentedLog {
    /// Opens or creates the log in `dir`. `segment_bytes` is the size at
    /// which the writer rolls to a new segment; segments may overshoot it by
    /// one record.
    pub fn open(
        dir: impl Into<PathBuf>,
        segment_bytes: u64,
        fsync_interval: Duration,
    ) -> io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        let mut segs = list_segments(&dir)?;
        let head_file = open_mark_file(&dir.join(HEAD_FILE))?;
        let checkpoint_file = open_mark_file(&dir.join(CHECKPOINT_FILE))?;
        let persisted_head = load_mark(&head_file)?;
        let checkpoint = match load_mark(&checkpoint_file)? {
            Loaded::Mark(m) => Some(m),
            Loaded::PosOnly(_) | Loaded::Missing => None,
        };

        // Tail: the last segment, truncated to its last complete record.
        // Everything before the checkpoint was fsynced as whole records, so
        // the torn-tail scan can start there.
        let tail_seg = segs.last().copied().unwrap_or(0);
        if segs.is_empty() {
            segs.push(0);
        }
        let tail_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(segment_path(&dir, tail_seg))?;
        let file_len = tail_file.metadata()?.len();
        let scan_from = checkpoint
            .map(|c| c.pos)
            .filter(|p| p.seg == tail_seg && p.off <= file_len)
            .map_or(0, |p| p.off);
        let (valid_end, _) = scan_segment(&tail_file, scan_from, file_len)?;
        if valid_end < file_len {
            tracing::warn!(
                "queue log {}: truncating torn tail of segment {tail_seg} from {file_len} to {valid_end} bytes",
                dir.display()
            );
            tail_file.set_len(valid_end)?;
            tail_file.sync_data()?;
        }
        let tail = Pos {
            seg: tail_seg,
            off: valid_end,
        };

        // Head: resume from the persisted position, clamped into the range
        // of segments that still exist. A missing or corrupt head file
        // re-delivers from the oldest segment, never skips.
        let first_seg = segs[0];
        let persisted_pos = match persisted_head {
            Loaded::Mark(m) => Some(m.pos),
            Loaded::PosOnly(p) => Some(p),
            Loaded::Missing => None,
        };
        let mut head = match persisted_pos {
            Some(h) if h.seg >= first_seg => h,
            _ => Pos {
                seg: first_seg,
                off: 0,
            },
        };
        if head > tail {
            head = tail;
        }
        // A crash between persisting the head and unlinking the segments
        // behind it leaves them on disk; finish the job.
        for &seg in segs.iter().filter(|&&s| s < head.seg) {
            remove_segment(&dir, seg);
        }

        // Unread count. The head's ordinal is only an anchor if the head is
        // used exactly as persisted; the checkpoint only if it shares the
        // head's epoch and lies within [head, tail]. If the checkpoint is
        // behind the head, everything unread was appended since it and the
        // scan from the head is short. Otherwise rescan and start an epoch.
        let head_anchor = match persisted_head {
            Loaded::Mark(m) if m.pos == head => Some(m),
            _ => None,
        };
        let (epoch, head_ordinal, unread) = if let Some(h) = head_anchor {
            let unread = match checkpoint {
                Some(c)
                    if c.epoch == h.epoch
                        && c.pos >= h.pos
                        && c.pos <= tail
                        && c.ordinal >= h.ordinal =>
                {
                    c.ordinal - h.ordinal + count_records(&dir, c.pos, tail)?
                }
                _ => count_records(&dir, head, tail)?,
            };
            (h.epoch, h.ordinal, unread)
        } else {
            (rand::random(), 0, count_records(&dir, head, tail)?)
        };
        let tail_ordinal = head_ordinal + unread;

        let log = Self {
            dir,
            segment_bytes,
            fsync_interval,
            epoch,
            writer: Mutex::new(Writer {
                file: Arc::new(tail_file),
                pos: tail,
                ordinal: tail_ordinal,
                dirty: false,
                last_sync: Instant::now(),
            }),
            committed: Mutex::new(tail),
            head: Mutex::new(Head {
                pos: head,
                ordinal: head_ordinal,
            }),
            head_file,
            checkpoint_file,
            checkpointed: Mutex::new(Pos { seg: 0, off: 0 }),
            unread: AtomicU64::new(unread),
        };
        log.persist_head(head, head_ordinal)?;
        // A checkpoint from another epoch, or past a truncated tail, would
        // force a full rescan on every open until the next periodic sync.
        match checkpoint {
            Some(c) if c.epoch == epoch && c.pos <= tail => *lock(&log.checkpointed) = c.pos,
            _ => {
                let w = lock(&log.writer);
                // Earlier segments were synced when the writer rolled past them.
                if tail.off > 0 {
                    w.file.sync_data()?;
                }
                log.persist_checkpoint(w.pos, w.ordinal)?;
                drop(w);
            }
        }
        Ok(log)
    }

    fn persist_head(&self, pos: Pos, ordinal: u64) -> io::Result<()> {
        let mark = Mark {
            pos,
            ordinal,
            epoch: self.epoch,
        };
        self.head_file.write_all_at(&mark.encode(), 0)
    }

    /// Records that everything before `pos` is durable. Call only after an
    /// fsync that covered `pos`. An older position than the one already
    /// recorded is ignored.
    fn persist_checkpoint(&self, pos: Pos, ordinal: u64) -> io::Result<()> {
        let mut last = lock(&self.checkpointed);
        if pos < *last {
            return Ok(());
        }
        let mark = Mark {
            pos,
            ordinal,
            epoch: self.epoch,
        };
        self.checkpoint_file.write_all_at(&mark.encode(), 0)?;
        *last = pos;
        drop(last);
        Ok(())
    }

    /// Rolls to a new segment once the current one is full. The old
    /// segment is fsynced under the writer lock, unlike the periodic sync:
    /// nothing may land in the next segment before the previous one is
    /// durable, or a crash could leave a hole in the middle of the log.
    /// Rolls happen once per `segment_bytes`, so the stall is rare.
    fn roll_if_full(&self, w: &mut Writer) -> io::Result<()> {
        if w.pos.off < self.segment_bytes {
            return Ok(());
        }
        w.file.sync_data()?;
        self.persist_checkpoint(w.pos, w.ordinal)?;
        let next = w.pos.seg + 1;
        w.file = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(segment_path(&self.dir, next))?,
        );
        w.pos = Pos { seg: next, off: 0 };
        w.dirty = false;
        w.last_sync = Instant::now();
        Ok(())
    }

    /// Claims the periodic fsync if it is due. The caller runs it with
    /// `run_sync` after releasing the writer lock.
    fn claim_sync_if_due(&self, w: &mut Writer) -> Option<PendingSync> {
        if !w.dirty || w.last_sync.elapsed() < self.fsync_interval {
            return None;
        }
        w.dirty = false;
        w.last_sync = Instant::now();
        Some(PendingSync {
            file: Arc::clone(&w.file),
            pos: w.pos,
            ordinal: w.ordinal,
        })
    }

    fn run_sync(&self, sync: &PendingSync) -> io::Result<()> {
        if let Err(e) = sync.file.sync_data() {
            // Let the next append claim it again. If the writer has rolled
            // since, the roll already synced this segment.
            lock(&self.writer).dirty = true;
            return Err(e);
        }
        self.persist_checkpoint(sync.pos, sync.ordinal)
    }

    /// Appends one record and returns its key.
    pub fn append(&self, payload: &[u8]) -> io::Result<Vec<u8>> {
        let mut record = Vec::with_capacity(RECORD_HEADER + payload.len());
        encode_record(&mut record, payload);
        let mut w = lock(&self.writer);
        self.roll_if_full(&mut w)?;
        let pos = w.pos;
        w.file.write_all_at(&record, pos.off)?;
        w.pos.off += record.len() as u64;
        w.ordinal += 1;
        w.dirty = true;
        let pending = self.claim_sync_if_due(&mut w);
        // Counted before the record becomes readable and under the writer
        // lock, so neither a dequeue nor a recount can see it uncounted.
        self.unread.fetch_add(1, Ordering::Relaxed);
        *lock(&self.committed) = w.pos;
        drop(w);
        if let Some(sync) = pending {
            self.run_sync(&sync)?;
        }
        Ok(pos.to_key())
    }

    /// Reads and consumes up to `limit` records in arrival order.
    pub fn read_batch(&self, limit: usize) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut head = lock(&self.head);
        let end = *lock(&self.committed);
        let (records, new_head, skipped) = self.read_records(head.pos, end, limit)?;
        if new_head == head.pos {
            // Retry a reclaim that an earlier drain skipped.
            if head.pos == end && head.pos.off > 0 {
                self.reclaim_drained(&mut head)?;
            }
            return Ok(records);
        }
        let ordinal = head.ordinal + records.len() as u64;
        self.persist_head(new_head, ordinal)?;
        for seg in head.pos.seg..new_head.seg {
            remove_segment(&self.dir, seg);
        }
        *head = Head {
            pos: new_head,
            ordinal,
        };
        if new_head == end && new_head.off > 0 {
            self.reclaim_drained(&mut head)?;
        }
        if skipped {
            self.recount(&mut head)?;
            return Ok(records);
        }
        drop(head);
        let taken = records.len() as u64;
        let _ = self
            .unread
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(taken))
            });
        Ok(records)
    }

    /// Re-derives the unread count by scanning from the head. After a read
    /// skipped a damaged segment tail, the records lost with it were never
    /// counted as consumed; without this the overcount would persist in the
    /// marks and outlive restarts. Holds the writer for the scan, so appends
    /// stall, but only on this corruption path.
    fn recount(&self, head: &mut Head) -> io::Result<()> {
        let w = lock(&self.writer);
        let unread = count_records(&self.dir, head.pos, w.pos)?;
        head.ordinal = w.ordinal.saturating_sub(unread);
        self.persist_head(head.pos, head.ordinal)?;
        self.unread.store(unread, Ordering::Relaxed);
        drop(w);
        Ok(())
    }

    /// A fully drained log still holds its current segment, every byte of
    /// it dead. Roll the writer to a fresh segment and unlink the old one,
    /// so a drained queue occupies no space. Lock order: head, writer,
    /// committed (append takes writer then committed and never head).
    ///
    /// Only housekeeping, so it never waits for the writer: if an append or
    /// a roll holds it, the next read that finds the log drained retries.
    fn reclaim_drained(&self, head: &mut Head) -> io::Result<()> {
        let mut w = match self.writer.try_lock() {
            Ok(w) => w,
            Err(TryLockError::Poisoned(e)) => e.into_inner(),
            Err(TryLockError::WouldBlock) => return Ok(()),
        };
        if w.pos != head.pos {
            // Something was appended since we read `committed`; not drained.
            return Ok(());
        }
        let old = w.pos.seg;
        let next = Pos {
            seg: old + 1,
            off: 0,
        };
        w.file = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(segment_path(&self.dir, next.seg))?,
        );
        w.pos = next;
        w.dirty = false;
        w.last_sync = Instant::now();
        *lock(&self.committed) = next;
        drop(w);
        self.persist_head(next, head.ordinal)?;
        head.pos = next;
        remove_segment(&self.dir, old);
        Ok(())
    }

    /// Visits every unread record without consuming any. Holds the head
    /// lock for the whole walk, so it is a consistent snapshot and blocks
    /// concurrent dequeues; meant for operator commands, not hot paths.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "the head lock is held for the whole walk so no dequeue can move it"
    )]
    pub fn for_each_unread(&self, mut f: impl FnMut(&[u8])) -> io::Result<()> {
        let head = lock(&self.head);
        let end = *lock(&self.committed);
        let mut pos = head.pos;
        while pos < end {
            let (records, next, _) = self.read_records(pos, end, 4096)?;
            if records.is_empty() || next == pos {
                break;
            }
            for (_, payload) in &records {
                f(payload);
            }
            pos = next;
        }
        Ok(())
    }

    /// Reads up to `limit` records without consuming them.
    pub fn peek(&self, limit: usize) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let head = lock(&self.head).pos;
        let end = *lock(&self.committed);
        Ok(self.read_records(head, end, limit)?.0)
    }

    /// Walks records from `pos` (exclusive of nothing) up to `end`, at most
    /// `limit`. Returns the records, the position to resume from, and
    /// whether a torn or corrupt record made it skip the rest of a segment.
    #[allow(clippy::type_complexity)]
    fn read_records(
        &self,
        mut pos: Pos,
        end: Pos,
        limit: usize,
    ) -> io::Result<(Vec<(Vec<u8>, Vec<u8>)>, Pos, bool)> {
        let mut out = Vec::with_capacity(limit.min(4096));
        let mut skipped = false;
        while out.len() < limit && pos < end {
            let file = File::open(segment_path(&self.dir, pos.seg))?;
            let seg_end = if pos.seg < end.seg {
                file.metadata()?.len()
            } else {
                end.off
            };
            let mut chunk = READ_CHUNK;
            while out.len() < limit && pos.off < seg_end {
                let take = (seg_end - pos.off).min(chunk);
                #[allow(clippy::cast_possible_truncation)]
                let mut buf = vec![0u8; take as usize];
                file.read_exact_at(&mut buf, pos.off)?;
                let mut cursor = 0usize;
                // A chunk used up exactly is not a torn record: the outer
                // loop reads the next chunk or finishes the segment.
                while out.len() < limit && cursor < buf.len() {
                    match parse_record(&buf[cursor..]) {
                        Parsed::Record { payload, total } => {
                            out.push((pos.to_key(), payload.to_vec()));
                            cursor += total;
                            pos.off += total as u64;
                        }
                        Parsed::Incomplete { need } => {
                            if pos.off + need as u64 <= seg_end {
                                chunk = (need as u64).max(READ_CHUNK);
                            } else {
                                tracing::error!(
                                    "queue log {}: torn record at segment {} offset {}; skipping rest of segment",
                                    self.dir.display(),
                                    pos.seg,
                                    pos.off
                                );
                                pos.off = seg_end;
                                skipped = true;
                            }
                            break;
                        }
                        Parsed::Corrupt => {
                            tracing::error!(
                                "queue log {}: corrupt record at segment {} offset {}; skipping rest of segment",
                                self.dir.display(),
                                pos.seg,
                                pos.off
                            );
                            pos.off = seg_end;
                            skipped = true;
                            break;
                        }
                    }
                }
            }
            if pos.off >= seg_end && pos.seg < end.seg {
                pos = Pos {
                    seg: pos.seg + 1,
                    off: 0,
                };
            }
        }
        Ok((out, pos, skipped))
    }

    /// Records appended but not yet consumed. O(1), and exact: a read that
    /// skips a damaged segment tail recounts from the head.
    #[must_use]
    pub fn len(&self) -> usize {
        usize::try_from(self.unread.load(Ordering::Relaxed)).unwrap_or(usize::MAX)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Forces pending appends to disk.
    pub fn sync(&self) -> io::Result<()> {
        let mut w = lock(&self.writer);
        if w.dirty {
            w.file.sync_data()?;
            self.persist_checkpoint(w.pos, w.ordinal)?;
            w.dirty = false;
            w.last_sync = Instant::now();
        }
        drop(w);
        Ok(())
    }
}

impl Drop for SegmentedLog {
    fn drop(&mut self) {
        if let Err(e) = self.sync() {
            tracing::warn!(
                "queue log {}: fsync on drop failed: {e}",
                self.dir.display()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open(dir: &Path, segment_bytes: u64) -> SegmentedLog {
        SegmentedLog::open(dir, segment_bytes, Duration::from_secs(1)).unwrap()
    }

    fn payloads(records: &[(Vec<u8>, Vec<u8>)]) -> Vec<String> {
        records
            .iter()
            .map(|(_, p)| String::from_utf8(p.clone()).unwrap())
            .collect()
    }

    #[test]
    fn roundtrip_in_arrival_order_with_monotonic_keys() {
        let dir = TempDir::new().unwrap();
        let log = open(dir.path(), 1 << 20);
        for i in 0..5 {
            log.append(format!("r{i}").as_bytes()).unwrap();
        }
        assert_eq!(log.len(), 5);
        let batch = log.read_batch(3).unwrap();
        assert_eq!(payloads(&batch), ["r0", "r1", "r2"]);
        assert!(batch[0].0 < batch[1].0 && batch[1].0 < batch[2].0);
        assert_eq!(batch[0].0.len(), 16);
        assert_eq!(log.len(), 2);
        let rest = log.read_batch(10).unwrap();
        assert_eq!(payloads(&rest), ["r3", "r4"]);
        assert!(log.read_batch(10).unwrap().is_empty());
        assert!(log.is_empty());
    }

    #[test]
    fn reopen_resumes_after_persisted_head() {
        let dir = TempDir::new().unwrap();
        {
            let log = open(dir.path(), 1 << 20);
            for i in 0..6 {
                log.append(format!("r{i}").as_bytes()).unwrap();
            }
            assert_eq!(log.read_batch(4).unwrap().len(), 4);
        }
        let log = open(dir.path(), 1 << 20);
        assert_eq!(log.len(), 2);
        assert_eq!(payloads(&log.read_batch(10).unwrap()), ["r4", "r5"]);
        log.append(b"after").unwrap();
        assert_eq!(payloads(&log.read_batch(10).unwrap()), ["after"]);
    }

    #[test]
    fn consumed_segments_are_unlinked() {
        let dir = TempDir::new().unwrap();
        // Every record is 8 + 3 bytes, so a 20-byte segment holds two.
        let log = open(dir.path(), 20);
        for i in 0..9 {
            log.append(format!("r0{i}").as_bytes()).unwrap();
        }
        let segs = list_segments(dir.path()).unwrap();
        assert_eq!(segs.len(), 5, "expected 5 segments, got {segs:?}");

        assert_eq!(log.read_batch(5).unwrap().len(), 5);
        let segs = list_segments(dir.path()).unwrap();
        assert_eq!(segs, [2, 3, 4], "segments behind the head must be gone");

        // Reopen preserves the count and the order across the roll.
        drop(log);
        let log = open(dir.path(), 20);
        assert_eq!(log.len(), 4);
        assert_eq!(
            payloads(&log.read_batch(10).unwrap()),
            ["r05", "r06", "r07", "r08"]
        );
        // Fully drained: the dead segment 4 is unlinked and the writer sits
        // on a fresh, empty segment 5.
        assert_eq!(list_segments(dir.path()).unwrap(), [5]);
    }

    #[test]
    fn torn_tail_is_truncated_on_open() {
        let dir = TempDir::new().unwrap();
        {
            let log = open(dir.path(), 1 << 20);
            log.append(b"good1").unwrap();
            log.append(b"good2").unwrap();
        }
        // Simulate a crash mid-append: a header promising more than exists.
        let seg = segment_path(dir.path(), 0);
        let mut bytes = fs::read(&seg).unwrap();
        bytes.extend_from_slice(&100u32.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(b"partial");
        fs::write(&seg, &bytes).unwrap();

        let log = open(dir.path(), 1 << 20);
        assert_eq!(log.len(), 2);
        log.append(b"good3").unwrap();
        assert_eq!(
            payloads(&log.read_batch(10).unwrap()),
            ["good1", "good2", "good3"]
        );
    }

    #[test]
    fn corrupt_head_file_redelivers_from_start() {
        let dir = TempDir::new().unwrap();
        {
            let log = open(dir.path(), 1 << 20);
            log.append(b"a").unwrap();
            log.append(b"b").unwrap();
            assert_eq!(log.read_batch(1).unwrap().len(), 1);
        }
        fs::write(dir.path().join(HEAD_FILE), b"garbage").unwrap();
        let log = open(dir.path(), 1 << 20);
        assert_eq!(log.len(), 2);
        assert_eq!(payloads(&log.read_batch(10).unwrap()), ["a", "b"]);
    }

    /// Never fsyncs on its own, so the checkpoint only moves on open, roll,
    /// explicit sync and a clean drop.
    fn open_manual_sync(dir: &Path, segment_bytes: u64) -> SegmentedLog {
        SegmentedLog::open(dir, segment_bytes, Duration::from_secs(3600)).unwrap()
    }

    fn corrupt_payload_byte(dir: &Path, seg: u64, off: u64) {
        let path = segment_path(dir, seg);
        let mut bytes = fs::read(&path).unwrap();
        #[allow(clippy::cast_possible_truncation)]
        let at = off as usize + RECORD_HEADER;
        bytes[at] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
    }

    #[test]
    fn reopen_takes_len_from_marks_without_rescanning() {
        let dir = TempDir::new().unwrap();
        // 11-byte records, two per segment.
        {
            let log = open(dir.path(), 20);
            for i in 0..10 {
                log.append(format!("r0{i}").as_bytes()).unwrap();
            }
            assert_eq!(log.read_batch(1).unwrap().len(), 1);
        }
        // Damage a record between head and checkpoint. A rescan would stop
        // counting at it; the marks still say 9.
        corrupt_payload_byte(dir.path(), 2, 0);
        let log = open(dir.path(), 20);
        assert_eq!(log.len(), 9);
    }

    #[test]
    fn crash_counts_appends_since_the_checkpoint() {
        let dir = TempDir::new().unwrap();
        {
            let log = open_manual_sync(dir.path(), 1 << 20);
            for i in 0..5 {
                log.append(format!("r{i}").as_bytes()).unwrap();
            }
            log.sync().unwrap();
            for i in 5..8 {
                log.append(format!("r{i}").as_bytes()).unwrap();
            }
            assert_eq!(log.read_batch(2).unwrap().len(), 2);
            // Crash: no drop, so no final sync or checkpoint.
            std::mem::forget(log);
        }
        let log = open_manual_sync(dir.path(), 1 << 20);
        assert_eq!(log.len(), 6);
        assert_eq!(
            payloads(&log.read_batch(10).unwrap()),
            ["r2", "r3", "r4", "r5", "r6", "r7"]
        );
    }

    #[test]
    fn crash_with_head_past_the_checkpoint() {
        let dir = TempDir::new().unwrap();
        {
            // The checkpoint is written at open and never moves after.
            let log = open_manual_sync(dir.path(), 1 << 20);
            for i in 0..4 {
                log.append(format!("r{i}").as_bytes()).unwrap();
            }
            assert_eq!(log.read_batch(1).unwrap().len(), 1);
            std::mem::forget(log);
        }
        let log = open_manual_sync(dir.path(), 1 << 20);
        assert_eq!(log.len(), 3);
        assert_eq!(payloads(&log.read_batch(10).unwrap()), ["r1", "r2", "r3"]);
        // Drained and reclaimed, then crashed again: nothing unread.
        std::mem::forget(log);
        let log = open_manual_sync(dir.path(), 1 << 20);
        assert_eq!(log.len(), 0);
        log.append(b"next").unwrap();
        assert_eq!(payloads(&log.read_batch(10).unwrap()), ["next"]);
    }

    #[test]
    fn legacy_head_file_resumes_and_rescans() {
        let dir = TempDir::new().unwrap();
        let resume_at;
        {
            let log = open(dir.path(), 1 << 20);
            for i in 0..4 {
                log.append(format!("r{i}").as_bytes()).unwrap();
            }
            resume_at = log.read_batch(1).unwrap()[0].0.clone();
        }
        // The pre-ordinal head format, pointing just past r0.
        let seg = be_u64(&resume_at[..8]);
        let off = be_u64(&resume_at[8..]) + RECORD_HEADER as u64 + 2;
        let mut legacy = [0u8; LEGACY_HEAD_LEN];
        legacy[..8].copy_from_slice(&seg.to_be_bytes());
        legacy[8..16].copy_from_slice(&off.to_be_bytes());
        let check = crc32(&legacy[..16]);
        legacy[16..20].copy_from_slice(&check.to_le_bytes());
        let head_path = dir.path().join(HEAD_FILE);
        fs::write(&head_path, legacy).unwrap();
        fs::remove_file(dir.path().join(CHECKPOINT_FILE)).unwrap();

        let log = open(dir.path(), 1 << 20);
        assert_eq!(log.len(), 3);
        assert_eq!(fs::metadata(&head_path).unwrap().len(), MARK_LEN as u64);
        drop(log);
        let log = open(dir.path(), 1 << 20);
        assert_eq!(log.len(), 3);
        assert_eq!(payloads(&log.read_batch(10).unwrap()), ["r1", "r2", "r3"]);
    }

    fn checkpoint_on_disk(dir: &Path) -> Mark {
        let file = File::open(dir.join(CHECKPOINT_FILE)).unwrap();
        match load_mark(&file).unwrap() {
            Loaded::Mark(m) => m,
            Loaded::PosOnly(_) | Loaded::Missing => panic!("no checkpoint"),
        }
    }

    #[test]
    fn reclaim_does_not_wait_for_the_writer_and_retries() {
        let dir = TempDir::new().unwrap();
        let log = open(dir.path(), 1 << 20);
        log.append(b"a").unwrap();
        log.append(b"b").unwrap();
        {
            // An append or roll in progress.
            let _writer = lock(&log.writer);
            assert_eq!(payloads(&log.read_batch(10).unwrap()), ["a", "b"]);
        }
        assert_eq!(list_segments(dir.path()).unwrap(), [0], "reclaim skipped");
        // The next poll of the drained log finishes the job.
        assert!(log.read_batch(10).unwrap().is_empty());
        assert_eq!(list_segments(dir.path()).unwrap(), [1]);
        log.append(b"c").unwrap();
        assert_eq!(payloads(&log.read_batch(10).unwrap()), ["c"]);
    }

    #[test]
    fn periodic_sync_moves_the_checkpoint() {
        let dir = TempDir::new().unwrap();
        // Every append is due for a sync.
        let log = SegmentedLog::open(dir.path(), 1 << 20, Duration::ZERO).unwrap();
        log.append(b"one").unwrap();
        log.append(b"two").unwrap();
        let c = checkpoint_on_disk(dir.path());
        assert_eq!(c.pos, *lock(&log.committed));
        assert_eq!(c.ordinal, 2);
        assert!(!lock(&log.writer).dirty);
    }

    #[test]
    fn a_late_sync_never_moves_the_checkpoint_back() {
        let dir = TempDir::new().unwrap();
        let log = open_manual_sync(dir.path(), 1 << 20);
        log.append(b"one").unwrap();
        let early = *lock(&log.committed);
        log.append(b"two").unwrap();
        log.sync().unwrap();
        let synced = checkpoint_on_disk(dir.path());
        // A sync claimed before the second append finishing last.
        log.persist_checkpoint(early, 1).unwrap();
        assert_eq!(checkpoint_on_disk(dir.path()), synced);
    }

    #[test]
    fn reading_to_a_segment_end_is_not_a_skip() {
        let dir = TempDir::new().unwrap();
        // 11-byte records, two per segment.
        let log = open(dir.path(), 20);
        for i in 0..3 {
            log.append(format!("r0{i}").as_bytes()).unwrap();
        }
        let start = lock(&log.head).pos;
        let end = *lock(&log.committed);
        // Across the segment boundary and up to the committed end.
        let (records, pos, skipped) = log.read_records(start, end, 10).unwrap();
        assert_eq!(payloads(&records), ["r00", "r01", "r02"]);
        assert_eq!(pos, end);
        assert!(!skipped);
        // Stopping exactly at the end of the first segment.
        let (records, _, skipped) = log.read_records(start, end, 2).unwrap();
        assert_eq!(records.len(), 2);
        assert!(!skipped);
    }

    #[test]
    fn skipping_a_corrupt_segment_tail_recounts() {
        let dir = TempDir::new().unwrap();
        // 11-byte records: three per segment, the third overshooting.
        let log = open_manual_sync(dir.path(), 30);
        for i in 0..6 {
            log.append(format!("r0{i}").as_bytes()).unwrap();
        }
        assert_eq!(log.len(), 6);
        // r01 is damaged: the read skips r01 and r02.
        corrupt_payload_byte(dir.path(), 0, 11);
        assert_eq!(payloads(&log.read_batch(1).unwrap()), ["r00"]);
        assert_eq!(payloads(&log.read_batch(1).unwrap()), ["r03"]);
        assert_eq!(log.len(), 2);
        drop(log);
        let log = open_manual_sync(dir.path(), 30);
        assert_eq!(log.len(), 2);
        assert_eq!(payloads(&log.read_batch(10).unwrap()), ["r04", "r05"]);
    }

    #[test]
    fn peek_does_not_consume() {
        let dir = TempDir::new().unwrap();
        let log = open(dir.path(), 1 << 20);
        log.append(b"x").unwrap();
        log.append(b"y").unwrap();
        assert_eq!(payloads(&log.peek(10).unwrap()), ["x", "y"]);
        assert_eq!(log.len(), 2);
        assert_eq!(payloads(&log.read_batch(1).unwrap()), ["x"]);
        assert_eq!(payloads(&log.peek(10).unwrap()), ["y"]);
    }

    #[test]
    fn records_larger_than_the_read_chunk_are_read_whole() {
        let dir = TempDir::new().unwrap();
        let log = open(dir.path(), 1 << 30);
        let big = vec![7u8; usize::try_from(READ_CHUNK).unwrap() * 2 + 13];
        log.append(b"small").unwrap();
        log.append(&big).unwrap();
        log.append(b"tail").unwrap();
        let batch = log.read_batch(10).unwrap();
        assert_eq!(batch.len(), 3);
        assert_eq!(batch[1].1, big);
        assert_eq!(batch[2].1, b"tail");
    }
}
