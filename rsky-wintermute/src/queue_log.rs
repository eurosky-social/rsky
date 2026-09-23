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
//! head                          [u64 seg][u64 off][u32 crc32][u32 zero]
//! ```
//!
//! Durability: appends are fsynced at most once per `fsync_interval`, on
//! segment roll and on drop. The head is rewritten after every read batch
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
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

const RECORD_HEADER: usize = 8;
/// Anything larger is treated as corruption rather than allocated.
const MAX_RECORD: usize = 64 * 1024 * 1024;
const HEAD_FILE: &str = "head";
const HEAD_LEN: usize = 24;
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

struct Writer {
    file: File,
    /// Next append position.
    pos: Pos,
    dirty: bool,
    last_sync: Instant,
}

pub struct SegmentedLog {
    dir: PathBuf,
    segment_bytes: u64,
    fsync_interval: Duration,
    writer: Mutex<Writer>,
    /// Everything before this position is fully written and readable.
    committed: Mutex<Pos>,
    /// Next record to read. Held for the whole of a read batch.
    head: Mutex<Pos>,
    head_file: File,
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

fn load_head(file: &File) -> io::Result<Option<Pos>> {
    let mut buf = [0u8; HEAD_LEN];
    if file.metadata()?.len() < HEAD_LEN as u64 {
        return Ok(None);
    }
    file.read_exact_at(&mut buf, 0)?;
    let want = u32::from_le_bytes([buf[16], buf[17], buf[18], buf[19]]);
    if crc32(&buf[..16]) != want {
        return Ok(None);
    }
    let seg = u64::from_be_bytes(buf[..8].try_into().unwrap_or([0; 8]));
    let off = u64::from_be_bytes(buf[8..16].try_into().unwrap_or([0; 8]));
    Ok(Some(Pos { seg, off }))
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
        let head_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join(HEAD_FILE))?;
        let persisted_head = load_head(&head_file)?;

        // Tail: the last segment, truncated to its last complete record.
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
        let (valid_end, _) = scan_segment(&tail_file, 0, file_len)?;
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
        let mut head = match persisted_head {
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

        let mut unread = 0u64;
        for &seg in segs.iter().filter(|&&s| s >= head.seg) {
            let file = File::open(segment_path(&dir, seg))?;
            let from = if seg == head.seg { head.off } else { 0 };
            let to = if seg == tail.seg {
                tail.off
            } else {
                file.metadata()?.len()
            };
            let (_, count) = scan_segment(&file, from, to)?;
            unread += count;
        }

        let log = Self {
            dir,
            segment_bytes,
            fsync_interval,
            writer: Mutex::new(Writer {
                file: tail_file,
                pos: tail,
                dirty: false,
                last_sync: Instant::now(),
            }),
            committed: Mutex::new(tail),
            head: Mutex::new(head),
            head_file,
            unread: AtomicU64::new(unread),
        };
        log.persist_head(head)?;
        Ok(log)
    }

    fn persist_head(&self, pos: Pos) -> io::Result<()> {
        let mut buf = [0u8; HEAD_LEN];
        buf[..8].copy_from_slice(&pos.seg.to_be_bytes());
        buf[8..16].copy_from_slice(&pos.off.to_be_bytes());
        let check = crc32(&buf[..16]);
        buf[16..20].copy_from_slice(&check.to_le_bytes());
        self.head_file.write_all_at(&buf, 0)
    }

    fn roll_if_full(&self, w: &mut Writer) -> io::Result<()> {
        if w.pos.off < self.segment_bytes {
            return Ok(());
        }
        w.file.sync_data()?;
        let next = w.pos.seg + 1;
        w.file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(segment_path(&self.dir, next))?;
        w.pos = Pos { seg: next, off: 0 };
        w.dirty = false;
        w.last_sync = Instant::now();
        Ok(())
    }

    fn sync_if_due(&self, w: &mut Writer) -> io::Result<()> {
        if w.dirty && w.last_sync.elapsed() >= self.fsync_interval {
            w.file.sync_data()?;
            w.dirty = false;
            w.last_sync = Instant::now();
        }
        Ok(())
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
        w.dirty = true;
        self.sync_if_due(&mut w)?;
        *lock(&self.committed) = w.pos;
        drop(w);
        self.unread.fetch_add(1, Ordering::Relaxed);
        Ok(pos.to_key())
    }

    /// Reads and consumes up to `limit` records in arrival order.
    pub fn read_batch(&self, limit: usize) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut head = lock(&self.head);
        let end = *lock(&self.committed);
        let (records, new_head) = self.read_records(*head, end, limit)?;
        if new_head == *head {
            return Ok(records);
        }
        self.persist_head(new_head)?;
        for seg in head.seg..new_head.seg {
            remove_segment(&self.dir, seg);
        }
        *head = new_head;
        if new_head == end && new_head.off > 0 {
            self.reclaim_drained(&mut head)?;
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

    /// A fully drained log still holds its current segment, every byte of
    /// it dead. Roll the writer to a fresh segment and unlink the old one,
    /// so a drained queue occupies no space. Lock order: head, writer,
    /// committed (append takes writer then committed and never head).
    fn reclaim_drained(&self, head: &mut Pos) -> io::Result<()> {
        let mut w = lock(&self.writer);
        if w.pos != *head {
            // Something was appended since we read `committed`; not drained.
            return Ok(());
        }
        let old = w.pos.seg;
        let next = Pos {
            seg: old + 1,
            off: 0,
        };
        w.file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(segment_path(&self.dir, next.seg))?;
        w.pos = next;
        w.dirty = false;
        w.last_sync = Instant::now();
        *lock(&self.committed) = next;
        drop(w);
        self.persist_head(next)?;
        *head = next;
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
        let mut pos = *head;
        while pos < end {
            let (records, next) = self.read_records(pos, end, 4096)?;
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
        let head = *lock(&self.head);
        let end = *lock(&self.committed);
        Ok(self.read_records(head, end, limit)?.0)
    }

    /// Walks records from `pos` (exclusive of nothing) up to `end`, at most
    /// `limit`. Returns the records and the position to resume from.
    #[allow(clippy::type_complexity)]
    fn read_records(
        &self,
        mut pos: Pos,
        end: Pos,
        limit: usize,
    ) -> io::Result<(Vec<(Vec<u8>, Vec<u8>)>, Pos)> {
        let mut out = Vec::with_capacity(limit.min(4096));
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
                while out.len() < limit {
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
        Ok((out, pos))
    }

    /// Records appended but not yet consumed. Exact and O(1).
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
        assert_eq!(payloads(&log.read_batch(10).unwrap()), ["a", "b"]);
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
