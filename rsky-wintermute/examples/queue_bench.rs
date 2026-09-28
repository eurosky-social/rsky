//! Throughput/latency benchmark for the segmented-log queues in `Storage`.
//!
//! cargo run --release -p rsky-wintermute --example queue_bench -- <scenario> [args]
//!
//! Scenarios:
//!   live <secs> <producers>        max-rate enqueue_firehose_live + one drain loop
//!   paced <secs> <rate>            fixed-rate live enqueue, end-to-end latency
//!   backfill <secs> <producers>    enqueue_firehose_backfill_batch + 4 partitioned workers
//!   backlog <jobs>                 fill firehose_live, reopen, drain
#![allow(clippy::unwrap_used, clippy::print_stdout, clippy::cast_precision_loss)]

use rsky_wintermute::storage::Storage;
use rsky_wintermute::types::{IndexJob, WriteAction};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DRAIN_BATCH: usize = 2000;
const BACKFILL_BATCH: usize = 1000;
const BACKFILL_WORKERS: usize = 4;

fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

/// A job shaped like real firehose traffic: mostly likes, then follows,
/// reposts and posts. `indexed_at` carries the enqueue time in ns.
fn job(i: u64) -> IndexJob {
    let did = format!(
        "did:plc:{:024x}",
        i.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 8
    );
    let subject = serde_json::json!({
        "uri": format!("at://did:plc:{:024x}/app.bsky.feed.post/3l{:011x}", i ^ 0x5555, i),
        "cid": "bafyreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy",
    });
    let (coll, record) = match i % 20 {
        0..=10 => (
            "app.bsky.feed.like",
            serde_json::json!({
            "$type": "app.bsky.feed.like", "subject": subject,
            "createdAt": "2026-09-28T12:00:00.000Z"}),
        ),
        11..=14 => (
            "app.bsky.graph.follow",
            serde_json::json!({
            "$type": "app.bsky.graph.follow",
            "subject": format!("did:plc:{:024x}", i ^ 0xabcdef),
            "createdAt": "2026-09-28T12:00:00.000Z"}),
        ),
        15..=16 => (
            "app.bsky.feed.repost",
            serde_json::json!({
            "$type": "app.bsky.feed.repost", "subject": subject,
            "createdAt": "2026-09-28T12:00:00.000Z"}),
        ),
        _ => (
            "app.bsky.feed.post",
            serde_json::json!({
            "$type": "app.bsky.feed.post",
            "text": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam #bsky",
            "langs": ["en"],
            "facets": [{"index": {"byteStart": 150, "byteEnd": 155},
                        "features": [{"$type": "app.bsky.richtext.facet#tag", "tag": "bsky"}]}],
            "reply": {"root": subject, "parent": subject},
            "createdAt": "2026-09-28T12:00:00.000Z"}),
        ),
    };
    IndexJob {
        uri: format!("at://{did}/{coll}/3l{i:011x}"),
        cid: "bafyreib2rxk3rh6kzwq2qvhlmdyfb7wxkmqzmn4lcu3yvbgxz5y4vwa7ey".to_owned(),
        action: WriteAction::Create,
        record: Some(record),
        indexed_at: now_ns().to_string(),
        rev: format!("3l{i:011x}"),
        provenance: None,
    }
}

struct Hist(Vec<u64>);
impl Hist {
    fn report(mut self, name: &str) {
        if self.0.is_empty() {
            return;
        }
        self.0.sort_unstable();
        let q = |p: f64| {
            let idx = ((self.0.len() - 1) as f64 * p) as usize;
            fmt_ns(self.0[idx])
        };
        println!(
            "  {name}: p50 {}  p99 {}  p99.9 {}  max {}",
            q(0.5),
            q(0.99),
            q(0.999),
            fmt_ns(*self.0.last().unwrap())
        );
    }
}

fn fmt_ns(ns: u64) -> String {
    if ns >= 1_000_000 {
        format!("{:.1}ms", ns as f64 / 1e6)
    } else {
        format!("{:.1}µs", ns as f64 / 1e3)
    }
}

fn dir_size(p: &std::path::Path) -> u64 {
    let mut total = 0;
    for e in std::fs::read_dir(p).unwrap().flatten() {
        let m = e.metadata().unwrap();
        total += if m.is_dir() {
            dir_size(&e.path())
        } else {
            m.len()
        };
    }
    total
}

fn open(dir: &std::path::Path) -> Arc<Storage> {
    let t = Instant::now();
    let s = Arc::new(Storage::new(Some(dir.to_path_buf())).unwrap());
    println!("  open: {:?}", t.elapsed());
    s
}

fn encoded_size() -> f64 {
    let n = 1000;
    let mut total = 0;
    for i in 0..n {
        let mut out = Vec::new();
        ciborium::into_writer(&job(i), &mut out).unwrap();
        total += out.len();
    }
    total as f64 / n as f64
}

/// One drain loop, like `process_firehose_live_loop`. Returns (jobs, e2e hist).
fn spawn_live_drain(
    s: Arc<Storage>,
    stop: Arc<AtomicBool>,
) -> thread::JoinHandle<(u64, Hist, Hist)> {
    thread::spawn(move || {
        let mut n = 0u64;
        let mut e2e = Vec::new();
        let mut deq = Vec::new();
        loop {
            let t = Instant::now();
            let batch = s.dequeue_firehose_live_batch(DRAIN_BATCH).unwrap();
            deq.push(t.elapsed().as_nanos() as u64);
            let now = now_ns();
            for (_, j) in &batch {
                let at: u128 = j.indexed_at.parse().unwrap();
                e2e.push(now.saturating_sub(at) as u64);
            }
            n += batch.len() as u64;
            if batch.is_empty() {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                thread::sleep(Duration::from_micros(200));
            }
        }
        (n, Hist(deq), Hist(e2e))
    })
}

fn live(secs: u64, producers: usize) {
    let dir =
        tempfile::tempdir_in(std::env::var("BENCH_DIR").unwrap_or_else(|_| ".".into())).unwrap();
    let s = open(dir.path());
    let stop = Arc::new(AtomicBool::new(false));
    let drain = spawn_live_drain(Arc::clone(&s), Arc::clone(&stop));
    let deadline = Instant::now() + Duration::from_secs(secs);
    let start = Instant::now();
    let handles: Vec<_> = (0..producers)
        .map(|p| {
            let s = Arc::clone(&s);
            thread::spawn(move || {
                let mut lat = Vec::new();
                let mut i = p as u64 * 1_000_000_000;
                while Instant::now() < deadline {
                    let j = job(i);
                    let t = Instant::now();
                    s.enqueue_firehose_live(&j).unwrap();
                    lat.push(t.elapsed().as_nanos() as u64);
                    i += 1;
                }
                lat
            })
        })
        .collect();
    let mut lat = Vec::new();
    for h in handles {
        lat.extend(h.join().unwrap());
    }
    let produced = lat.len() as u64;
    let prod_elapsed = start.elapsed();
    stop.store(true, Ordering::Relaxed);
    let (consumed, deq, e2e) = drain.join().unwrap();
    let total = start.elapsed();
    println!(
        "  produced {produced} in {prod_elapsed:.1?} = {:.0}/s ({producers} producers)",
        produced as f64 / prod_elapsed.as_secs_f64()
    );
    println!(
        "  consumed {consumed} in {total:.1?} = {:.0}/s (1 drain loop, batch {DRAIN_BATCH})",
        consumed as f64 / total.as_secs_f64()
    );
    Hist(lat).report("enqueue");
    deq.report("dequeue batch");
    e2e.report("enqueue->dequeue");
}

fn paced(secs: u64, rate: u64) {
    let dir =
        tempfile::tempdir_in(std::env::var("BENCH_DIR").unwrap_or_else(|_| ".".into())).unwrap();
    let s = open(dir.path());
    let stop = Arc::new(AtomicBool::new(false));
    let drain = spawn_live_drain(Arc::clone(&s), Arc::clone(&stop));
    let start = Instant::now();
    let total = secs * rate;
    let mut lat = Vec::with_capacity(total as usize);
    for i in 0..total {
        let due = start + Duration::from_nanos(i * 1_000_000_000 / rate);
        while Instant::now() < due {
            std::hint::spin_loop();
        }
        let j = job(i);
        let t = Instant::now();
        s.enqueue_firehose_live(&j).unwrap();
        lat.push(t.elapsed().as_nanos() as u64);
    }
    stop.store(true, Ordering::Relaxed);
    let (consumed, deq, e2e) = drain.join().unwrap();
    println!("  {total} jobs at {rate}/s, consumed {consumed}");
    Hist(lat).report("enqueue");
    deq.report("dequeue batch");
    e2e.report("enqueue->dequeue");
}

fn backfill(secs: u64, producers: usize) {
    let dir =
        tempfile::tempdir_in(std::env::var("BENCH_DIR").unwrap_or_else(|_| ".".into())).unwrap();
    let s = open(dir.path());
    let stop = Arc::new(AtomicBool::new(false));
    let consumed = Arc::new(AtomicU64::new(0));
    let workers: Vec<_> = (0..BACKFILL_WORKERS)
        .map(|w| {
            let (s, stop, consumed) = (Arc::clone(&s), Arc::clone(&stop), Arc::clone(&consumed));
            thread::spawn(move || {
                let mut deq = Vec::new();
                loop {
                    let t = Instant::now();
                    let b = s
                        .dequeue_firehose_backfill_partitioned(w, BACKFILL_WORKERS, BACKFILL_BATCH)
                        .unwrap();
                    deq.push(t.elapsed().as_nanos() as u64);
                    consumed.fetch_add(b.len() as u64, Ordering::Relaxed);
                    if b.is_empty() {
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        thread::sleep(Duration::from_micros(500));
                    }
                }
                deq
            })
        })
        .collect();
    // Pre-build the batches so the producers measure storage, not JSON.
    let batch: Vec<IndexJob> = (0..BACKFILL_BATCH as u64).map(job).collect();
    let batch = Arc::new(batch);
    let barrier = Arc::new(Barrier::new(producers));
    let deadline = Instant::now() + Duration::from_secs(secs);
    let start = Instant::now();
    let handles: Vec<_> = (0..producers)
        .map(|_| {
            let (s, batch, barrier) = (Arc::clone(&s), Arc::clone(&batch), Arc::clone(&barrier));
            thread::spawn(move || {
                barrier.wait();
                let mut lat = Vec::new();
                while Instant::now() < deadline {
                    let t = Instant::now();
                    s.enqueue_firehose_backfill_batch(&batch).unwrap();
                    lat.push(t.elapsed().as_nanos() as u64);
                }
                lat
            })
        })
        .collect();
    let mut lat = Vec::new();
    for h in handles {
        lat.extend(h.join().unwrap());
    }
    let produced = lat.len() as u64 * BACKFILL_BATCH as u64;
    let prod_elapsed = start.elapsed();
    let at_stop = consumed.load(Ordering::Relaxed);
    stop.store(true, Ordering::Relaxed);
    let mut deq = Vec::new();
    for w in workers {
        deq.extend(w.join().unwrap());
    }
    let total = start.elapsed();
    println!(
        "  produced {produced} in {prod_elapsed:.1?} = {:.0}/s ({producers} producers, batch {BACKFILL_BATCH})",
        produced as f64 / prod_elapsed.as_secs_f64()
    );
    println!(
        "  consumed {at_stop} while producing = {:.0}/s; drained all {} in {total:.1?} = {:.0}/s ({BACKFILL_WORKERS} workers)",
        at_stop as f64 / prod_elapsed.as_secs_f64(),
        consumed.load(Ordering::Relaxed),
        consumed.load(Ordering::Relaxed) as f64 / total.as_secs_f64()
    );
    Hist(lat).report("enqueue batch");
    Hist(deq).report("dequeue batch");
}

fn backlog(jobs: u64) {
    let dir =
        tempfile::tempdir_in(std::env::var("BENCH_DIR").unwrap_or_else(|_| ".".into())).unwrap();
    {
        let s = open(dir.path());
        let t = Instant::now();
        for i in 0..jobs {
            s.enqueue_firehose_live(&job(i)).unwrap();
        }
        let el = t.elapsed();
        println!(
            "  filled {jobs} in {el:.1?} = {:.0}/s, {:.2} GB on disk",
            jobs as f64 / el.as_secs_f64(),
            dir_size(dir.path()) as f64 / 1e9
        );
    }
    let s = open(dir.path());
    println!("  len after reopen: {}", s.firehose_live_len().unwrap());
    let t = Instant::now();
    let mut n = 0u64;
    let mut deq = Vec::new();
    loop {
        let d = Instant::now();
        let b = s.dequeue_firehose_live_batch(DRAIN_BATCH).unwrap();
        deq.push(d.elapsed().as_nanos() as u64);
        if b.is_empty() {
            break;
        }
        n += b.len() as u64;
    }
    let el = t.elapsed();
    println!(
        "  drained {n} in {el:.1?} = {:.0}/s, {:.2} MB left on disk",
        n as f64 / el.as_secs_f64(),
        dir_size(dir.path()) as f64 / 1e6
    );
    Hist(deq).report("dequeue batch");
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let arg = |i: usize, d: u64| args.get(i).and_then(|s| s.parse().ok()).unwrap_or(d);
    println!("avg encoded job: {:.0} bytes", encoded_size());
    match args.first().map(String::as_str) {
        Some("live") => live(arg(1, 10), arg(2, 1) as usize),
        Some("paced") => paced(arg(1, 10), arg(2, 5000)),
        Some("backfill") => backfill(arg(1, 10), arg(2, 32) as usize),
        Some("backlog") => backlog(arg(1, 5_000_000)),
        _ => println!("usage: queue_bench live|paced|backfill|backlog [args]"),
    }
}
