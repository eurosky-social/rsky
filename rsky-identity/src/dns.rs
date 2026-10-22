//! One host-name resolver for the whole process.
//!
//! reqwest's built-in hickory resolver lives inside each `reqwest::Client`,
//! and so does its cache: every client built per request (or per
//! `IdResolver`) starts cold and sends its own queries. Wintermute built one
//! per firehose identity/account/sync event, and on juice that came to ~40k
//! queries per second for the single PLC host name. Every transport in this
//! crate resolves through [`shared`] instead, which
//!
//! - keeps one hickory cache, so a name costs one lookup per TTL (at least
//!   [`MIN_POSITIVE_TTL`]);
//! - lets only one lookup per name be in flight, the other callers wait for
//!   its answer, so a cold or expired entry is not fetched once per waiting
//!   connection;
//! - remembers a failed lookup for [`FAILURE_MEMO`], so a resolver outage
//!   (timeouts, SERVFAIL, which hickory does not cache) cannot turn into a
//!   query per request either;
//! - re-reads `/etc/resolv.conf` when it changes (checked at most every
//!   [`RELOAD_CHECK`]), so a host switching resolvers (e.g. from Tailscale's
//!   100.100.100.100 to systemd-resolved) needs no restart.

use hickory_resolver::config::{ResolverConfig, ResolverOpts};
use hickory_resolver::TokioAsyncResolver;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::OnceCell;

/// How long a failed lookup is answered from memory.
pub const FAILURE_MEMO: Duration = Duration::from_secs(1);

/// How often, at most, the resolver configuration file is checked.
pub const RELOAD_CHECK: Duration = Duration::from_secs(5);

/// The shortest time a positive answer is cached, whatever its TTL: a
/// forwarder handing out TTL 0 (or a near-expired TTL from its own cache)
/// would otherwise leave nothing to cache.
pub const MIN_POSITIVE_TTL: Duration = Duration::from_secs(30);

/// Above this many remembered names, finished entries are dropped.
const MAX_FLIGHTS: usize = 4096;

const RESOLV_CONF: &str = "/etc/resolv.conf";

static SHARED: LazyLock<Arc<CachingResolver>> =
    LazyLock::new(|| Arc::new(CachingResolver::system(RESOLV_CONF)));

/// The process-wide resolver.
#[must_use]
pub fn shared() -> Arc<CachingResolver> {
    SHARED.clone()
}

type Answer = Result<Arc<Vec<IpAddr>>, String>;

/// One lookup of a name; everyone asking while it runs shares its answer.
type Flight = Arc<OnceCell<(Instant, Answer)>>;

/// The file a resolver configuration came from, as last seen.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Stamp {
    modified: Option<SystemTime>,
    len: u64,
    #[cfg(unix)]
    inode: u64,
}

impl Stamp {
    fn of(path: &PathBuf) -> Option<Self> {
        // Follows symlinks: /etc/resolv.conf repointed to another file counts.
        let meta = std::fs::metadata(path).ok()?;
        Some(Self {
            modified: meta.modified().ok(),
            len: meta.len(),
            #[cfg(unix)]
            inode: std::os::unix::fs::MetadataExt::ino(&meta),
        })
    }
}

enum Source {
    /// A resolv.conf, re-read when it changes.
    File(PathBuf),
    /// A fixed configuration (tests, or a caller's own servers).
    Fixed,
}

struct Current {
    resolver: TokioAsyncResolver,
    stamp: Option<Stamp>,
    checked: Instant,
    nameservers: Vec<SocketAddr>,
}

/// A caching, coalescing hickory resolver. See the module docs.
pub struct CachingResolver {
    source: Source,
    current: RwLock<Current>,
    flights: Mutex<HashMap<String, Flight>>,
}

impl std::fmt::Debug for CachingResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachingResolver")
            .field("nameservers", &self.nameservers())
            .finish_non_exhaustive()
    }
}

fn build(config: ResolverConfig, mut opts: ResolverOpts) -> (TokioAsyncResolver, Vec<SocketAddr>) {
    opts.positive_min_ttl = Some(
        opts.positive_min_ttl
            .map_or(MIN_POSITIVE_TTL, |t| t.max(MIN_POSITIVE_TTL)),
    );
    let nameservers = config
        .name_servers()
        .iter()
        .map(|ns| ns.socket_addr)
        .collect();
    (TokioAsyncResolver::tokio(config, opts), nameservers)
}

/// The configuration in `path`; hickory's default (Google) only when it
/// cannot be read, as before.
fn read(path: &PathBuf) -> (ResolverConfig, ResolverOpts) {
    #[cfg(unix)]
    {
        if let Ok(parsed) = std::fs::read(path)
            .map_err(|e| e.to_string())
            .and_then(|data| {
                hickory_resolver::system_conf::parse_resolv_conf(data).map_err(|e| e.to_string())
            })
        {
            return parsed;
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        if let Ok(parsed) = hickory_resolver::system_conf::read_system_conf() {
            return parsed;
        }
    }
    (ResolverConfig::default(), ResolverOpts::default())
}

impl CachingResolver {
    /// Resolves with the configuration in `path` (a resolv.conf), reloaded
    /// when the file changes.
    pub fn system(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        let stamp = Stamp::of(&path);
        let (config, opts) = read(&path);
        let (resolver, nameservers) = build(config, opts);
        Self {
            source: Source::File(path),
            current: RwLock::new(Current {
                resolver,
                stamp,
                checked: Instant::now(),
                nameservers,
            }),
            flights: Mutex::new(HashMap::new()),
        }
    }

    /// Resolves with a fixed configuration.
    #[must_use]
    pub fn fixed(config: ResolverConfig, opts: ResolverOpts) -> Self {
        let (resolver, nameservers) = build(config, opts);
        Self {
            source: Source::Fixed,
            current: RwLock::new(Current {
                resolver,
                stamp: None,
                checked: Instant::now(),
                nameservers,
            }),
            flights: Mutex::new(HashMap::new()),
        }
    }

    /// The name servers currently asked.
    #[must_use]
    pub fn nameservers(&self) -> Vec<SocketAddr> {
        self.current
            .read()
            .map(|c| c.nameservers.clone())
            .unwrap_or_default()
    }

    /// Rebuilds the resolver if its configuration file changed; checks at
    /// most every `every`. Returns whether it was rebuilt.
    pub fn reload_if_changed(&self, every: Duration) -> bool {
        let Source::File(path) = &self.source else {
            return false;
        };
        {
            let Ok(current) = self.current.read() else {
                return false;
            };
            if current.checked.elapsed() < every {
                return false;
            }
        }
        let Ok(mut current) = self.current.write() else {
            return false;
        };
        if current.checked.elapsed() < every {
            return false;
        }
        current.checked = Instant::now();
        let stamp = Stamp::of(path);
        if stamp == current.stamp {
            return false;
        }
        let (config, opts) = read(path);
        let (resolver, nameservers) = build(config, opts);
        log_change(&current.nameservers, &nameservers);
        *current = Current {
            resolver,
            stamp,
            checked: Instant::now(),
            nameservers,
        };
        drop(current);
        // Answers (and failures) from the old servers no longer apply.
        if let Ok(mut flights) = self.flights.lock() {
            flights.clear();
        }
        true
    }

    fn resolver(&self) -> TokioAsyncResolver {
        self.reload_if_changed(RELOAD_CHECK);
        // A poisoned lock only means a panic elsewhere mid-swap; the resolver
        // inside is still whole.
        match self.current.read() {
            Ok(c) => c.resolver.clone(),
            Err(poisoned) => poisoned.into_inner().resolver.clone(),
        }
    }

    /// The flight to wait on for `host`: the running or recently failed one,
    /// else a new one.
    fn flight(&self, host: &str) -> Flight {
        let mut flights = match self.flights.lock() {
            Ok(f) => f,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(flight) = flights.get(host) {
            match flight.get() {
                None => return flight.clone(),
                Some((at, Err(_))) if at.elapsed() < FAILURE_MEMO => return flight.clone(),
                Some(_) => {}
            }
        }
        if flights.len() >= MAX_FLIGHTS {
            flights.retain(|_, f| match f.get() {
                None => true,
                Some((at, Err(_))) => at.elapsed() < FAILURE_MEMO,
                Some((_, Ok(_))) => false,
            });
        }
        let flight = Flight::default();
        flights.insert(host.to_owned(), flight.clone());
        flight
    }

    /// The addresses of `host`.
    pub async fn lookup(&self, host: &str) -> Result<Vec<IpAddr>, String> {
        let flight = self.flight(host);
        let (_, answer) = flight
            .get_or_init(|| async {
                let answer = self
                    .resolver()
                    .lookup_ip(host)
                    .await
                    .map(|found| Arc::new(found.iter().collect::<Vec<_>>()))
                    .map_err(|e| e.to_string());
                (Instant::now(), answer)
            })
            .await;
        let answer = answer.clone();
        if answer.is_ok() {
            // A success is hickory's to cache, for as long as its TTL says.
            if let Ok(mut flights) = self.flights.lock() {
                if flights.get(host).is_some_and(|f| Arc::ptr_eq(f, &flight)) {
                    flights.remove(host);
                }
            }
        }
        answer.map(|ips| ips.as_ref().clone())
    }
}

/// To stderr: this crate carries no logging dependency.
fn log_change(old: &[SocketAddr], new: &[SocketAddr]) {
    if old != new {
        eprintln!("rsky-identity: resolver configuration changed, name servers {old:?} -> {new:?}");
    }
}

/// An `Arc<CachingResolver>` as a reqwest resolver, with no address policy:
/// for transports to configured hosts (the PLC directory).
#[derive(Debug, Clone)]
pub struct Handle(pub Arc<CachingResolver>);

impl Resolve for Handle {
    fn resolve(&self, name: Name) -> Resolving {
        let resolver = self.0.clone();
        let host = name.as_str().to_owned();
        Box::pin(async move {
            let addresses = resolver.lookup(&host).await?;
            let addrs: Addrs = Box::new(
                addresses
                    .into_iter()
                    .map(|ip| SocketAddr::new(ip, 0))
                    .collect::<Vec<_>>()
                    .into_iter(),
            );
            Ok(addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::did::plc_resolver::DidPlcResolver;
    use hickory_resolver::config::{NameServerConfigGroup, Protocol};
    use hickory_resolver::proto::op::{Message, MessageType};
    use hickory_resolver::proto::rr::rdata::A;
    use hickory_resolver::proto::rr::{RData, Record, RecordType};
    use std::net::Ipv4Addr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, UdpSocket};

    /// A UDP name server on loopback answering every A query with 127.0.0.1
    /// for `ttl` seconds and every other type with no records, or nothing at
    /// all when `ttl` is `None`. Counts the queries it receives.
    async fn name_server(ttl: Option<u32>) -> (u16, Arc<AtomicUsize>) {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = socket.local_addr().unwrap().port();
        let queries = Arc::new(AtomicUsize::new(0));
        let counter = queries.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            while let Ok((len, from)) = socket.recv_from(&mut buf).await {
                counter.fetch_add(1, Ordering::SeqCst);
                let Some(ttl) = ttl else {
                    continue;
                };
                let Ok(request) = Message::from_vec(&buf[..len]) else {
                    continue;
                };
                let mut response = Message::new();
                response
                    .set_id(request.id())
                    .set_message_type(MessageType::Response)
                    .set_op_code(request.op_code())
                    .set_recursion_desired(true)
                    .set_recursion_available(true);
                for query in request.queries() {
                    response.add_query(query.clone());
                    if query.query_type() == RecordType::A {
                        response.add_answer(Record::from_rdata(
                            query.name().clone(),
                            ttl,
                            RData::A(A(Ipv4Addr::LOCALHOST)),
                        ));
                    }
                }
                let _ = socket.send_to(&response.to_vec().unwrap(), from).await;
            }
        });
        (port, queries)
    }

    fn resolver_for(port: u16, timeout: Duration) -> Arc<CachingResolver> {
        let servers =
            NameServerConfigGroup::from_ips_clear(&[IpAddr::V4(Ipv4Addr::LOCALHOST)], port, true);
        let servers: Vec<_> = servers
            .into_inner()
            .into_iter()
            .filter(|ns| ns.protocol == Protocol::Udp)
            .collect();
        let config = ResolverConfig::from_parts(None, vec![], servers);
        let mut opts = ResolverOpts::default();
        opts.timeout = timeout;
        opts.attempts = 1;
        opts.use_hosts_file = false;
        Arc::new(CachingResolver::fixed(config, opts))
    }

    #[tokio::test]
    async fn concurrent_and_repeated_lookups_cost_one_query() {
        let (port, queries) = name_server(Some(3600)).await;
        let resolver = resolver_for(port, Duration::from_secs(2));

        let cold: Vec<_> = (0..200)
            .map(|_| {
                let r = resolver.clone();
                tokio::spawn(async move { r.lookup("plc.example.test.").await })
            })
            .collect();
        for task in cold {
            assert_eq!(
                task.await.unwrap().unwrap(),
                vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]
            );
        }
        for _ in 0..200 {
            resolver.lookup("plc.example.test.").await.unwrap();
        }
        assert_eq!(queries.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_zero_ttl_answer_is_still_cached() {
        let (port, queries) = name_server(Some(0)).await;
        let resolver = resolver_for(port, Duration::from_secs(2));
        for _ in 0..100 {
            resolver.lookup("plc.example.test.").await.unwrap();
        }
        assert_eq!(queries.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_failure_is_remembered_briefly() {
        let (port, queries) = name_server(None).await;
        let resolver = resolver_for(port, Duration::from_millis(200));

        let tasks: Vec<_> = (0..100)
            .map(|_| {
                let r = resolver.clone();
                tokio::spawn(async move { r.lookup("down.example.test.").await })
            })
            .collect();
        for task in tasks {
            assert!(task.await.unwrap().is_err());
        }
        for _ in 0..100 {
            assert!(resolver.lookup("down.example.test.").await.is_err());
        }
        // One lookup's worth of packets (hickory asks A, then AAAA), not 200.
        let one_lookup = queries.load(Ordering::SeqCst);
        assert!((1..=4).contains(&one_lookup), "{one_lookup} queries");

        tokio::time::sleep(FAILURE_MEMO).await;
        assert!(resolver.lookup("down.example.test.").await.is_err());
        assert_eq!(queries.load(Ordering::SeqCst), 2 * one_lookup);
    }

    #[test]
    fn a_replaced_resolv_conf_is_picked_up() {
        let dir = std::env::temp_dir().join(format!("rsky-dns-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("resolv.conf");
        std::fs::write(&path, "nameserver 100.100.100.100\n").unwrap();
        let resolver = CachingResolver::system(&path);
        assert_eq!(
            resolver.nameservers()[0].ip().to_string(),
            "100.100.100.100"
        );
        assert!(!resolver.reload_if_changed(Duration::ZERO));

        // Replaced the way resolved and tailscale do it: a new file renamed
        // over the old one.
        let next = dir.join("resolv.conf.new");
        std::fs::write(&next, "nameserver 127.0.0.53\n").unwrap();
        std::fs::rename(&next, &path).unwrap();
        assert!(!resolver.reload_if_changed(Duration::from_secs(3600)));
        assert!(resolver.reload_if_changed(Duration::ZERO));
        assert_eq!(resolver.nameservers()[0].ip().to_string(), "127.0.0.53");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A PLC directory on loopback that closes every connection after one
    /// response, so every request needs a new connection and a resolution.
    async fn plc_directory() -> (u16, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let connections = Arc::new(AtomicUsize::new(0));
        let counter = connections.clone();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let _ = socket.read(&mut buf).await;
                    let body = r#"{"id":"did:plc:test"}"#;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.shutdown().await;
                });
            }
        });
        (port, connections)
    }

    /// The juice failure mode: a PLC resolver built per event, every request
    /// on a new connection. All of them share one cache, so the directory's
    /// name is looked up once.
    #[tokio::test]
    async fn plc_resolvers_built_per_event_share_one_lookup() {
        let (dns_port, queries) = name_server(Some(3600)).await;
        let (plc_port, connections) = plc_directory().await;
        let dns = resolver_for(dns_port, Duration::from_secs(2));
        let plc_url = format!("http://plc.example.test:{plc_port}");

        let shared =
            DidPlcResolver::with_dns(plc_url.clone(), Duration::from_secs(5), None, dns.clone());
        for _ in 0..50 {
            assert!(shared
                .resolve_no_check("did:plc:test".into())
                .await
                .unwrap()
                .is_some());
        }
        let per_event: Vec<_> = (0..50)
            .map(|_| {
                let plc = DidPlcResolver::with_dns(
                    plc_url.clone(),
                    Duration::from_secs(5),
                    None,
                    dns.clone(),
                );
                tokio::spawn(async move { plc.resolve_no_check("did:plc:test".into()).await })
            })
            .collect();
        for task in per_event {
            assert!(task.await.unwrap().unwrap().is_some());
        }

        assert_eq!(connections.load(Ordering::SeqCst), 100);
        assert_eq!(queries.load(Ordering::SeqCst), 1);
    }
}
