use crate::safe_fetch::{FetchError, NetworkPolicy, Redirects, SafeClient};
use crate::types::HandleResolverOpts;
use anyhow::Result;
use hickory_resolver::config::*;
use hickory_resolver::error::ResolveErrorKind;
use hickory_resolver::proto::op::ResponseCode;
use hickory_resolver::TokioAsyncResolver;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use url::Url;

pub const SUBDOMAIN: &str = "_atproto";
pub const PREFIX: &str = "did=";

/// The most a well-known handle document may be.
const WELL_KNOWN_LIMIT: usize = 8 * 1024;

/// What resolving a handle established. Unlike [`HandleResolver::resolve`],
/// which folds every failure into "no DID", this separates a handle that
/// verifiably points nowhere from one that could not be checked right now, so
/// callers can refuse to discard a handle on a timeout, a 429 or a 5xx.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HandleResolution {
    /// The handle claims this DID.
    Found(String),
    /// Every method answered definitively (NXDOMAIN or no `_atproto` record,
    /// well-known 404/410 or not a DID) and none claims a DID.
    NotFound,
    /// No method found a DID and at least one failed in a way that may be
    /// temporary; the reason is for logs. Retry later.
    Unavailable(String),
}

/// One method's answer.
enum Probe {
    Found(String),
    Absent,
    Unavailable(String),
}

/// Transport failures that mean the handle's host does not exist, as opposed
/// to one that could not be reached right now.
const NO_SUCH_HOST: [&str; 4] = [
    "no record found",
    "No address associated with hostname",
    "Name or service not known",
    "nodename nor servname provided",
];

fn host_does_not_exist(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut source = Some(err);
    while let Some(current) = source {
        let message = current.to_string();
        if NO_SUCH_HOST.iter().any(|m| message.contains(m)) {
            return true;
        }
        source = current.source();
    }
    false
}

/// A well-known response, classified: only 404/410 and a successful response
/// that is not a DID are definitive; 429, 5xx and other statuses may pass.
fn classify_well_known(status: reqwest::StatusCode, body: &[u8]) -> Probe {
    if status.is_success() {
        let text = String::from_utf8_lossy(body);
        let first = text.lines().next().unwrap_or("").trim();
        return if first.starts_with("did:") {
            Probe::Found(first.to_owned())
        } else {
            Probe::Absent
        };
    }
    match status.as_u16() {
        404 | 410 => Probe::Absent,
        code => Probe::Unavailable(format!("well-known HTTP {code}")),
    }
}

#[derive(Clone)]
pub struct HandleResolver {
    pub timeout: Duration,
    backup_nameservers: Option<Vec<String>>,
    backup_nameserver_ips: Option<Vec<IpAddr>>,
    /// The transport for the well-known lookup, bound to a network policy.
    client: SafeClient,
    /// The `_atproto` TXT resolver, built once and shared by every clone (it
    /// is reference-counted inside), so lookups share one cache.
    dns: TokioAsyncResolver,
}

impl std::fmt::Debug for HandleResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandleResolver")
            .field("timeout", &self.timeout)
            .field("backup_nameservers", &self.backup_nameservers)
            .field("backup_nameserver_ips", &self.backup_nameserver_ips)
            .field("client", &self.client)
            .finish_non_exhaustive()
    }
}

/// The host's resolver (resolv.conf), not a hardcoded public one: hosts often
/// cannot reach 8.8.8.8 directly (egress filtering, or a local forwarder such
/// as systemd-resolved or Tailscale's), and then every TXT lookup burns the
/// whole timeout before the well-known fallback runs, and handles proven only
/// by DNS (e.g. Bridgy Fed's) fail verification outright. hickory's default
/// (Google) is kept only for hosts whose system config cannot be read.
fn dns_resolver(timeout: Duration) -> TokioAsyncResolver {
    let (config, mut opts) = hickory_resolver::system_conf::read_system_conf()
        .unwrap_or_else(|_| (ResolverConfig::default(), ResolverOpts::default()));
    opts.timeout = timeout;
    TokioAsyncResolver::tokio(config, opts)
}

impl HandleResolver {
    pub fn new(opts: HandleResolverOpts) -> Self {
        let timeout = opts.timeout.unwrap_or(Duration::from_millis(3000));
        Self {
            timeout,
            backup_nameservers: opts.backup_nameservers,
            backup_nameserver_ips: None,
            client: SafeClient::new(NetworkPolicy::PUBLIC, timeout).expect("reqwest client"),
            dns: dns_resolver(timeout),
        }
    }

    /// Resolves well-known documents under `policy` instead of the public
    /// default.
    pub fn with_network(mut self, policy: NetworkPolicy) -> Self {
        self.client = SafeClient::new(policy, self.timeout).expect("reqwest client");
        self
    }

    pub async fn resolve(&mut self, handle: &String) -> Result<Option<String>> {
        // Try DNS first
        if let Ok(Some(did)) = self.resolve_dns(handle).await {
            return Ok(Some(did));
        }

        // Fall back to HTTP (/.well-known/atproto-did)
        if let Ok(Some(did)) = self.resolve_http(handle).await {
            return Ok(Some(did));
        }

        // Last resort: backup DNS nameservers
        self.resolve_backup_dns(handle).await
    }

    /// Resolves `handle`, telling a definitive miss from a transient failure.
    /// Same order as [`resolve`](Self::resolve): DNS, well-known, backup DNS.
    pub async fn resolve_outcome(&mut self, handle: &String) -> HandleResolution {
        let dns = self.probe_dns(handle).await;
        if let Probe::Found(did) = dns {
            return HandleResolution::Found(did);
        }
        let http = self.probe_http(handle).await;
        if let Probe::Found(did) = http {
            return HandleResolution::Found(did);
        }
        if let Ok(Some(did)) = self.resolve_backup_dns(handle).await {
            return HandleResolution::Found(did);
        }
        let reasons: Vec<String> = [dns, http]
            .into_iter()
            .filter_map(|p| match p {
                Probe::Unavailable(reason) => Some(reason),
                _ => None,
            })
            .collect();
        if reasons.is_empty() {
            HandleResolution::NotFound
        } else {
            HandleResolution::Unavailable(reasons.join("; "))
        }
    }

    async fn probe_dns(&self, handle: &String) -> Probe {
        match self.dns.txt_lookup(format!("{SUBDOMAIN}.{handle}")).await {
            Ok(records) => {
                let records = records.iter().map(|r| r.to_string()).collect();
                match self.parse_dns_result(records) {
                    Ok(Some(did)) => Probe::Found(did),
                    _ => Probe::Absent,
                }
            }
            Err(e) => match e.kind() {
                ResolveErrorKind::NoRecordsFound {
                    response_code: ResponseCode::NXDomain | ResponseCode::NoError,
                    ..
                } => Probe::Absent,
                _ => Probe::Unavailable(format!("dns: {e}")),
            },
        }
    }

    async fn probe_http(&self, handle: &String) -> Probe {
        let mut url = match Url::parse(&format!("https://{handle}/.well-known/atproto-did")) {
            Ok(url) => url,
            Err(_) => return Probe::Absent,
        };
        if url.host_str() == Some("localhost") {
            let _ = url.set_scheme("http");
        }
        let response = match self.client.get(url, Redirects::Follow(3)).await {
            Ok(response) => response,
            Err(FetchError::Transport(e)) if host_does_not_exist(&e) => return Probe::Absent,
            Err(FetchError::Transport(e)) => return Probe::Unavailable(format!("well-known: {e}")),
            // Refused by policy, bad URL, redirect loop, oversized: not a handle host.
            Err(_) => return Probe::Absent,
        };
        match SafeClient::read_bounded(response, WELL_KNOWN_LIMIT).await {
            Ok((status, body)) => classify_well_known(status, &body),
            Err(FetchError::Transport(e)) => Probe::Unavailable(format!("well-known: {e}")),
            Err(_) => Probe::Absent,
        }
    }

    pub async fn resolve_dns(&self, handle: &String) -> Result<Option<String>> {
        let results = match self.dns.txt_lookup(format!("{SUBDOMAIN}.{handle}")).await {
            Ok(res) => res,
            Err(_) => return Ok(None),
        };

        let results = results
            .iter()
            .map(|item| item.to_string())
            .collect::<Vec<String>>();

        self.parse_dns_result(results)
    }

    pub async fn resolve_http(&self, handle: &String) -> Result<Option<String>> {
        let mut url = Url::parse(format!("https://{handle}/.well-known/atproto-did").as_str())?;
        if url.host_str() == Some("localhost") {
            let _ = url.set_scheme("http");
        }
        let response = self.client.get(url, Redirects::Follow(3)).await?;
        let (_, body) = SafeClient::read_bounded(response, WELL_KNOWN_LIMIT).await?;
        let res = String::from_utf8_lossy(&body).to_string();

        let did = match res.split("\n").collect::<Vec<&str>>().first() {
            None => return Ok(None),
            Some(first) => first.trim(),
        };

        match did.starts_with("did:") {
            true => Ok(Some(did.to_string())),
            false => Ok(None),
        }
    }

    pub async fn resolve_backup_dns(&mut self, handle: &String) -> Result<Option<String>> {
        let backup_ips = self.get_backup_nameserver_ips().await?;
        match backup_ips {
            Some(backup_ips) if backup_ips.len() >= 1 => {
                let mut config = ResolverConfig::default();
                let _ = backup_ips
                    .iter()
                    .map(|ip| {
                        config.add_name_server(NameServerConfig {
                            socket_addr: SocketAddr::new(*ip, 8080),
                            protocol: Default::default(),
                            tls_dns_name: None,
                            trust_negative_responses: false,
                            bind_addr: None,
                        })
                    })
                    .collect::<Vec<()>>();

                let resolver = TokioAsyncResolver::tokio(config, ResolverOpts::default());

                let results = match resolver.txt_lookup(format!("{SUBDOMAIN}.{handle}")).await {
                    Ok(res) => res,
                    Err(_) => return Ok(None),
                };

                let results = results
                    .iter()
                    .map(|item| item.to_string())
                    .collect::<Vec<String>>();

                self.parse_dns_result(results)
            }
            _ => Ok(None),
        }
    }

    pub fn parse_dns_result(&self, results: Vec<String>) -> Result<Option<String>> {
        let found = results
            .iter()
            .filter(|i| i.starts_with(PREFIX))
            .collect::<Vec<&String>>();

        match found.len() != 1 {
            true => Ok(None),
            false => Ok(Some(found[0][PREFIX.len()..].to_string())),
        }
    }

    async fn get_backup_nameserver_ips(&mut self) -> Result<Option<Vec<IpAddr>>> {
        match &self.backup_nameservers {
            None => return Ok(None),
            Some(backup_nameservers) => {
                if self.backup_nameserver_ips.is_none() {
                    let resolver = TokioAsyncResolver::tokio(
                        ResolverConfig::default(),
                        ResolverOpts::default(),
                    );

                    // Look up all backup nameservers
                    for h in backup_nameservers {
                        if let Ok(response) = resolver.lookup_ip(h.as_str()).await {
                            let mut backup_nameserver_ips = match &self.backup_nameserver_ips {
                                None => vec![],
                                Some(backup_nameserver_ips) => backup_nameserver_ips.clone(),
                            };
                            backup_nameserver_ips
                                .append(&mut response.iter().map(|ip| ip).collect::<Vec<IpAddr>>());
                            self.backup_nameserver_ips = Some(backup_nameserver_ips);
                        }
                    }
                }
            }
        }
        Ok(self.backup_nameserver_ips.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // new() is sync and callers build resolvers outside any runtime; building
    // the shared DNS resolver there must not need one.
    #[test]
    fn new_builds_without_a_runtime() {
        let resolver = HandleResolver::new(HandleResolverOpts {
            timeout: Some(Duration::from_millis(1500)),
            backup_nameservers: None,
        });
        assert_eq!(resolver.timeout, Duration::from_millis(1500));
    }

    #[test]
    fn dns_result_needs_exactly_one_did_record() {
        let resolver = HandleResolver::new(HandleResolverOpts {
            timeout: None,
            backup_nameservers: None,
        });
        let one = vec!["did=did:plc:abc".to_owned(), "v=spf1".to_owned()];
        assert_eq!(
            resolver.parse_dns_result(one).unwrap(),
            Some("did:plc:abc".to_owned())
        );
        let two = vec!["did=did:plc:abc".to_owned(), "did=did:plc:def".to_owned()];
        assert_eq!(resolver.parse_dns_result(two).unwrap(), None);
    }

    fn classified(status: u16, body: &str) -> HandleResolution {
        match classify_well_known(
            reqwest::StatusCode::from_u16(status).unwrap(),
            body.as_bytes(),
        ) {
            Probe::Found(did) => HandleResolution::Found(did),
            Probe::Absent => HandleResolution::NotFound,
            Probe::Unavailable(reason) => HandleResolution::Unavailable(reason),
        }
    }

    #[test]
    fn well_known_success_with_a_did_is_found() {
        assert_eq!(
            classified(200, "did:plc:abc\n"),
            HandleResolution::Found("did:plc:abc".to_owned())
        );
    }

    #[test]
    fn well_known_404_and_non_did_bodies_are_definitive() {
        assert_eq!(classified(404, "not found"), HandleResolution::NotFound);
        assert_eq!(classified(410, ""), HandleResolution::NotFound);
        assert_eq!(
            classified(200, "<html>parked</html>"),
            HandleResolution::NotFound
        );
    }

    // A rate limit or an outage must never read as "this handle is gone".
    #[test]
    fn well_known_429_and_5xx_are_unavailable() {
        for status in [429, 500, 502, 503, 403, 408] {
            assert!(
                matches!(
                    classified(status, "did:plc:abc"),
                    HandleResolution::Unavailable(_)
                ),
                "{status} must not be definitive"
            );
        }
    }

    #[test]
    fn missing_host_errors_are_definitive() {
        let err =
            std::io::Error::other("failed to lookup address: No address associated with hostname");
        assert!(host_does_not_exist(&err));
        let err = std::io::Error::other("connection reset by peer");
        assert!(!host_does_not_exist(&err));
    }

    #[tokio::test]
    #[ignore]
    async fn outcome_found_and_not_found_against_the_network() {
        let mut resolver = HandleResolver::new(HandleResolverOpts {
            timeout: None,
            backup_nameservers: None,
        });
        assert_eq!(
            resolver.resolve_outcome(&"bsky.app".to_owned()).await,
            HandleResolution::Found("did:plc:z72i7hdynmk6r22z27h6tvur".to_owned())
        );
        assert_eq!(
            resolver
                .resolve_outcome(&"no-such-handle-xq7z.invalid".to_owned())
                .await,
            HandleResolution::NotFound
        );
    }

    // The system resolver must answer real TXT lookups. Needs network, so it
    // is ignored by default: `cargo test -p rsky-identity -- --ignored`.
    #[tokio::test]
    #[ignore]
    async fn resolves_a_dns_only_handle_through_the_system_resolver() {
        let resolver = HandleResolver::new(HandleResolverOpts {
            timeout: None,
            backup_nameservers: None,
        });
        let did = resolver.resolve_dns(&"bsky.app".to_owned()).await.unwrap();
        assert_eq!(did.as_deref(), Some("did:plc:z72i7hdynmk6r22z27h6tvur"));
    }
}
