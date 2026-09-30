use crate::safe_fetch::{NetworkPolicy, Redirects, SafeClient};
use crate::types::HandleResolverOpts;
use anyhow::Result;
use hickory_resolver::config::*;
use hickory_resolver::TokioAsyncResolver;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use url::Url;

pub const SUBDOMAIN: &str = "_atproto";
pub const PREFIX: &str = "did=";

/// The most a well-known handle document may be.
const WELL_KNOWN_LIMIT: usize = 8 * 1024;

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
