//! Finds posts the appview is missing although other posts reference them,
//! and fetches each one from its author's PDS.
//!
//! A post lost from the firehose shows up as a reply parent, thread root or
//! quoted post that is not in the `post` table, and clients render it as a
//! deleted post. Unlike a firehose replay this works for gaps of any age,
//! because the post is read from the repository that holds it.

use crate::backfiller::convert_record_to_ipld;
use crate::types::WintermuteError;
use chrono::{DateTime, Utc};
use deadpool_postgres::Client;
use rsky_identity::safe_fetch::{Redirects, SafeClient};
use rsky_repo::parse::get_and_parse_record;
use rsky_repo::readable_repo::ReadableRepo;
use rsky_repo::storage::memory_blockstore::MemoryBlockstore;
use std::sync::Arc;
use tokio::sync::RwLock;

pub const POST_COLLECTION: &str = "app.bsky.feed.post";

/// The posts missing from the `post` table that posts sorted in
/// `[since, until)` reference.
///
/// A reference is a reply parent, a reply root or a quoted post. The window
/// is on `"sortAt"`, which is indexed, so it bounds the scan.
pub async fn missing_targets(
    client: &Client,
    since: &str,
    until: &str,
) -> Result<Vec<String>, WintermuteError> {
    let rows = client
        .query(
            "SELECT DISTINCT t.target FROM ( \
               SELECT p.\"replyParent\" AS target FROM post p \
                WHERE p.\"sortAt\" >= $1 AND p.\"sortAt\" < $2 AND p.\"replyParent\" IS NOT NULL \
               UNION ALL \
               SELECT p.\"replyRoot\" FROM post p \
                WHERE p.\"sortAt\" >= $1 AND p.\"sortAt\" < $2 AND p.\"replyRoot\" IS NOT NULL \
               UNION ALL \
               SELECT q.subject FROM post p JOIN quote q ON q.uri = p.uri \
                WHERE p.\"sortAt\" >= $1 AND p.\"sortAt\" < $2 \
             ) t \
             WHERE t.target LIKE 'at://%/app.bsky.feed.post/%' \
               AND NOT EXISTS (SELECT 1 FROM post x WHERE x.uri = t.target) \
             ORDER BY t.target",
            &[&since, &until],
        )
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

/// A post to fetch, from its AT URI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub uri: String,
    pub did: String,
    pub rkey: String,
}

impl Target {
    /// `None` unless `uri` names a post by DID, the only form a strong
    /// reference to a post takes.
    #[must_use]
    pub fn parse(uri: &str) -> Option<Self> {
        let mut parts = uri.strip_prefix("at://")?.split('/');
        let did = parts.next()?;
        let collection = parts.next()?;
        let rkey = parts.next()?;
        if parts.next().is_some()
            || !did.starts_with("did:")
            || collection != POST_COLLECTION
            || rkey.is_empty()
        {
            return None;
        }
        Some(Self {
            uri: uri.to_owned(),
            did: did.to_owned(),
            rkey: rkey.to_owned(),
        })
    }
}

/// A record read out of a repository, ready for the indexer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoundRecord {
    pub cid: String,
    /// The revision of the commit the proof was taken at.
    pub rev: String,
    pub record: serde_json::Value,
}

/// What the author's PDS says about a post.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fetched {
    Found(FoundRecord),
    /// The repository proves the record is absent, or the PDS says so.
    Deleted,
    /// The PDS will not serve the repository: deactivated, taken down,
    /// suspended or gone. The reason is the XRPC error name.
    Unavailable(String),
}

/// Reads `collection/rkey` out of a `com.atproto.sync.getRecord` response.
///
/// The response is a CAR rooted at the repository's current commit, holding
/// the MST path to the key and the record block when it exists. `None` when
/// the proof shows the key is absent.
///
/// The commit signature is not checked: the PDS was resolved from the
/// author's DID document, as the backfiller trusts it for whole repositories.
pub async fn record_from_proof(
    car: &[u8],
    did: &str,
    collection: &str,
    rkey: &str,
) -> Result<Option<FoundRecord>, WintermuteError> {
    let car = rsky_repo::car::read_car_with_root(car.to_vec())
        .await
        .map_err(|e| WintermuteError::Repo(format!("proof car unreadable: {e}")))?;
    let blocks = car.blocks.clone();
    let storage = MemoryBlockstore::new(Some(car.blocks))
        .await
        .map_err(|e| WintermuteError::Repo(format!("proof blockstore: {e}")))?;
    let mut repo = ReadableRepo::load(Arc::new(RwLock::new(storage)), car.root)
        .await
        .map_err(|e| WintermuteError::Repo(format!("proof commit unreadable: {e}")))?;
    if repo.did() != did {
        return Err(WintermuteError::Repo(format!(
            "proof is for {}, not {did}",
            repo.did()
        )));
    }
    let key = format!("{collection}/{rkey}");
    let Some(cid) = repo
        .data
        .get(&key)
        .await
        .map_err(|e| WintermuteError::Repo(format!("proof path for {key} incomplete: {e}")))?
    else {
        return Ok(None);
    };
    let parsed = get_and_parse_record(&blocks, cid)
        .map_err(|e| WintermuteError::Repo(format!("record block for {key}: {e}")))?;
    let json = serde_json::to_value(&parsed.record)
        .map_err(|e| WintermuteError::Serialization(format!("record {key}: {e}")))?;
    Ok(Some(FoundRecord {
        cid: cid.to_string(),
        rev: repo.commit.rev.clone(),
        record: convert_record_to_ipld(&json),
    }))
}

/// Reads an XRPC error response for a record fetch. `None` for an error
/// that does not settle the post's state, which the caller should report.
#[must_use]
pub fn classify_error(status: u16, body: &str) -> Option<Fetched> {
    if !(400..500).contains(&status) {
        return None;
    }
    let name = serde_json::from_str::<serde_json::Value>(body)
        .ok()?
        .get("error")?
        .as_str()?
        .to_owned();
    match name.as_str() {
        "RecordNotFound" => Some(Fetched::Deleted),
        "RepoNotFound" | "RepoTakendown" | "RepoDeactivated" | "RepoSuspended" => {
            Some(Fetched::Unavailable(name))
        }
        _ => None,
    }
}

/// Fetches `target` from `pds` with `com.atproto.sync.getRecord`.
pub async fn fetch_post(
    http: &SafeClient,
    pds: &str,
    target: &Target,
) -> Result<Fetched, WintermuteError> {
    let mut url = url::Url::parse(&format!(
        "{}/xrpc/com.atproto.sync.getRecord",
        pds.trim_end_matches('/')
    ))
    .map_err(|e| WintermuteError::Other(format!("bad pds endpoint {pds}: {e}")))?;
    url.query_pairs_mut()
        .append_pair("did", &target.did)
        .append_pair("collection", POST_COLLECTION)
        .append_pair("rkey", &target.rkey);
    let url = http
        .checked(url.as_str())
        .map_err(|e| WintermuteError::Other(format!("refused {url}: {e}")))?;
    let response = http
        .get(url, Redirects::Follow(3))
        .await
        .map_err(|e| WintermuteError::Other(format!("fetch failed: {e}")))?;
    let status = response.status();
    let body = response.bytes().await?;
    if status.is_success() {
        let found = record_from_proof(&body, &target.did, POST_COLLECTION, &target.rkey).await?;
        return Ok(found.map_or(Fetched::Deleted, Fetched::Found));
    }
    classify_error(status.as_u16(), &String::from_utf8_lossy(&body)).ok_or_else(|| {
        WintermuteError::Other(format!(
            "{status}: {}",
            String::from_utf8_lossy(&body)
                .chars()
                .take(200)
                .collect::<String>()
        ))
    })
}

/// The `indexedAt` for a post repaired now: its `createdAt`, so that it
/// sorts where it was written rather than as new, but never later than
/// `now`, since a client's clock can run ahead.
#[must_use]
pub fn repaired_indexed_at(record: &serde_json::Value, now: DateTime<Utc>) -> String {
    let created_at = record
        .get("createdAt")
        .and_then(serde_json::Value::as_str)
        .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
        .map(|t| t.with_timezone(&Utc));
    created_at
        .map_or(now, |t| t.min(now))
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

#[cfg(test)]
mod tests;
