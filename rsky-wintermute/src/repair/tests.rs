//! The proof parsing runs against repositories built through the real
//! commit path. `missing_targets` needs a Postgres with the appview schema
//! (`DATABASE_URL`, default
//! `postgresql://postgres:postgres@localhost:5432/bsky_test`).

use super::{
    Fetched, POST_COLLECTION, Target, classify_error, missing_targets, record_from_proof,
    repaired_indexed_at,
};
use chrono::{DateTime, Utc};
use rsky_repo::repo::Repo;
use rsky_repo::storage::memory_blockstore::MemoryBlockstore;
use rsky_repo::sync::provider::get_records;
use rsky_repo::types::{RecordCreateOrUpdateOp, RecordPath, RepoRecord, WriteOpAction};
use secp256k1::Keypair;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::RwLock;

fn keypair() -> Keypair {
    let seed: [u8; 32] = rand::random();
    Keypair::from_seckey_slice(secp256k1::SECP256K1, &seed).unwrap()
}

fn post(rkey: &str, text: &str) -> RecordCreateOrUpdateOp {
    let record: RepoRecord = serde_json::from_value(json!({
        "$type": POST_COLLECTION,
        "text": text,
        "createdAt": "2026-10-02T16:00:00.000Z",
    }))
    .unwrap();
    RecordCreateOrUpdateOp {
        action: WriteOpAction::Create,
        collection: POST_COLLECTION.to_owned(),
        rkey: rkey.to_owned(),
        record,
    }
}

/// A repository holding `posts`, and the `getRecord` proof CAR for `rkey`.
async fn proof_for(did: &str, posts: &[(&str, &str)], rkey: &str) -> (Repo, Vec<u8>) {
    let storage = Arc::new(RwLock::new(MemoryBlockstore::new(None).await.unwrap()));
    let writes = posts.iter().map(|(rkey, text)| post(rkey, text)).collect();
    let repo = Repo::create(storage.clone(), did.to_owned(), &keypair(), Some(writes))
        .await
        .unwrap();
    let car = get_records(
        storage,
        repo.cid,
        vec![RecordPath {
            collection: POST_COLLECTION.to_owned(),
            rkey: rkey.to_owned(),
        }],
    )
    .await
    .unwrap();
    (repo, car)
}

#[tokio::test]
async fn reads_a_record_out_of_its_proof() {
    let did = "did:plc:repairproof";
    let posts: Vec<(String, String)> = (0..40)
        .map(|i| (format!("3mwz{i:09}"), format!("post {i}")))
        .collect();
    let posts: Vec<(&str, &str)> = posts
        .iter()
        .map(|(rkey, text)| (rkey.as_str(), text.as_str()))
        .collect();
    let (repo, car) = proof_for(did, &posts, "3mwz000000017").await;

    let found = record_from_proof(&car, did, POST_COLLECTION, "3mwz000000017")
        .await
        .unwrap()
        .expect("the record is in the proof");
    assert_eq!(found.rev, repo.commit.rev);
    assert_eq!(found.record["text"], "post 17");
    assert_eq!(found.record["$type"], POST_COLLECTION);
    assert!(found.cid.starts_with("bafyrei"), "{}", found.cid);
}

#[tokio::test]
async fn a_proof_of_absence_reads_as_none() {
    let did = "did:plc:repairabsent";
    let (_, car) = proof_for(did, &[("3mwz000000001", "kept")], "3mwz000000002").await;
    let found = record_from_proof(&car, did, POST_COLLECTION, "3mwz000000002")
        .await
        .unwrap();
    assert_eq!(found, None);
}

#[tokio::test]
async fn a_proof_for_another_repository_is_refused() {
    let (_, car) = proof_for(
        "did:plc:someoneelse",
        &[("3mwz000000001", "x")],
        "3mwz000000001",
    )
    .await;
    assert!(
        record_from_proof(&car, "did:plc:expected", POST_COLLECTION, "3mwz000000001")
            .await
            .is_err()
    );
}

#[test]
fn targets_are_posts_named_by_did() {
    assert_eq!(
        Target::parse("at://did:plc:abc/app.bsky.feed.post/3mwz5he7p4c2d"),
        Some(Target {
            uri: "at://did:plc:abc/app.bsky.feed.post/3mwz5he7p4c2d".to_owned(),
            did: "did:plc:abc".to_owned(),
            rkey: "3mwz5he7p4c2d".to_owned(),
        })
    );
    assert_eq!(
        Target::parse("at://alice.example/app.bsky.feed.post/3mwz"),
        None
    );
    assert_eq!(
        Target::parse("at://did:plc:abc/app.bsky.feed.like/3mwz"),
        None
    );
    assert_eq!(Target::parse("at://did:plc:abc/app.bsky.feed.post/"), None);
    assert_eq!(
        Target::parse("at://did:plc:abc/app.bsky.feed.post/a/b"),
        None
    );
    assert_eq!(
        Target::parse("https://did:plc:abc/app.bsky.feed.post/3mwz"),
        None
    );
}

#[test]
fn xrpc_errors_that_settle_a_post() {
    assert_eq!(
        classify_error(400, r#"{"error":"RecordNotFound","message":"x"}"#),
        Some(Fetched::Deleted)
    );
    assert_eq!(
        classify_error(400, r#"{"error":"RepoDeactivated"}"#),
        Some(Fetched::Unavailable("RepoDeactivated".to_owned()))
    );
    assert_eq!(
        classify_error(404, r#"{"error":"RepoNotFound"}"#),
        Some(Fetched::Unavailable("RepoNotFound".to_owned()))
    );
    assert_eq!(classify_error(400, r#"{"error":"InvalidRequest"}"#), None);
    assert_eq!(classify_error(502, r#"{"error":"RecordNotFound"}"#), None);
    assert_eq!(classify_error(404, "<html>not found</html>"), None);
}

#[test]
fn repaired_posts_keep_their_creation_time() {
    let now = DateTime::parse_from_rfc3339("2026-10-07T20:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    assert_eq!(
        repaired_indexed_at(&json!({"createdAt": "2026-10-02T16:01:02.345678Z"}), now),
        "2026-10-02T16:01:02.345Z"
    );
    assert_eq!(
        repaired_indexed_at(&json!({"createdAt": "2030-01-01T00:00:00Z"}), now),
        "2026-10-07T20:00:00.000Z"
    );
    assert_eq!(
        repaired_indexed_at(&json!({"text": "no time"}), now),
        "2026-10-07T20:00:00.000Z"
    );
}

#[tokio::test]
async fn finds_reply_parents_roots_and_quotes_that_are_missing() {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:5432/bsky_test".to_owned());
    let pool =
        crate::config::create_pg_pool(&database_url, crate::config::pg_pool_config(2)).unwrap();
    let client = pool.get().await.unwrap();
    let a = "did:plc:repairtargets";
    let uri = |rkey: &str| format!("at://{a}/app.bsky.feed.post/{rkey}");
    let cleanup = || async {
        client
            .execute("DELETE FROM post WHERE creator = $1", &[&a])
            .await
            .unwrap();
        client
            .execute(
                "DELETE FROM quote WHERE uri LIKE $1",
                &[&format!("at://{a}/%")],
            )
            .await
            .unwrap();
    };
    cleanup().await;

    let insert_post =
        |rkey: &'static str, at: &'static str, root: Option<String>, parent: Option<String>| {
            let client = &client;
            let uri = uri(rkey);
            async move {
                client
                .execute(
                    "INSERT INTO post (uri, cid, creator, text, \"replyRoot\", \"replyRootCid\", \
                     \"replyParent\", \"replyParentCid\", \"createdAt\", \"indexedAt\") \
                     VALUES ($1, 'bafy', $2, '', $3, 'bafy', $4, 'bafy', $5, $5)",
                    &[&uri, &a, &root, &parent, &at],
                )
                .await
                .unwrap();
            }
        };
    // present root and parent
    insert_post("present", "2026-10-02T10:00:00.000Z", None, None).await;
    // a reply in the window: root present, parent missing
    insert_post(
        "reply",
        "2026-10-02T18:00:00.000Z",
        Some(uri("present")),
        Some(uri("lostparent")),
    )
    .await;
    // a reply in the window whose root is missing too
    insert_post(
        "deepreply",
        "2026-10-02T18:30:00.000Z",
        Some(uri("lostroot")),
        Some(uri("present")),
    )
    .await;
    // a reply outside the window
    insert_post(
        "late",
        "2026-10-03T18:00:00.000Z",
        None,
        Some(uri("lateparent")),
    )
    .await;
    // a quote in the window, of a missing post
    insert_post("quoting", "2026-10-02T18:10:00.000Z", None, None).await;
    client
        .execute(
            "INSERT INTO quote (uri, cid, subject, \"subjectCid\", \"createdAt\", \"indexedAt\") \
             VALUES ($1, 'bafy', $2, 'bafy', $3, $3)",
            &[
                &uri("quoting"),
                &uri("lostquoted"),
                &"2026-10-02T18:10:00.000Z",
            ],
        )
        .await
        .unwrap();

    let found = missing_targets(&client, "2026-10-02T17:00:00Z", "2026-10-02T19:00:00Z")
        .await
        .unwrap();
    let ours: Vec<&String> = found.iter().filter(|t| t.contains(a)).collect();
    assert_eq!(
        ours,
        [&uri("lostparent"), &uri("lostquoted"), &uri("lostroot")]
    );
    cleanup().await;
}
