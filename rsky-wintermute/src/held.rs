//! What the appview holds for an account.
//!
//! Every indexed record has a row in `record`, except likes, reposts, follows
//! and blocks written while `RECORD_SKIP_BOILERPLATE` is on: those live only
//! in their typed tables (`like`, `repost`, `follow`, `actor_block`), and the
//! appview rebuilds them from there. The tools that compare a repository with
//! what the appview holds (`fill_repo_gaps`, the reconcile workflow,
//! `car_loader`) read both through this module, so they give the same answer
//! with the setting on or off.

use std::collections::{HashMap, HashSet};

use deadpool_postgres::Client;

use crate::types::WintermuteError;

/// Where a held record was found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeldIn {
    /// A row in `record`, with the repository revision it was indexed from
    /// when one was kept.
    Record { rev: Option<String> },
    /// Only in its typed table, which keeps no revision: when it was indexed.
    Typed { indexed_at: String },
}

/// A record the appview holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Held {
    pub cid: String,
    pub held_in: HeldIn,
}

/// Everything held for one account.
#[derive(Debug, Default)]
pub struct Holdings {
    /// By URI. A record in both `record` and its typed table counts once, as
    /// its `record` row.
    pub records: HashMap<String, Held>,
    /// `(collection, subject)` → URI of every like, repost, follow and block
    /// in the typed tables. Those tables keep one row per creator and
    /// subject, so a second like of the same post is never stored: see
    /// [`Holdings::subject_held_by`].
    subjects: HashMap<(String, String), String>,
    /// Likes, reposts, follows and blocks with a `record` row but none in
    /// their typed table: refused there as a second one of a subject, or
    /// lost (`fill_repo_gaps` before INF-126 wrote a like again before
    /// deleting the one that held its subject, which the table refused).
    pub untyped: HashSet<String>,
}

impl Holdings {
    /// The URI of the like, repost, follow or block of `subject` held for
    /// the account, if there is one.
    #[must_use]
    pub fn subject_held_by(&self, collection: &str, subject: &str) -> Option<&str> {
        self.subjects
            .get(&(collection.to_owned(), subject.to_owned()))
            .map(String::as_str)
    }
}

/// A typed table that holds one of the boilerplate collections.
struct Typed {
    collection: &'static str,
    table: &'static str,
    /// `uri, cid, "indexedAt", <subject>` of one creator's rows. Likes in a
    /// space are not part of the public repository.
    by_creator: &'static str,
}

const TYPED: [Typed; 4] = [
    Typed {
        collection: "app.bsky.feed.like",
        table: "\"like\"",
        by_creator: "SELECT uri, cid, \"indexedAt\", subject FROM \"like\" \
                     WHERE creator = $1 AND space_uri IS NULL",
    },
    Typed {
        collection: "app.bsky.feed.repost",
        table: "repost",
        by_creator: "SELECT uri, cid, \"indexedAt\", subject FROM repost WHERE creator = $1",
    },
    Typed {
        collection: "app.bsky.graph.follow",
        table: "follow",
        by_creator: "SELECT uri, cid, \"indexedAt\", \"subjectDid\" FROM follow WHERE creator = $1",
    },
    Typed {
        collection: "app.bsky.graph.block",
        table: "actor_block",
        by_creator: "SELECT uri, cid, \"indexedAt\", \"subjectDid\" FROM actor_block \
                     WHERE creator = $1",
    },
];

/// `collection` of an `at://did/collection/rkey` URI.
#[must_use]
pub fn collection_of(uri: &str) -> &str {
    uri.trim_start_matches("at://")
        .split('/')
        .nth(1)
        .unwrap_or("")
}

/// The subject a like, repost, follow or block points at, as its typed table
/// keeps it; `None` for other collections or a record without one.
#[must_use]
pub fn subject_of(collection: &str, record: &serde_json::Value) -> Option<String> {
    let subject = record.get("subject")?;
    match collection {
        "app.bsky.feed.like" | "app.bsky.feed.repost" => subject.get("uri")?.as_str(),
        "app.bsky.graph.follow" | "app.bsky.graph.block" => subject.as_str(),
        _ => None,
    }
    .map(str::to_owned)
}

/// Every record the appview holds for `did`.
pub async fn holdings(client: &Client, did: &str) -> Result<Holdings, WintermuteError> {
    let mut records: HashMap<String, Held> = client
        .query("SELECT uri, cid, rev FROM record WHERE did = $1", &[&did])
        .await?
        .into_iter()
        .map(|row| {
            (
                row.get(0),
                Held {
                    cid: row.get(1),
                    held_in: HeldIn::Record { rev: row.get(2) },
                },
            )
        })
        .collect();
    let mut subjects = HashMap::new();
    let mut in_typed = HashSet::new();
    for typed in &TYPED {
        for row in client.query(typed.by_creator, &[&did]).await? {
            let uri: String = row.get(0);
            subjects.insert(
                (typed.collection.to_owned(), row.get::<_, String>(3)),
                uri.clone(),
            );
            in_typed.insert(uri.clone());
            records.entry(uri).or_insert_with(|| Held {
                cid: row.get(1),
                held_in: HeldIn::Typed {
                    indexed_at: row.get(2),
                },
            });
        }
    }
    let untyped = records
        .keys()
        .filter(|uri| {
            let collection = collection_of(uri);
            TYPED.iter().any(|t| t.collection == collection) && !in_typed.contains(*uri)
        })
        .cloned()
        .collect();
    Ok(Holdings {
        records,
        subjects,
        untyped,
    })
}

/// Whether the appview still holds `uri`, in `record` or its typed table.
pub async fn still_held(client: &Client, uri: &str) -> Result<bool, WintermuteError> {
    if client
        .query_opt("SELECT 1 FROM record WHERE uri = $1", &[&uri])
        .await?
        .is_some()
    {
        return Ok(true);
    }
    let collection = collection_of(uri);
    let Some(typed) = TYPED.iter().find(|t| t.collection == collection) else {
        return Ok(false);
    };
    Ok(client
        .query_opt(
            &format!("SELECT 1 FROM {} WHERE uri = $1", typed.table),
            &[&uri],
        )
        .await?
        .is_some())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subjects_are_read_as_the_typed_tables_keep_them() {
        let post = "at://did:plc:a/app.bsky.feed.post/1";
        let strong = serde_json::json!({"subject": {"uri": post, "cid": "bafy"}});
        let did = serde_json::json!({"subject": "did:plc:b"});
        assert_eq!(
            subject_of("app.bsky.feed.like", &strong).as_deref(),
            Some(post)
        );
        assert_eq!(
            subject_of("app.bsky.feed.repost", &strong).as_deref(),
            Some(post)
        );
        assert_eq!(
            subject_of("app.bsky.graph.follow", &did).as_deref(),
            Some("did:plc:b")
        );
        assert_eq!(
            subject_of("app.bsky.graph.block", &did).as_deref(),
            Some("did:plc:b")
        );
        assert_eq!(subject_of("app.bsky.graph.follow", &strong), None);
        assert_eq!(subject_of("app.bsky.feed.post", &strong), None);
    }

    #[test]
    fn collection_comes_from_the_uri() {
        assert_eq!(
            collection_of("at://did:plc:a/app.bsky.feed.like/3k"),
            "app.bsky.feed.like"
        );
        assert_eq!(collection_of("at://did:plc:a"), "");
    }

    fn test_pool() -> deadpool_postgres::Pool {
        let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/bsky_test".to_owned()
        });
        crate::config::create_pg_pool(&url, crate::config::pg_pool_config(2)).unwrap()
    }

    async fn clean(client: &Client, did: &str) {
        for sql in [
            "DELETE FROM record WHERE did = $1",
            "DELETE FROM \"like\" WHERE creator = $1",
            "DELETE FROM repost WHERE creator = $1",
            "DELETE FROM follow WHERE creator = $1",
            "DELETE FROM actor_block WHERE creator = $1",
        ] {
            client.execute(sql, &[&did]).await.unwrap();
        }
    }

    #[tokio::test]
    async fn holdings_join_record_rows_and_typed_only_rows() {
        let pool = test_pool();
        let client = pool.get().await.unwrap();
        let did = "did:plc:wintermute-test-held";
        clean(&client, did).await;
        let post = "at://did:plc:wintermute-test-held-author/app.bsky.feed.post/p";
        let other = "at://did:plc:wintermute-test-held-author/app.bsky.feed.post/q";
        let ts = "2026-11-25T08:00:00.000Z";
        let old_like = format!("at://{did}/app.bsky.feed.like/old");
        let new_like = format!("at://{did}/app.bsky.feed.like/new");
        let space_like = format!("at://{did}/app.bsky.feed.like/space");
        let follow = format!("at://{did}/app.bsky.graph.follow/f");
        let profile = format!("at://{did}/app.bsky.actor.profile/self");

        // A like from before the switch: in `record` and its table. Another
        // only in `record`.
        let lost_like = format!("at://{did}/app.bsky.feed.like/lost");
        for (uri, rev) in [(&old_like, "3a"), (&profile, "3a"), (&lost_like, "3a")] {
            client
                .execute(
                    "INSERT INTO record (uri, cid, did, json, rev, \"indexedAt\") \
                     VALUES ($1, 'cid-r', $2, 'null', $3, $4)",
                    &[uri, &did, &rev, &ts],
                )
                .await
                .unwrap();
        }
        for (uri, subject, space) in [
            (&old_like, post, None::<&str>),
            (&new_like, other, None),
            (
                &space_like,
                "at://did:plc:x/app.bsky.feed.post/s",
                Some("at://space"),
            ),
        ] {
            client
                .execute(
                    "INSERT INTO \"like\" (uri, cid, creator, subject, \"subjectCid\", \"createdAt\", \
                     \"indexedAt\", space_uri) VALUES ($1, 'cid-t', $2, $3, 'c', $4, $4, $5)",
                    &[uri, &did, &subject, &ts, &space],
                )
                .await
                .unwrap();
        }
        client
            .execute(
                "INSERT INTO follow (uri, cid, creator, \"subjectDid\", \"createdAt\", \"indexedAt\") \
                 VALUES ($1, 'cid-f', $2, 'did:plc:followed', $3, $3)",
                &[&follow, &did, &ts],
            )
            .await
            .unwrap();

        let held = holdings(&client, did).await.unwrap();
        assert_eq!(held.records.len(), 5, "{:?}", held.records.keys());
        assert_eq!(
            held.untyped,
            HashSet::from([lost_like.clone()]),
            "only the like missing from its table; a profile has no typed table here"
        );
        assert_eq!(
            held.records[&old_like].held_in,
            HeldIn::Record {
                rev: Some("3a".to_owned())
            },
            "a record row wins over the typed row"
        );
        assert_eq!(held.records[&old_like].cid, "cid-r");
        assert_eq!(
            held.records[&new_like],
            Held {
                cid: "cid-t".to_owned(),
                held_in: HeldIn::Typed {
                    indexed_at: ts.to_owned()
                },
            }
        );
        assert!(
            matches!(held.records[&follow].held_in, HeldIn::Typed { .. }),
            "a follow held only in its table"
        );
        assert!(held.records.contains_key(&profile));
        assert!(
            !held.records.contains_key(&space_like),
            "space likes are not in the repo"
        );
        assert_eq!(
            held.subject_held_by("app.bsky.feed.like", post),
            Some(old_like.as_str())
        );
        assert_eq!(
            held.subject_held_by("app.bsky.feed.like", other),
            Some(new_like.as_str())
        );
        assert_eq!(
            held.subject_held_by("app.bsky.graph.follow", "did:plc:followed"),
            Some(follow.as_str())
        );
        assert_eq!(held.subject_held_by("app.bsky.feed.repost", post), None);

        assert!(still_held(&client, &new_like).await.unwrap());
        assert!(still_held(&client, &profile).await.unwrap());
        let never_liked = format!("at://{did}/app.bsky.feed.like/none");
        let never_posted = format!("at://{did}/app.bsky.feed.post/none");
        assert!(!still_held(&client, &never_liked).await.unwrap());
        assert!(!still_held(&client, &never_posted).await.unwrap());
        clean(&client, did).await;
    }
}
