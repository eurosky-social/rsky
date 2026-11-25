//! Eurosky: a reply to a video post can name the second of the video it is
//! about, in an extra field on the post record:
//!
//! ```json
//! "network.eurosky.mediaTime": {"$type": "network.eurosky.mediaTime", "seconds": 41}
//! ```
//!
//! Valid values are kept in `post_media_time`, one row per reply, so the
//! AppView reads a video's moments without opening the records of its other
//! replies. The AppView's dataplane creates the table (migration
//! `20261125T120000000Z`) and applies the same rules when it indexes
//! (`indexing/media-time.ts`).

use crate::types::WintermuteError;

pub const MEDIA_TIME_FIELD: &str = "network.eurosky.mediaTime";
/// 100 hours, the same limit the app applies when writing the field.
pub const MAX_MEDIA_TIME_SECONDS: i64 = 360_000;

/// The second a post record names, or `None` when it names none.
pub fn media_time_seconds(record: &serde_json::Value) -> Option<i32> {
    let value = record.get(MEDIA_TIME_FIELD)?.as_object()?;
    if let Some(value_type) = value.get("$type") {
        if value_type.as_str() != Some(MEDIA_TIME_FIELD) {
            return None;
        }
    }
    let seconds = value.get("seconds")?.as_i64()?;
    if !(0..=MAX_MEDIA_TIME_SECONDS).contains(&seconds) {
        return None;
    }
    i32::try_from(seconds).ok()
}

/// A linked reply: (reply uri, parent uri, second).
pub type MediaTimeRow = (String, String, i32);

/// Records the linked replies of a batch. Rare, so a plain insert, no COPY.
pub async fn insert_media_times(
    client: &deadpool_postgres::Client,
    rows: &[MediaTimeRow],
) -> Result<(), WintermuteError> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut uris = Vec::with_capacity(rows.len());
    let mut parents = Vec::with_capacity(rows.len());
    let mut seconds = Vec::with_capacity(rows.len());
    for (uri, parent, second) in rows {
        uris.push(uri.as_str());
        parents.push(parent.as_str());
        seconds.push(*second);
    }
    client
        .execute(
            "INSERT INTO post_media_time (\"postUri\", \"parentUri\", seconds)
             SELECT * FROM unnest($1::varchar[], $2::varchar[], $3::int4[])
             ON CONFLICT DO NOTHING",
            &[&uris, &parents, &seconds],
        )
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn post_with(value: serde_json::Value) -> serde_json::Value {
        json!({ "text": "reply", MEDIA_TIME_FIELD: value })
    }

    #[test]
    fn reads_whole_seconds_within_100_hours() {
        assert_eq!(
            media_time_seconds(&post_with(
                json!({"$type": MEDIA_TIME_FIELD, "seconds": 41})
            )),
            Some(41)
        );
        assert_eq!(
            media_time_seconds(&post_with(json!({"seconds": 0}))),
            Some(0)
        );
        assert_eq!(
            media_time_seconds(&post_with(json!({"seconds": MAX_MEDIA_TIME_SECONDS}))),
            Some(360_000)
        );
    }

    #[test]
    fn ignores_anything_else() {
        for value in [
            json!(null),
            json!(41),
            json!([41]),
            json!({"$type": "x.y.z", "seconds": 7}),
            json!({"$type": null, "seconds": 7}),
            json!({"seconds": -1}),
            json!({"seconds": 4.5}),
            json!({"seconds": MAX_MEDIA_TIME_SECONDS + 1}),
            json!({"seconds": "41"}),
            json!({}),
        ] {
            assert_eq!(
                media_time_seconds(&post_with(value.clone())),
                None,
                "{value}"
            );
        }
        assert_eq!(media_time_seconds(&json!({"text": "no field"})), None);
    }
}
