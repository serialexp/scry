//! Owner-fenced publication of remote agent status snapshots.

use std::time::Duration;

use anyhow::{Context, Result};
use fred::prelude::*;
use uuid::Uuid;

use crate::status::STATUS_PREFIX;

pub const AGENT_STATUS_TTL: Duration = Duration::from_secs(20);
const OWNER_PREFIX: &str = "scry/status-owner/";

const UPSERT_LUA: &str = r#"
local owner = redis.call('GET', KEYS[2])
if owner and owner > ARGV[1] then return 0 end
redis.call('SET', KEYS[2], ARGV[1], 'PX', ARGV[3])
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
return 1
"#;

const REMOVE_LUA: &str = r#"
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1])
redis.call('DEL', KEYS[2])
return 1
"#;

fn keys(instance_id: &str) -> (String, String) {
    let encoded = instance_id.replace('/', "%2F");
    (
        format!("{STATUS_PREFIX}{encoded}"),
        format!("{OWNER_PREFIX}{encoded}"),
    )
}

pub async fn upsert_remote_status(
    client: &Client,
    instance_id: &str,
    owner: Uuid,
    snapshot_json: &str,
) -> Result<bool> {
    let (status_key, owner_key) = keys(instance_id);
    let ttl_ms = AGENT_STATUS_TTL.as_millis() as i64;
    let result: i64 = client
        .eval(
            UPSERT_LUA,
            vec![status_key, owner_key],
            vec![
                owner.to_string(),
                snapshot_json.to_string(),
                ttl_ms.to_string(),
            ],
        )
        .await
        .context("upserting owner-fenced remote status")?;
    Ok(result == 1)
}

pub async fn remove_remote_status(client: &Client, instance_id: &str, owner: Uuid) -> Result<bool> {
    let (status_key, owner_key) = keys(instance_id);
    let result: i64 = client
        .eval(
            REMOVE_LUA,
            vec![status_key, owner_key],
            vec![owner.to_string()],
        )
        .await
        .context("removing owner-fenced remote status")?;
    Ok(result == 1)
}
