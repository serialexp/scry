use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use scry_server::{FleetSource, RemoteAgentStatus, RemoteStatusRelay};
use scry_valkey::{remove_remote_status, upsert_remote_status, ValkeyClient, AGENT_STATUS_TTL};
use tokio::sync::mpsc;
use tracing::warn;
use uuid::Uuid;

#[derive(Clone)]
struct Entry {
    owner: Uuid,
    sequence: u64,
    expires: Instant,
    json: String,
}

#[derive(Default)]
pub struct AgentStatusRegistry {
    entries: Mutex<HashMap<String, Entry>>,
}

impl AgentStatusRegistry {
    fn report(&self, status: &RemoteAgentStatus, json: String) -> bool {
        let mut entries = self.entries.lock().unwrap();
        let replace = entries.get(&status.instance_id).is_none_or(|old| {
            status.owner_token > old.owner
                || (status.owner_token == old.owner && status.sequence > old.sequence)
        });
        if replace {
            entries.insert(
                status.instance_id.clone(),
                Entry {
                    owner: status.owner_token,
                    sequence: status.sequence,
                    expires: Instant::now() + AGENT_STATUS_TTL,
                    json,
                },
            );
        }
        replace
    }

    fn remove(&self, instance_id: &str, owner: Uuid) {
        let mut entries = self.entries.lock().unwrap();
        if entries
            .get(instance_id)
            .is_some_and(|entry| entry.owner == owner)
        {
            entries.remove(instance_id);
        }
    }

    pub fn blobs(&self) -> Vec<String> {
        let now = Instant::now();
        let mut entries = self.entries.lock().unwrap();
        entries.retain(|_, entry| entry.expires > now);
        entries.values().map(|entry| entry.json.clone()).collect()
    }
}

#[async_trait::async_trait]
impl FleetSource for AgentStatusRegistry {
    async fn blobs(&self) -> Vec<String> {
        self.blobs()
    }
}

enum Command {
    Report(RemoteAgentStatus),
    Remove { instance_id: String, owner: Uuid },
}

pub struct AgentStatusRelay {
    registry: Arc<AgentStatusRegistry>,
    tx: mpsc::Sender<Command>,
}

impl AgentStatusRelay {
    pub fn new(valkey: Option<ValkeyClient>) -> (Arc<Self>, Arc<AgentStatusRegistry>) {
        let registry = Arc::new(AgentStatusRegistry::default());
        let (tx, mut rx) = mpsc::channel(64);
        let worker_registry = registry.clone();
        tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                match command {
                    Command::Report(status) => {
                        let json = match serde_json::to_string(&status.snapshot) {
                            Ok(json) => json,
                            Err(e) => {
                                warn!(error = %e, "serializing remote agent status failed");
                                continue;
                            }
                        };
                        if !worker_registry.report(&status, json.clone()) {
                            continue;
                        }
                        if let Some(client) = &valkey {
                            if let Err(e) = upsert_remote_status(
                                client,
                                &status.instance_id,
                                status.owner_token,
                                &json,
                            )
                            .await
                            {
                                warn!(error = %e, agent = %status.instance_id, "publishing remote agent status failed");
                            }
                        }
                    }
                    Command::Remove { instance_id, owner } => {
                        worker_registry.remove(&instance_id, owner);
                        if let Some(client) = &valkey {
                            if let Err(e) = remove_remote_status(client, &instance_id, owner).await
                            {
                                warn!(error = %e, agent = %instance_id, "removing remote agent status failed");
                            }
                        }
                    }
                }
            }
        });
        (
            Arc::new(Self {
                registry: registry.clone(),
                tx,
            }),
            registry,
        )
    }
}

impl RemoteStatusRelay for AgentStatusRelay {
    fn report(&self, status: RemoteAgentStatus) {
        if self.tx.try_send(Command::Report(status)).is_err() {
            warn!("remote agent status queue full; dropping report");
        }
    }

    fn remove(&self, instance_id: &str, owner_token: Uuid) {
        // Remove synchronously from local visibility; Valkey deletion is best effort.
        self.registry.remove(instance_id, owner_token);
        if self
            .tx
            .try_send(Command::Remove {
                instance_id: instance_id.to_string(),
                owner: owner_token,
            })
            .is_err()
        {
            warn!(agent = %instance_id, "remote agent status queue full; TTL will remove report");
        }
    }
}

pub struct MergedFleetSource {
    pub valkey: Option<ValkeyClient>,
    pub local: Arc<AgentStatusRegistry>,
}

#[async_trait::async_trait]
impl FleetSource for MergedFleetSource {
    fn source(&self) -> &'static str {
        if self.valkey.is_some() {
            "mixed"
        } else {
            "local"
        }
    }

    async fn blobs(&self) -> Vec<String> {
        let mut by_id: HashMap<String, (u64, String)> = HashMap::new();
        let mut blobs = self.local.blobs();
        if let Some(valkey) = &self.valkey {
            match scry_valkey::discover_status_blobs(valkey).await {
                Ok(remote) => blobs.extend(remote),
                Err(e) => warn!(error = %e, "status fleet discovery failed; using local agents"),
            }
        }
        for blob in blobs {
            if let Ok(snapshot) = serde_json::from_str::<scry_server::StatusSnapshot>(&blob) {
                let key = format!("{}\0{}", snapshot.role, snapshot.instance_id);
                if by_id
                    .get(&key)
                    .is_none_or(|(timestamp, _)| snapshot.now_unix_ms >= *timestamp)
                {
                    by_id.insert(key, (snapshot.now_unix_ms, blob));
                }
            }
        }
        by_id.into_values().map(|(_, blob)| blob).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn report(owner: Uuid, sequence: u64, marker: u64) -> RemoteAgentStatus {
        RemoteAgentStatus {
            instance_id: "agent/node-1".into(),
            owner_token: owner,
            sequence,
            snapshot: scry_server::StatusSnapshot {
                role: "agent".into(),
                instance_id: "agent/node-1".into(),
                addr: "node-1".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                now_unix_ms: marker,
                uptime_secs: 1.0,
                rss_kib: None,
                data: json!({}),
            },
        }
    }

    #[test]
    fn successor_fences_old_updates_and_removal() {
        let registry = AgentStatusRegistry::default();
        let a = Uuid::now_v7();
        let b = Uuid::now_v7();
        assert!(registry.report(&report(a, 1, 1), "a".into()));
        assert!(registry.report(&report(b, 1, 2), "b".into()));
        assert!(!registry.report(&report(a, 2, 3), "late-a".into()));
        registry.remove("agent/node-1", a);
        assert_eq!(registry.blobs(), vec!["b"]);
        registry.remove("agent/node-1", b);
        assert!(registry.blobs().is_empty());
    }
}
