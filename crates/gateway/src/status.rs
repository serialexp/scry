use std::{sync::Arc, time::Instant};

use scry_status::{FleetSource, LocalStatus, StatusSnapshot};

use crate::{metrics::GatewayMetrics, sink::AppState};

pub struct GatewayStatus {
    instance_id: String,
    addr: String,
    http_addr: String,
    grpc_addr: Option<String>,
    wire_addr: Option<String>,
    started: Instant,
    state: AppState,
    metrics: Arc<GatewayMetrics>,
}

impl GatewayStatus {
    pub fn new(
        instance_id: String,
        http_addr: String,
        grpc_addr: Option<String>,
        wire_addr: Option<String>,
        state: AppState,
        metrics: Arc<GatewayMetrics>,
    ) -> Self {
        Self {
            instance_id,
            addr: http_addr.clone(),
            http_addr,
            grpc_addr,
            wire_addr,
            started: Instant::now(),
            state,
            metrics,
        }
    }
}

impl LocalStatus for GatewayStatus {
    fn snapshot(&self) -> StatusSnapshot {
        let mut data = self.metrics.snapshot(&self.state.queue_snapshots());
        let object = data
            .as_object_mut()
            .expect("gateway status data is an object");
        object.insert("listeners".into(), serde_json::json!({"http":self.http_addr,"otlp_grpc":self.grpc_addr,"wire":self.wire_addr}));
        StatusSnapshot {
            role: "gateway".into(),
            instance_id: self.instance_id.clone(),
            addr: self.addr.clone(),
            version: env!("CARGO_PKG_VERSION").into(),
            now_unix_ms: scry_status::unix_ms_now(),
            uptime_secs: self.started.elapsed().as_secs_f64(),
            rss_kib: scry_status::rss_kib(),
            data,
        }
    }
}

pub struct ValkeyFleetSource(pub scry_valkey::ValkeyClient);
#[async_trait::async_trait]
impl FleetSource for ValkeyFleetSource {
    async fn blobs(&self) -> Vec<String> {
        match scry_valkey::discover_status_blobs(&self.0).await {
            Ok(blobs) => blobs,
            Err(error) => {
                tracing::warn!(%error, "gateway status fleet discovery failed");
                Vec::new()
            }
        }
    }
}
