//! OTLP/gRPC trace ingest.
//!
//! Caddy's built-in OpenTelemetry integration exports over gRPC, while the
//! existing gateway endpoint accepts OTLP/HTTP protobuf. Both transports feed
//! the same decoded request into [`crate::otlp::accept`], keeping mapping and
//! best-effort fan-out semantics identical.

use std::net::SocketAddr;
use std::str::FromStr;

use anyhow::{Context, Result};
use opentelemetry_proto::tonic::collector::trace::v1::{
    trace_service_server::{TraceService, TraceServiceServer},
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use tonic::{transport::Server, Request, Response, Status};
use tracing::info;

use crate::{otlp, AppState};

#[derive(Clone)]
pub struct OtlpTraceService {
    state: AppState,
}

impl OtlpTraceService {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl TraceService for OtlpTraceService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        if let Some(metrics) = self.state.metrics() {
            metrics.inbound_accepted(crate::metrics::Inbound::OtlpGrpc);
        }
        otlp::accept(&self.state, request.into_inner());
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

/// Serve OTLP/gRPC until `shutdown` resolves.
pub async fn serve<F>(listen_addr: String, state: AppState, shutdown: F) -> Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let addr = SocketAddr::from_str(&listen_addr)
        .with_context(|| format!("invalid OTLP/gRPC listen address {listen_addr}"))?;
    info!(addr = %addr, "scry-gateway OTLP/gRPC listener ready");
    Server::builder()
        .add_service(TraceServiceServer::new(OtlpTraceService::new(state)))
        .serve_with_shutdown(addr, shutdown)
        .await
        .context("OTLP/gRPC server error")
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
    use scry_proto::constants::SIGNAL_BIT_TRACES;
    use tokio::sync::{mpsc, oneshot};

    use super::*;
    use crate::sink::spawn_sink;

    #[tokio::test]
    async fn export_accepts_the_same_decoded_request_as_http() {
        let service = OtlpTraceService::new(AppState::new(Vec::new()));
        let response = service
            .export(Request::new(crate::otlp::sample_request(2)))
            .await
            .expect("OTLP/gRPC export should be acknowledged");

        assert!(response.into_inner().partial_success.is_none());
    }

    #[tokio::test]
    async fn export_accepts_an_empty_batch() {
        let service = OtlpTraceService::new(AppState::new(Vec::new()));
        let response = service
            .export(Request::new(crate::otlp::sample_request(0)))
            .await
            .expect("empty OTLP/gRPC exports are valid");

        assert!(response.into_inner().partial_success.is_none());
    }

    #[tokio::test]
    async fn grpc_wire_export_reaches_the_bounded_fanout() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve test port");
        let addr = listener.local_addr().expect("test listener address");
        drop(listener);

        let (offered_tx, mut offered_rx) = mpsc::channel(1);
        let sink = spawn_sink("capture", SIGNAL_BIT_TRACES, 1, move |mut rx| async move {
            if rx.recv().await.is_some() {
                let _ = offered_tx.send(()).await;
            }
        });
        let state = AppState::new(vec![sink]);
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server = tokio::spawn(serve(addr.to_string(), state, async move {
            let _ = shutdown_rx.await;
        }));

        let endpoint = format!("http://{addr}");
        let mut client = loop {
            match TraceServiceClient::connect(endpoint.clone()).await {
                Ok(client) => break client,
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
            }
        };
        let response = client
            .export(crate::otlp::sample_request(2))
            .await
            .expect("wire export should be acknowledged");
        assert!(response.into_inner().partial_success.is_none());
        tokio::time::timeout(std::time::Duration::from_secs(1), offered_rx.recv())
            .await
            .expect("trace batch should reach fanout before timeout")
            .expect("capture sink should remain alive");

        shutdown_tx.send(()).expect("signal test server shutdown");
        server
            .await
            .expect("test server task should join")
            .expect("test server should stop cleanly");
    }
}
