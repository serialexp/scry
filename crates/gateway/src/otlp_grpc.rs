//! OTLP/gRPC traces, logs, and scalar metrics ingestion.

use std::{net::SocketAddr, str::FromStr};

use anyhow::{Context, Result};
use opentelemetry_proto::tonic::collector::{
    logs::v1::{
        logs_service_server::{LogsService, LogsServiceServer},
        ExportLogsServiceRequest, ExportLogsServiceResponse,
    },
    metrics::v1::{
        metrics_service_server::{MetricsService, MetricsServiceServer},
        ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    },
    trace::v1::{
        trace_service_server::{TraceService, TraceServiceServer},
        ExportTraceServiceRequest, ExportTraceServiceResponse,
    },
};
use tonic::{codec::CompressionEncoding, transport::Server, Request, Response, Status};
use tracing::info;

use crate::{otlp, otlp_logs, otlp_metrics, AppState};

#[derive(Clone)]
pub struct OtlpTraceService {
    state: AppState,
}
#[derive(Clone)]
pub struct OtlpLogsService {
    state: AppState,
}
#[derive(Clone)]
pub struct OtlpMetricsService {
    state: AppState,
}

impl OtlpTraceService {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }
}
impl OtlpLogsService {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }
}
impl OtlpMetricsService {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }
}

fn accepted(state: &AppState) {
    if let Some(metrics) = state.metrics() {
        metrics.inbound_accepted(crate::metrics::Inbound::OtlpGrpc);
    }
}

#[tonic::async_trait]
impl TraceService for OtlpTraceService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        accepted(&self.state);
        otlp::accept(&self.state, request.into_inner());
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}
#[tonic::async_trait]
impl LogsService for OtlpLogsService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        accepted(&self.state);
        Ok(Response::new(otlp_logs::accept(
            &self.state,
            request.into_inner(),
        )))
    }
}
#[tonic::async_trait]
impl MetricsService for OtlpMetricsService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        accepted(&self.state);
        Ok(Response::new(otlp_metrics::accept(
            &self.state,
            request.into_inner(),
        )))
    }
}

pub async fn serve<F>(listen_addr: String, state: AppState, shutdown: F) -> Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let addr = SocketAddr::from_str(&listen_addr)
        .with_context(|| format!("invalid OTLP/gRPC listen address {listen_addr}"))?;
    info!(addr = %addr, "scry-gateway OTLP/gRPC listener ready");
    Server::builder()
        .add_service(
            TraceServiceServer::new(OtlpTraceService::new(state.clone()))
                .accept_compressed(CompressionEncoding::Gzip)
                .max_decoding_message_size(crate::otlp_common::MAX_OTLP_BODY_BYTES),
        )
        .add_service(
            LogsServiceServer::new(OtlpLogsService::new(state.clone()))
                .accept_compressed(CompressionEncoding::Gzip)
                .max_decoding_message_size(crate::otlp_common::MAX_OTLP_BODY_BYTES),
        )
        .add_service(
            MetricsServiceServer::new(OtlpMetricsService::new(state))
                .accept_compressed(CompressionEncoding::Gzip)
                .max_decoding_message_size(crate::otlp_common::MAX_OTLP_BODY_BYTES),
        )
        .serve_with_shutdown(addr, shutdown)
        .await
        .context("OTLP/gRPC server error")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn all_services_acknowledge_exports_and_metrics_report_partial_success() {
        let state = AppState::new(Vec::new());
        let trace = OtlpTraceService::new(state.clone())
            .export(Request::new(crate::otlp::sample_request(1)))
            .await
            .unwrap();
        assert!(trace.into_inner().partial_success.is_none());
        let logs = OtlpLogsService::new(state.clone())
            .export(Request::new(crate::otlp_logs::sample_request(1)))
            .await
            .unwrap();
        assert!(logs.into_inner().partial_success.is_none());
        let metrics = OtlpMetricsService::new(state)
            .export(Request::new(crate::otlp_metrics::sample_request(1)))
            .await
            .unwrap();
        assert!(metrics.into_inner().partial_success.is_none());
    }
}
