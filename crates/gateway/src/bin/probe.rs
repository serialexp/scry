//! Deterministic fixture generator and OTLP/gRPC driver for gateway smoke tests.

use std::io::Write;

use prost::Message;
use scry_gateway::{
    loki::{LokiPushRequest, LokiStream, LokiValue},
    loki_ingest, otlp, otlp_logs, otlp_metrics, promwrite, pyroscope_push,
};
use serde::Serialize;

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).expect("gzip fixture");
    encoder.finish().expect("finish gzip fixture")
}

fn encoded<T: Message + Serialize>(value: &T, representation: &str) -> Vec<u8> {
    match representation {
        "proto" => value.encode_to_vec(),
        "json" => serde_json::to_vec(value).expect("JSON fixture"),
        "proto-gzip" => gzip(&value.encode_to_vec()),
        "json-gzip" => gzip(&serde_json::to_vec(value).expect("JSON fixture")),
        _ => panic!("representation must be proto, json, proto-gzip, or json-gzip"),
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let usage = || -> ! {
        eprintln!("usage: scry-gateway-probe <otlp-traces|otlp-logs|otlp-metrics> <path> <proto|json|proto-gzip|json-gzip> [records]\n       scry-gateway-probe <loki-json|loki-proto|pprof|pyroscope-push|promwrite> <path> [counts...]\n       scry-gateway-probe grpc <endpoint> <traces|logs|metrics> <requests> <records>");
        std::process::exit(2);
    };
    if args.len() < 3 {
        usage();
    }
    match args[1].as_str() {
        "otlp-traces" | "otlp-logs" | "otlp-metrics" => {
            let representation = args.get(3).map(String::as_str).unwrap_or("proto");
            let records = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(4);
            let bytes = match args[1].as_str() {
                "otlp-traces" => encoded(&otlp::sample_request(records), representation),
                "otlp-logs" => encoded(&otlp_logs::sample_request(records), representation),
                _ => encoded(&otlp_metrics::sample_request(records), representation),
            };
            std::fs::write(&args[2], bytes).expect("write OTLP fixture");
            println!("records={records}");
        }
        "loki-json" => {
            let records = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(4);
            let mut values = Vec::with_capacity(records);
            for index in 0..records {
                values.push(LokiValue {
                    ts_unix_nano: (1_700_300_100_000_000_000u64 + index as u64).to_string(),
                    line: format!("loki json {index}"),
                    metadata: [("source".into(), "smoke".into())].into_iter().collect(),
                });
            }
            let request = LokiPushRequest {
                streams: vec![LokiStream {
                    stream: [("service".into(), "loki-json".into())]
                        .into_iter()
                        .collect(),
                    values,
                }],
            };
            std::fs::write(&args[2], serde_json::to_vec(&request).unwrap()).unwrap();
            println!("records={records}");
        }
        "loki-proto" => {
            let records = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(4);
            std::fs::write(
                &args[2],
                loki_ingest::encode_proto_snappy(&loki_ingest::sample_proto_request(records)),
            )
            .unwrap();
            println!("records={records}");
        }
        "pprof" => {
            let size: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(4096);
            let body: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
            std::fs::write(&args[2], body).unwrap();
            println!("bytes={size}");
        }
        "pyroscope-push" => {
            let records = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(2);
            let representation = args.get(4).map(String::as_str).unwrap_or("proto");
            std::fs::write(
                &args[2],
                encoded(&pyroscope_push::sample_request(records), representation),
            )
            .unwrap();
            println!("records={records}");
        }
        "promwrite" => {
            let series = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(5);
            let samples = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(4);
            std::fs::write(
                &args[2],
                promwrite::encode_snappy(&promwrite::sample_request(series, samples)),
            )
            .unwrap();
            println!("samples={}", series * samples);
        }
        "grpc" => grpc(&args).await,
        _ => usage(),
    }
}

async fn grpc(args: &[String]) {
    use opentelemetry_proto::tonic::collector::{
        logs::v1::logs_service_client::LogsServiceClient,
        metrics::v1::metrics_service_client::MetricsServiceClient,
        trace::v1::trace_service_client::TraceServiceClient,
    };
    let endpoint = args.get(2).expect("endpoint").clone();
    let signal = args.get(3).expect("signal").as_str();
    let requests: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(1);
    let records = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(4);
    match signal {
        "traces" => {
            let mut client = TraceServiceClient::connect(endpoint)
                .await
                .unwrap()
                .send_compressed(tonic::codec::CompressionEncoding::Gzip);
            for _ in 0..requests {
                client.export(otlp::sample_request(records)).await.unwrap();
            }
        }
        "logs" => {
            let mut client = LogsServiceClient::connect(endpoint)
                .await
                .unwrap()
                .send_compressed(tonic::codec::CompressionEncoding::Gzip);
            for _ in 0..requests {
                client
                    .export(otlp_logs::sample_request(records))
                    .await
                    .unwrap();
            }
        }
        "metrics" => {
            let mut client = MetricsServiceClient::connect(endpoint)
                .await
                .unwrap()
                .send_compressed(tonic::codec::CompressionEncoding::Gzip);
            for _ in 0..requests {
                client
                    .export(otlp_metrics::sample_request(records))
                    .await
                    .unwrap();
            }
        }
        _ => panic!("signal must be traces, logs, or metrics"),
    }
    println!("records={}", requests * records);
}
