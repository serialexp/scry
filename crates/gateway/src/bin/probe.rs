//! `scry-gateway-probe`: emit fixture bodies for the gateway smoke test.
//!
//! Subcommands:
//!   otlp  <path> [n_spans]   write an OTLP/HTTP protobuf ExportTraceServiceRequest
//!                            (one resource + scope, `n_spans` spans; default 4),
//!                            then print `spans=<N>` so the smoke script knows the
//!                            per-request row count.
//!   pprof <path> [size]      write `size` bytes of opaque profile body (default
//!                            4096) — stored verbatim by the gateway, never parsed.
//!   promwrite <path> [series] [samples]
//!                            write a snappy-compressed Prometheus remote-write
//!                            WriteRequest (default 5 series × 4 samples), then
//!                            print `samples=<series*samples>`.
//!
//! The smoke script POSTs these fixtures with `curl` (the OTLP body to
//! `/v1/traces` as application/x-protobuf, the pprof body as multipart field
//! `profile` to `/ingest`, the remote-write body to `/api/v1/write` with
//! Content-Encoding: snappy).

use prost::Message;
use scry_gateway::{otlp, promwrite};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let usage = || -> ! {
        eprintln!("usage: scry-gateway-probe <otlp|pprof> <path> [count]");
        std::process::exit(2);
    };
    if args.len() < 3 {
        usage();
    }
    let cmd = args[1].as_str();
    let path = &args[2];

    match cmd {
        "otlp" => {
            let n_spans: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(4);
            let req = otlp::sample_request(n_spans);
            let bytes = req.encode_to_vec();
            std::fs::write(path, &bytes).expect("write otlp fixture");
            println!("spans={n_spans}");
        }
        "pprof" => {
            let size: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(4096);
            // Opaque bytes — the gateway stores them verbatim. Deterministic so
            // the fixture is reproducible across runs.
            let body: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
            std::fs::write(path, &body).expect("write pprof fixture");
            println!("bytes={size}");
        }
        "promwrite" => {
            let n_series: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(5);
            let n_samples: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(4);
            let req = promwrite::sample_request(n_series, n_samples);
            let body = promwrite::encode_snappy(&req);
            std::fs::write(path, &body).expect("write promwrite fixture");
            println!("samples={}", n_series * n_samples);
        }
        _ => usage(),
    }
}
