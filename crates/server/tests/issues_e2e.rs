//! Issue request handlers over the real query wire.
//!
//! Covers `IssueListRequest` and `IssueOccurrencesRequest` against a
//! `QueryService` with no errors database, with a configured but not yet
//! installed one, and with populated databases: ordering, tie-breaks, the
//! server-side row cap, unknown and malformed issue IDs, a handle swap under a
//! running server, and the oversize-response `StreamError`.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use object_store::{memory::InMemory, ObjectStore};
use scry_block::AlwaysValid;
use scry_catalog::Catalog;
use scry_errors::handle::SharedErrorsDb;
use scry_errors::projection::{occurrence_keys, OccurrenceCommit};
use scry_errors::sqlite::{
    ErrorsDb, GroupedOccurrence, OccurrenceRow, UngroupedOccurrence, MAX_ISSUE_PAGE_ROWS,
};
use scry_objstore::BufPool;
use scry_proto::constants::{
    QUERY_ERR_BAD_REQUEST, QUERY_ERR_ISSUES_UNAVAILABLE, QUERY_ERR_RESOURCES,
};
use scry_proto::framing::{read_frame, write_frame};
use scry_proto::{IssueListRequestInput, IssueOccurrencesRequestInput, QueryFrame, QueryFrameMsg};
use scry_query::{BloomCache, PostingsCache};
use scry_server::QueryService;
use tempfile::TempDir;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use uuid::Uuid;

const DEPLOYMENT: [u8; 16] = [1; 16];
const GENERATION: &str = "fp-test";

struct Server {
    addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
    _tmp: TempDir,
}

impl Server {
    async fn start(errors: Option<SharedErrorsDb>) -> Self {
        let tmp = TempDir::new().unwrap();
        let catalog = Catalog::open(&tmp.path().join("cat.sqlite"), "test").unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let memory_pool = Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024));
        let runtime_env = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(memory_pool.clone())
                .build()
                .unwrap(),
        );
        let service = Arc::new(
            QueryService::new(
                Arc::new(Mutex::new(catalog)),
                store,
                BufPool::new(),
                Arc::new(PostingsCache::with_budget_bytes(1024 * 1024)),
                Arc::new(BloomCache::with_budget_bytes(1024 * 1024)),
                runtime_env,
                memory_pool,
                Arc::new(scry_query::QueryResultCache::with_budget_bytes(0)),
                scry_query::DEFAULT_QUERY_CACHE_ENTRY_BYTES,
            )
            .with_errors_db(errors),
        );
        let probe = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let (shutdown, shutdown_rx) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            service
                .serve_with_shutdown(addr, async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        for _ in 0..100 {
            if TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Self {
            addr,
            shutdown: Some(shutdown),
            task,
            _tmp: tmp,
        }
    }

    async fn stop(mut self) {
        let _ = self.shutdown.take().unwrap().send(());
        self.task.await.unwrap().unwrap();
    }

    async fn request(&self, msg: QueryFrameMsg) -> QueryFrameMsg {
        let (r, w) = TcpStream::connect(self.addr).await.unwrap().into_split();
        let mut r = BufReader::new(r);
        let mut w = BufWriter::new(w);
        write_frame(&mut w, &QueryFrame { msg }).await.unwrap();
        w.flush().await.unwrap();
        let response: QueryFrame = read_frame(&mut r).await.unwrap();
        response.msg
    }

    async fn list(&self, limit: u32) -> QueryFrameMsg {
        self.request(QueryFrameMsg::IssueListRequest(
            IssueListRequestInput { limit }.into(),
        ))
        .await
    }

    async fn occurrences(&self, issue_id: Vec<u8>, limit: u32) -> QueryFrameMsg {
        self.request(QueryFrameMsg::IssueOccurrencesRequest(
            IssueOccurrencesRequestInput { issue_id, limit }.into(),
        ))
        .await
    }
}

fn stream_error(msg: QueryFrameMsg) -> (u16, String) {
    match msg {
        QueryFrameMsg::StreamError(error) => (error.code, error.message),
        other => panic!("expected StreamError, got {other:?}"),
    }
}

fn issues(msg: QueryFrameMsg) -> Vec<serde_json::Value> {
    match msg {
        QueryFrameMsg::IssueListResponse(response) => response
            .issues_json
            .iter()
            .map(|json| serde_json::from_str(json).unwrap())
            .collect(),
        other => panic!("expected IssueListResponse, got {other:?}"),
    }
}

fn occurrences(msg: QueryFrameMsg) -> (String, Vec<serde_json::Value>) {
    match msg {
        QueryFrameMsg::IssueOccurrencesResponse(response) => (
            response.issue_json,
            response
                .occurrences_json
                .iter()
                .map(|json| serde_json::from_str(json).unwrap())
                .collect(),
        ),
        other => panic!("expected IssueOccurrencesResponse, got {other:?}"),
    }
}

/// An event ID whose first two bytes name its issue and `unique` separates it.
fn event(issue: u16, unique: u16) -> [u8; 16] {
    let mut id = [0_u8; 16];
    id[..2].copy_from_slice(&issue.to_be_bytes());
    id[14..].copy_from_slice(&unique.to_be_bytes());
    id
}

/// Fold `(event, occurred_at)` occurrences in one commit, then group each into
/// the issue named by its event's first two bytes with `title_bytes`-long titles.
fn populate(db: &mut ErrorsDb, commit_seed: u128, events: &[([u8; 16], u64)], title_bytes: usize) {
    db.set_max_rows_per_transaction(events.len().max(1));
    let source = Uuid::from_u128(commit_seed);
    let keys = occurrence_keys("2026-09-10", source, "occurrence-v1").unwrap();
    let commit = OccurrenceCommit::new(
        source,
        "occurrence-v1",
        keys.data,
        b"data",
        events.len() as u64,
        1,
    )
    .unwrap();
    let json = commit.canonical_json().unwrap();
    let rows = events.iter().map(|(event_id, occurred)| OccurrenceRow {
        deployment_id: &DEPLOYMENT,
        app_id: &[2; 16],
        app_identity_sha256: &[3; 32],
        event_id,
        occurred_at_unix_nano: *occurred,
        observed_at_unix_nano: None,
        received_at_unix_nano: *occurred,
        trace_id: None,
        span_id: None,
        trace_flags: 0,
        canonical_version: 1,
        scrub_policy_version: 1,
        canonical_sha256: &[4; 32],
        canonical: b"occ",
        source_log_block_uuid: source.as_bytes(),
        source_row_ordinal: 0,
    });
    db.fold_committed(
        "2026-09-10",
        &keys.commit,
        &commit,
        &json,
        rows,
        &AlwaysValid,
    )
    .unwrap();
    loop {
        let report = db
            .group_page(
                GENERATION,
                1_000,
                &AlwaysValid,
                |o: &UngroupedOccurrence<'_>| {
                    let mut issue_id = [0_u8; 16];
                    issue_id[..2].copy_from_slice(&o.event_id[..2]);
                    Ok(GroupedOccurrence {
                        issue_id,
                        fingerprint_version: 2,
                        fingerprint_digest: [0; 32],
                        grouping_quality: 0,
                        title: format!("{:0>width$}", issue_id[1], width = title_bytes),
                        severity: 17,
                    })
                },
            )
            .unwrap();
        if report.drained {
            break;
        }
    }
}

fn issue_id(issue: u16) -> Vec<u8> {
    let mut id = vec![0_u8; 16];
    id[..2].copy_from_slice(&issue.to_be_bytes());
    id
}

#[tokio::test]
async fn issues_unavailable_without_a_database() {
    let server = Server::start(None).await;
    let (code, message) = stream_error(server.list(0).await);
    assert_eq!(code, QUERY_ERR_ISSUES_UNAVAILABLE);
    assert!(message.contains("--errors-db"), "{message}");
    let (code, _) = stream_error(server.occurrences(issue_id(1), 0).await);
    assert_eq!(code, QUERY_ERR_ISSUES_UNAVAILABLE);
    server.stop().await;
}

#[tokio::test]
async fn configured_handle_serves_once_a_database_is_installed() {
    let handle = SharedErrorsDb::new();
    let server = Server::start(Some(handle.clone())).await;
    let (code, message) = stream_error(server.list(0).await);
    assert_eq!(code, QUERY_ERR_ISSUES_UNAVAILABLE);
    assert!(message.contains("installed"), "{message}");

    let mut db = ErrorsDb::open_in_memory(DEPLOYMENT).unwrap();
    populate(&mut db, 1, &[(event(7, 1), 1_000)], 8);
    handle.replace(Some(db));
    assert_eq!(issues(server.list(0).await).len(), 1);
    server.stop().await;
}

#[tokio::test]
async fn list_orders_by_last_seen_and_caps_the_limit() {
    let mut db = ErrorsDb::open_in_memory(DEPLOYMENT).unwrap();
    // MAX + 5 issues, issue n last seen at n.
    let events: Vec<_> = (1..=(MAX_ISSUE_PAGE_ROWS as u16 + 5))
        .map(|n| (event(n, 1), u64::from(n) * 1_000))
        .collect();
    populate(&mut db, 1, &events, 8);
    let server = Server::start(Some(SharedErrorsDb::with_db(db))).await;

    let default_page = issues(server.list(0).await);
    assert_eq!(default_page.len(), 100);
    let last_seen: Vec<u64> = default_page
        .iter()
        .map(|issue| issue["last_seen_unix_nano"].as_u64().unwrap())
        .collect();
    assert!(last_seen.windows(2).all(|w| w[0] > w[1]));
    assert_eq!(last_seen[0], (MAX_ISSUE_PAGE_ROWS as u64 + 5) * 1_000);

    assert_eq!(issues(server.list(3).await).len(), 3);
    assert_eq!(
        issues(server.list(u32::MAX).await).len(),
        MAX_ISSUE_PAGE_ROWS
    );
    server.stop().await;
}

#[tokio::test]
async fn occurrences_are_newest_first_with_event_tie_break() {
    let mut db = ErrorsDb::open_in_memory(DEPLOYMENT).unwrap();
    populate(
        &mut db,
        1,
        &[
            (event(9, 1), 1_000),
            (event(9, 3), 2_000),
            (event(9, 2), 2_000),
            (event(8, 1), 5_000),
        ],
        8,
    );
    let server = Server::start(Some(SharedErrorsDb::with_db(db))).await;

    let (issue_json, rows) = occurrences(server.occurrences(issue_id(9), 0).await);
    let issue: serde_json::Value = serde_json::from_str(&issue_json).unwrap();
    assert_eq!(issue["occurrence_count"], 3);
    assert_eq!(issue["first_seen_unix_nano"], 1_000);
    let order: Vec<(u64, String)> = rows
        .iter()
        .map(|row| {
            (
                row["occurred_at_unix_nano"].as_u64().unwrap(),
                row["event_id"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    assert_eq!(order.len(), 3);
    assert_eq!(order[0].0, 2_000);
    assert!(order[0].1.ends_with("0003"), "{order:?}");
    assert!(order[1].1.ends_with("0002"), "{order:?}");
    assert_eq!(order[2].0, 1_000);

    let (_, limited) = occurrences(server.occurrences(issue_id(9), 1).await);
    assert_eq!(limited.len(), 1);

    let (unknown_issue, unknown_rows) = occurrences(server.occurrences(issue_id(42), 0).await);
    assert_eq!(unknown_issue, "");
    assert!(unknown_rows.is_empty());

    let (code, _) = stream_error(server.occurrences(vec![1, 2, 3], 0).await);
    assert_eq!(code, QUERY_ERR_BAD_REQUEST);
    server.stop().await;
}

#[tokio::test]
async fn oversized_issue_response_is_a_resources_error() {
    let mut db = ErrorsDb::open_in_memory(DEPLOYMENT).unwrap();
    // 1,000 issues with 40 KiB titles: ~40 MiB of JSON, over the 32 MiB frame
    // limit. Grouping normally bounds titles; this database bypasses it.
    let events: Vec<_> = (1..=MAX_ISSUE_PAGE_ROWS as u16)
        .map(|n| (event(n, 1), u64::from(n)))
        .collect();
    populate(&mut db, 1, &events, 40 * 1024);
    let server = Server::start(Some(SharedErrorsDb::with_db(db))).await;
    let (code, message) = stream_error(server.list(u32::MAX).await);
    assert_eq!(code, QUERY_ERR_RESOURCES);
    assert!(message.contains("frame limit"), "{message}");
    // A smaller page of the same database still succeeds.
    assert_eq!(issues(server.list(10).await).len(), 10);
    server.stop().await;
}
