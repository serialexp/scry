//! Scale harness for the errors database: folding, grouping throughput, and
//! the issue list / issue detail queries at a realistic size.
//!
//! Not a correctness test; a stopwatch with budgets. The default scale is one
//! million occurrences across ten thousand issues, skewed so one hot issue owns
//! a tenth of all occurrences (the adversarial case for the detail query).
//! Occurrences are real OCC1 bytes produced by the extractor and grouped by the
//! production callback (`engine::group_occurrence`), in production-sized pages,
//! against a file-backed WAL database.
//!
//! Ignored by default because it takes tens of seconds. Run it in release mode:
//!
//! ```text
//! cargo test --release -p scry-errors --test scale -- --ignored --nocapture
//! ```
//!
//! `N_OCCURRENCES` and `N_ISSUES` override the scale.

use std::path::Path;
use std::time::{Duration, Instant};

use scry_block::AlwaysValid;
use scry_errors::engine::group_occurrence;
use scry_errors::projection::{occurrence_keys, OccurrenceCommit};
use scry_errors::sqlite::{
    ErrorsDb, GroupingReport, OccurrenceRow, DEFAULT_MAX_ROWS_PER_TRANSACTION, MAX_ISSUE_PAGE_ROWS,
};
use scry_errors::{extract, DeploymentId, Limits, Occurrence, Scratch};
use scry_proto::{
    encode_log_record_v2_into, validate_logs_v2_record, LogRecordInput,
    LogsV2AnyValueInput as Value, LogsV2DecodeLimits, LogsV2KeyValueInput as Kv,
};
use uuid::Uuid;

const DEPLOYMENT: &str = "018f1f8e-7b2c-7a91-8123-abcdef012345";
const GENERATION: &str = "fp-v2";
/// Production grouping page size (`ReconcileConfig::grouping_page_rows`).
const GROUPING_PAGE_ROWS: usize = 1024;
const BASE_NANOS: u64 = 1_758_000_000_000_000_000;

// Budgets at the default scale, release build. They are deliberately loose:
// they catch pathological regressions (a lost index, a temp sort, a per-row
// round trip, per-occurrence issue rewrites), not few-percent drift.
//
// Measured on the home development machine (2026-09-26), default scale:
// fold ~145k rows/s; grouping 15k-30k rows/s depending on concurrent machine
// load (1M occurrences over 10k issues is write-bound on random leaf pages of
// `occurrence_issues_by_issue`); 95k-118k rows/s with 50-100 issues; slowest
// grouping page 0.3-1.2 s (a WAL checkpoint); every read median under 1 ms.
const MIN_GROUPING_ROWS_PER_SEC: f64 = 10_000.0;
const MAX_GROUPING_PAGE: Duration = Duration::from_secs(2);
const MAX_READ_QUERY: Duration = Duration::from_millis(50);

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// A short lowercase word for `n`. Letters only, so message normalization
/// cannot collapse distinct issues into one the way digits would.
fn word(mut n: usize, out: &mut String) {
    out.clear();
    loop {
        out.push(char::from(b'a' + (n % 26) as u8));
        n /= 26;
        if n == 0 {
            break;
        }
    }
}

/// Issue index for occurrence `i`: every tenth occurrence is the hot issue 0,
/// the rest spread uniformly over the remaining issues.
fn issue_for(i: usize, issues: usize) -> usize {
    if i.is_multiple_of(10) || issues == 1 {
        0
    } else {
        1 + i % (issues - 1)
    }
}

/// Builds occurrences through the real encoder and extractor, reusing the
/// record, text, and attribute buffers across calls.
struct Generator {
    deployment: DeploymentId,
    encoded: Vec<u8>,
    scratch: Scratch,
    event_id: String,
    exception_type: String,
    message: String,
    word: String,
}

impl Generator {
    fn new() -> Self {
        Self {
            deployment: DeploymentId::parse(DEPLOYMENT, "deployment").unwrap(),
            encoded: Vec::with_capacity(1024),
            scratch: Scratch::with_capacity(1024),
            event_id: String::with_capacity(36),
            exception_type: String::with_capacity(32),
            message: String::with_capacity(64),
            word: String::with_capacity(8),
        }
    }

    fn occurrence(&mut self, i: usize, issue: usize) -> Occurrence {
        use std::fmt::Write;
        self.event_id.clear();
        let event = Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0000_0000 | (i as u128 + 1));
        write!(self.event_id, "{event}").unwrap();
        word(issue, &mut self.word);
        self.exception_type.clear();
        write!(self.exception_type, "Error{}", self.word).unwrap();
        self.message.clear();
        write!(
            self.message,
            "request {} failed for shard {} after {}ms",
            self.event_id,
            self.word,
            // Four or more digits: normalization keeps shorter numbers verbatim.
            1_000 + i % 9_000
        )
        .unwrap();
        let resource = [Kv {
            key: "service.name",
            value: Value::String("checkout"),
        }];
        // Keys in byte order, as the encoder requires.
        let attributes = [
            Kv {
                key: "exception.message",
                value: Value::String(&self.message),
            },
            Kv {
                key: "exception.stacktrace",
                value: Value::String(
                    "at handler (src/checkout.ts:42:7)\nat router (src/router.ts:118:3)",
                ),
            },
            Kv {
                key: "exception.type",
                value: Value::String(&self.exception_type),
            },
            Kv {
                key: "scry.event.id",
                value: Value::String(&self.event_id),
            },
        ];
        let time = BASE_NANOS + i as u64 * 1_000_000;
        let input = LogRecordInput {
            resource_schema_url: "",
            resource_dropped_attributes_count: 0,
            resource_attributes: &resource,
            scope: None,
            scope_schema_url: "",
            time_unix_nano: time,
            observed_time_unix_nano: time + 100,
            severity: 17 + (i % 8) as u8,
            severity_text: "ERROR",
            event_name: "exception",
            body: Value::String("request failed"),
            dropped_attributes_count: 0,
            attributes: &attributes,
            trace_flags: 1,
            trace_id: Some(&[5; 16]),
            span_id: Some(&[6; 8]),
        };
        self.encoded.clear();
        encode_log_record_v2_into(&input, LogsV2DecodeLimits::default(), &mut self.encoded)
            .unwrap();
        let record = validate_logs_v2_record(&self.encoded, LogsV2DecodeLimits::default()).unwrap();
        extract(
            record,
            self.deployment,
            Limits::default(),
            &mut self.scratch,
        )
        .unwrap()
    }
}

/// Fold `total` occurrences as one projection commit per
/// `DEFAULT_MAX_ROWS_PER_TRANSACTION` rows (one source block each).
fn fold_all(db: &mut ErrorsDb, total: usize, issues: usize) -> Duration {
    let mut generator = Generator::new();
    let mut batch: Vec<(Occurrence, u64)> = Vec::with_capacity(DEFAULT_MAX_ROWS_PER_TRANSACTION);
    let mut folding = Duration::ZERO;
    let mut next = 0;
    let mut block = 0_u128;
    while next < total {
        for occurrence in batch.drain(..) {
            generator.scratch.recycle(occurrence.0);
        }
        let end = (next + DEFAULT_MAX_ROWS_PER_TRANSACTION).min(total);
        for i in next..end {
            let occurrence = generator.occurrence(i, issue_for(i, issues));
            batch.push((occurrence, BASE_NANOS + i as u64 * 1_000_000));
        }
        block += 1;
        let source = Uuid::from_u128(0x018f_0000_0000_7000_9000_0000_0000_0000 | block);
        let keys = occurrence_keys("2026-09-26", source, "v1").unwrap();
        let commit = OccurrenceCommit::new(
            source,
            "v1",
            keys.data.clone(),
            b"data",
            batch.len() as u64,
            BASE_NANOS,
        )
        .unwrap();
        let json = commit.canonical_json().unwrap();
        let rows = batch.iter().map(|(occurrence, time)| OccurrenceRow {
            deployment_id: occurrence.deployment_id.as_bytes(),
            app_id: &occurrence.app.app_id,
            app_identity_sha256: &occurrence.app.digest,
            event_id: occurrence.event_id.as_bytes(),
            occurred_at_unix_nano: *time,
            observed_at_unix_nano: Some(*time + 100),
            received_at_unix_nano: *time + 1_000,
            trace_id: occurrence.trace_id.as_ref().map(|id| id.as_slice()),
            span_id: occurrence.span_id.as_ref().map(|id| id.as_slice()),
            trace_flags: occurrence.trace_flags,
            canonical_version: scry_errors::CANONICAL_VERSION,
            scrub_policy_version: scry_errors::SCRUB_POLICY_VERSION,
            canonical_sha256: &occurrence.canonical_sha256,
            canonical: &occurrence.canonical,
            source_log_block_uuid: source.as_bytes(),
            source_row_ordinal: 0,
        });
        let started = Instant::now();
        let report = db
            .fold_committed(
                "2026-09-26",
                &keys.commit,
                &commit,
                &json,
                rows,
                &AlwaysValid,
            )
            .unwrap();
        folding += started.elapsed();
        assert_eq!(report.inserted, end - next);
        next = end;
    }
    folding
}

/// Group the whole backlog in production-sized pages. Returns the report, the
/// total and slowest-page wall time, and the time spent fingerprinting.
fn group_all(db: &mut ErrorsDb) -> (GroupingReport, Duration, Duration, Duration) {
    let deployment = *DeploymentId::parse(DEPLOYMENT, "deployment")
        .unwrap()
        .as_bytes();
    let mut total = GroupingReport::default();
    let mut slowest = Duration::ZERO;
    let mut fingerprinting = Duration::ZERO;
    let started = Instant::now();
    loop {
        let page_started = Instant::now();
        let page = db
            .group_page(GENERATION, GROUPING_PAGE_ROWS, &AlwaysValid, |occurrence| {
                let fingerprint_started = Instant::now();
                let grouped = group_occurrence(&deployment, occurrence);
                fingerprinting += fingerprint_started.elapsed();
                grouped
            })
            .unwrap();
        slowest = slowest.max(page_started.elapsed());
        total.absorb(page);
        if page.drained {
            break;
        }
    }
    (total, started.elapsed(), slowest, fingerprinting)
}

/// Runs `query` `iterations` times; returns (median, max).
fn time_query(iterations: usize, mut query: impl FnMut()) -> (Duration, Duration) {
    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started = Instant::now();
        query();
        samples.push(started.elapsed());
    }
    samples.sort_unstable();
    (samples[samples.len() / 2], samples[samples.len() - 1])
}

fn file_bytes(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

#[test]
#[ignore = "scale harness: run with --release -- --ignored --nocapture"]
fn grouping_and_issue_queries_at_scale() {
    let occurrences = env_usize("N_OCCURRENCES", 1_000_000);
    let issues = env_usize("N_ISSUES", 10_000).max(1);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("errors.sqlite");
    let deployment = *DeploymentId::parse(DEPLOYMENT, "deployment")
        .unwrap()
        .as_bytes();
    let mut db = ErrorsDb::open(&path, deployment).unwrap();

    let generation_started = Instant::now();
    let folding = fold_all(&mut db, occurrences, issues);
    let generation = generation_started.elapsed() - folding;
    println!(
        "fold:     {occurrences} occurrences in {:.2?} ({:.0} rows/s; input generation {:.2?})",
        folding,
        occurrences as f64 / folding.as_secs_f64(),
        generation
    );

    let (grouped, grouping, slowest_page, fingerprinting) = group_all(&mut db);
    let rate = grouped.scanned as f64 / grouping.as_secs_f64();
    println!(
        "group:    {} occurrences into {} issues in {grouping:.2?} ({rate:.0} rows/s; \
         {fingerprinting:.2?} fingerprinting; slowest {GROUPING_PAGE_ROWS}-row page \
         {slowest_page:.2?})",
        grouped.scanned, grouped.issues_created
    );
    assert_eq!(grouped.scanned, occurrences);
    assert_eq!(grouped.occurrences_grouped, occurrences);
    assert_eq!(grouped.failures, 0);
    assert_eq!(grouped.issues_created, issues.min(occurrences));

    let listed = db.list_issues(MAX_ISSUE_PAGE_ROWS).unwrap();
    assert_eq!(listed.len(), MAX_ISSUE_PAGE_ROWS.min(issues));
    let hot = listed
        .iter()
        .max_by_key(|issue| issue.occurrence_count)
        .unwrap();
    let hot_id = *Uuid::parse_str(&hot.issue_id).unwrap().as_bytes();
    let cold_id = *Uuid::parse_str(&listed[listed.len() - 1].issue_id)
        .unwrap()
        .as_bytes();
    println!(
        "issues:   hot issue has {} occurrences; coldest listed has {}",
        hot.occurrence_count,
        listed[listed.len() - 1].occurrence_count
    );

    let mut reads = Vec::new();
    for (name, limit) in [("list_issues", 100), ("list_issues", MAX_ISSUE_PAGE_ROWS)] {
        let timing = time_query(50, || {
            assert!(!db.list_issues(limit).unwrap().is_empty());
        });
        reads.push((format!("{name}({limit})"), timing));
    }
    let timing = time_query(200, || {
        assert!(db.get_issue(&hot_id).unwrap().is_some());
    });
    reads.push(("get_issue".to_owned(), timing));
    for (label, issue_id) in [("hot", hot_id), ("cold", cold_id)] {
        for limit in [100, MAX_ISSUE_PAGE_ROWS] {
            let timing = time_query(50, || {
                let page = db.list_occurrences_for_issue(&issue_id, limit).unwrap();
                assert!(!page.is_empty());
                assert!(page
                    .windows(2)
                    .all(|w| w[0].occurred_at_unix_nano >= w[1].occurred_at_unix_nano));
            });
            reads.push((format!("occurrences({label}, {limit})"), timing));
        }
    }
    for (name, (median, max)) in &reads {
        println!("read:     {name:<28} median {median:>10.2?}  max {max:>10.2?}");
    }

    for (name, plan) in db.explain_hot_queries().unwrap() {
        println!("plan:     {name}: {plan}");
        assert!(!plan.contains("USE TEMP B-TREE"), "{name}: {plan}");
        assert!(!plan.contains("SCAN o"), "{name}: {plan}");
    }
    drop(db);
    println!(
        "size:     {:.1} MiB database, {:.1} MiB WAL",
        file_bytes(&path) as f64 / (1024.0 * 1024.0),
        file_bytes(&dir.path().join("errors.sqlite-wal")) as f64 / (1024.0 * 1024.0)
    );

    assert!(
        rate >= MIN_GROUPING_ROWS_PER_SEC,
        "grouping {rate:.0} rows/s is below the {MIN_GROUPING_ROWS_PER_SEC} rows/s budget"
    );
    assert!(
        slowest_page <= MAX_GROUPING_PAGE,
        "slowest grouping page {slowest_page:?} exceeds {MAX_GROUPING_PAGE:?}"
    );
    for (name, (median, _)) in &reads {
        assert!(
            *median <= MAX_READ_QUERY,
            "{name} median {median:?} exceeds {MAX_READ_QUERY:?}"
        );
    }
}
