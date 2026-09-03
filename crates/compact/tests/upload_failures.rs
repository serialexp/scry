//! Deterministic object-store failure/cancellation coverage for pre-commit output cleanup.

use std::fmt::{Debug, Display};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult, UploadPart,
};
use scry_block::{BlockBuilder, BlockBuilderConfig, LogsBlockBuilder};
use scry_catalog::Catalog;
use scry_compact::{compact_once, CompactConfig, CompactResources, ResourceConfig};
use scry_proto::streaming::LogsAppender;
use tempfile::TempDir;
use tokio::sync::{Notify, Semaphore};
use uuid::Uuid;

const BUCKET: &str = "test";
const MIB: u64 = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    Put,
    MultipartComplete,
}

#[derive(Debug)]
enum Effect {
    Fail,
    Gate {
        reached: Arc<Notify>,
        gate: Arc<Semaphore>,
    },
}

#[derive(Debug)]
struct Script {
    suffix: &'static str,
    stage: Stage,
    effect: Effect,
    unused: AtomicBool,
}

impl Script {
    fn fail(suffix: &'static str, stage: Stage) -> Arc<Self> {
        Arc::new(Self {
            suffix,
            stage,
            effect: Effect::Fail,
            unused: AtomicBool::new(true),
        })
    }

    fn gate(suffix: &'static str, stage: Stage) -> (Arc<Self>, Arc<Notify>, Arc<Semaphore>) {
        let reached = Arc::new(Notify::new());
        let gate = Arc::new(Semaphore::new(0));
        (
            Arc::new(Self {
                suffix,
                stage,
                effect: Effect::Gate {
                    reached: reached.clone(),
                    gate: gate.clone(),
                },
                unused: AtomicBool::new(true),
            }),
            reached,
            gate,
        )
    }

    async fn apply(&self, path: &Path, stage: Stage) -> StoreResult<()> {
        if stage != self.stage
            || !path.as_ref().ends_with(self.suffix)
            || !self.unused.swap(false, Ordering::SeqCst)
        {
            return Ok(());
        }
        match &self.effect {
            Effect::Fail => Err(object_store::Error::Generic {
                store: "ScriptedStore",
                source: Box::new(std::io::Error::other(format!(
                    "injected {stage:?} failure for {path}"
                ))),
            }),
            Effect::Gate { reached, gate } => {
                reached.notify_one();
                let _permit = gate.acquire().await.expect("test gate closed");
                Ok(())
            }
        }
    }
}

struct ScriptedStore {
    inner: Arc<dyn ObjectStore>,
    script: Arc<Script>,
}

impl Debug for ScriptedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ScriptedStore")
    }
}
impl Display for ScriptedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ScriptedStore")
    }
}

#[derive(Debug)]
struct ScriptedUpload {
    inner: Box<dyn MultipartUpload>,
    path: Path,
    script: Arc<Script>,
}

#[async_trait]
impl MultipartUpload for ScriptedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.inner.put_part(data)
    }
    async fn complete(&mut self) -> StoreResult<PutResult> {
        self.script
            .apply(&self.path, Stage::MultipartComplete)
            .await?;
        self.inner.complete().await
    }
    async fn abort(&mut self) -> StoreResult<()> {
        self.inner.abort().await
    }
}

#[async_trait]
impl ObjectStore for ScriptedStore {
    async fn put_opts(&self, p: &Path, v: PutPayload, o: PutOptions) -> StoreResult<PutResult> {
        self.script.apply(p, Stage::Put).await?;
        self.inner.put_opts(p, v, o).await
    }
    async fn put_multipart_opts(
        &self,
        p: &Path,
        o: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        let inner = self.inner.put_multipart_opts(p, o).await?;
        Ok(Box::new(ScriptedUpload {
            inner,
            path: p.clone(),
            script: self.script.clone(),
        }))
    }
    async fn get_opts(&self, p: &Path, o: GetOptions) -> StoreResult<GetResult> {
        self.inner.get_opts(p, o).await
    }
    fn delete_stream(
        &self,
        p: futures::stream::BoxStream<'static, StoreResult<Path>>,
    ) -> futures::stream::BoxStream<'static, StoreResult<Path>> {
        self.inner.delete_stream(p)
    }
    fn list(
        &self,
        p: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list(p)
    }
    fn list_with_offset(
        &self,
        p: Option<&Path>,
        o: &Path,
    ) -> futures::stream::BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list_with_offset(p, o)
    }
    async fn list_with_delimiter(&self, p: Option<&Path>) -> StoreResult<ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(&self, f: &Path, t: &Path, o: CopyOptions) -> StoreResult<()> {
        self.inner.copy_opts(f, t, o).await
    }
    async fn rename_opts(
        &self,
        f: &Path,
        t: &Path,
        o: object_store::RenameOptions,
    ) -> StoreResult<()> {
        self.inner.rename_opts(f, t, o).await
    }
}

fn block_cfg() -> BlockBuilderConfig {
    BlockBuilderConfig {
        max_rows: 1_000_000,
        target_bytes: 128 * MIB,
        row_group_size: 100,
        ..Default::default()
    }
}
fn compact_cfg() -> CompactConfig {
    CompactConfig {
        fanout: 2,
        max_level: 1,
        grace: Duration::ZERO,
        signal_filter: Some("logs".into()),
        parallelism: 1,
    }
}
fn resources() -> Arc<CompactResources> {
    CompactResources::new(ResourceConfig {
        envelope_bytes: 128 * MIB,
        datafusion_memory_bytes: 64 * MIB,
        non_datafusion_memory_bytes: 32 * MIB,
        spill_bytes: 64 * MIB,
        spill_page_cache_headroom_bytes: 8 * MIB,
        spill_dir: None,
        allow_memory_backed_spill: true,
        output_buffer_bytes: 5 * MIB as usize,
        parquet_writer_memory_bytes: MIB as usize,
        max_waiters: 2,
        admission_timeout: Duration::from_secs(1),
    })
    .unwrap()
}

async fn fixture() -> (Arc<dyn ObjectStore>, Arc<Mutex<Catalog>>, TempDir) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Uuid::now_v7();
    let tmp = TempDir::new().unwrap();
    let catalog = Catalog::open(&tmp.path().join("catalog.sqlite"), BUCKET).unwrap();
    for i in 0..2u64 {
        let mut b = LogsBlockBuilder::new(writer, block_cfg());
        b.observe_stream(i + 1, vec![(b"service".to_vec(), b"api".to_vec())]);
        b.append_entry(
            i + 1,
            1_000_000 + i,
            9,
            format!("body {i}").into_bytes(),
            vec![],
        );
        let meta = b.finish_and_upload(store.as_ref()).await.unwrap().unwrap();
        assert!(catalog.insert_block(&meta).unwrap());
    }
    (store, Arc::new(Mutex::new(catalog)), tmp)
}

async fn paths(store: &Arc<dyn ObjectStore>) -> Vec<String> {
    let mut list = store.list(None);
    let mut out = Vec::new();
    while let Some(item) = list.next().await {
        out.push(item.unwrap().location.to_string());
    }
    out.sort();
    out
}
fn scripted(inner: Arc<dyn ObjectStore>, script: Arc<Script>) -> Arc<dyn ObjectStore> {
    Arc::new(ScriptedStore { inner, script })
}

#[tokio::test]
async fn bloom_put_failure_cleans_precommit_outputs_and_releases_permit_for_recovery() {
    let (inner, catalog, _tmp) = fixture().await;
    let before = paths(&inner).await;
    let resources = resources();
    let store = scripted(inner.clone(), Script::fail("body.bloom", Stage::Put));
    let report = compact_once(
        store,
        &catalog,
        BUCKET,
        &compact_cfg(),
        &block_cfg(),
        resources.clone(),
    )
    .await
    .unwrap();
    assert_eq!(report.partition_failed, 1);
    assert_eq!(
        paths(&inner).await,
        before,
        "all staged output must be removed"
    );
    assert_eq!(resources.telemetry().weighted_running_bytes, 0);

    let recovered = compact_once(
        inner,
        &catalog,
        BUCKET,
        &compact_cfg(),
        &block_cfg(),
        resources.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        recovered.merges, 1,
        "released permit must allow the next pass"
    );
    assert_eq!(resources.telemetry().weighted_running_bytes, 0);
}

#[tokio::test]
async fn cancelling_while_next_precommit_put_is_blocked_eventually_cleans_and_releases() {
    let (inner, catalog, _tmp) = fixture().await;
    let before = paths(&inner).await;
    let resources = resources();
    let (script, reached, _gate) = Script::gate("body.bloom", Stage::Put);
    let store = scripted(inner.clone(), script);
    let task = tokio::spawn({
        let catalog = catalog.clone();
        let resources = resources.clone();
        async move {
            compact_once(
                store,
                &catalog,
                BUCKET,
                &compact_cfg(),
                &block_cfg(),
                resources,
            )
            .await
        }
    });
    tokio::time::timeout(Duration::from_secs(10), reached.notified())
        .await
        .expect("merge reached bloom PUT after completing main/postings");
    task.abort();
    let _ = task.await;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if paths(&inner).await == before && resources.telemetry().weighted_running_bytes == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("drop-triggered async cleanup and permit release");
}

#[tokio::test]
async fn meta_put_failure_intentionally_retains_staged_data_due_to_ambiguous_commit() {
    let (inner, catalog, _tmp) = fixture().await;
    let before = paths(&inner).await;
    let resources = resources();
    let store = scripted(inner.clone(), Script::fail("meta.json", Stage::Put));
    let report = compact_once(
        store,
        &catalog,
        BUCKET,
        &compact_cfg(),
        &block_cfg(),
        resources.clone(),
    )
    .await
    .unwrap();
    assert_eq!(report.partition_failed, 1);
    let after = paths(&inner).await;
    assert!(
        after.len() > before.len(),
        "precommit data is retained once meta PUT is attempted"
    );
    assert_eq!(
        after.iter().filter(|p| p.ends_with("meta.json")).count(),
        2,
        "injected fail-before-persist leaves no new commit marker"
    );
    assert_eq!(catalog.lock().unwrap().list_blocks().unwrap().len(), 2);
    assert_eq!(resources.telemetry().weighted_running_bytes, 0);
}
