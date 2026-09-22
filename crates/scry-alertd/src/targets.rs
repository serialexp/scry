use std::{
    collections::HashSet,
    net::{IpAddr, SocketAddr},
    str::FromStr,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode},
    routing::{get as route_get, post},
    Json, Router,
};
use futures::TryStreamExt;
use hmac::{Hmac, Mac};
use object_store::{path::Path as ObjectPath, ObjectStore, ObjectStoreExt, PutPayload};
use scry_alert::{
    canonical_json, sha256_hex, AlertStore, AlertsDb, BuiltInTargetFormat, CompiledJsonTemplate,
    LogicalSecretId, NotificationTarget, NotificationTargetId, NotificationTargetKind,
    NotificationTargetProjection, SecretBinding, SecretGenerationRecord, SecretHead, SecretKey,
    SecretKeyring, TargetFormat, TargetHeader, TargetMutationKind, TargetMutationReceipt,
    TargetTombstone, TemplateValues, ALERT_RECORD_SCHEMA_VERSION, MAX_ENDPOINT_BYTES, MAX_HEADERS,
    MAX_HEADER_BYTES, MAX_SECRET_BYTES, MAX_TARGET_NAME_BYTES, MAX_TIMEOUT_MILLIS,
};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use url::Url;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::{admit_control, authenticate, require_idempotency, ApiError, AppState, PageQuery};

pub const CURRENT_KEY_ENV: &str = "SCRY_ALERTD_TARGET_KEY_CURRENT";
pub const PREVIOUS_KEY_ENV: &str = "SCRY_ALERTD_TARGET_KEY_PREVIOUS";
const MAX_TARGETS: usize = 10_000;
const MAX_SECRET_LINEAGE: usize = 10_000;
const SIGNATURE_HEADER: &str = "x-scry-signature";
const SIGNATURE_TIMESTAMP_HEADER: &str = "x-scry-signature-timestamp";

type HmacSha256 = Hmac<Sha256>;

pub fn load_keyring() -> anyhow::Result<SecretKeyring> {
    let current = std::env::var(CURRENT_KEY_ENV)
        .map_err(|_| anyhow::anyhow!("{CURRENT_KEY_ENV} must be set"))?;
    let previous = std::env::var(PREVIOUS_KEY_ENV).ok();
    parse_keyring(&current, previous.as_deref())
}

fn parse_key(value: &str) -> anyhow::Result<(String, SecretKey)> {
    let (id, encoded) = value
        .split_once(':')
        .ok_or_else(|| anyhow::anyhow!("target key must be <id>:<base64url32>"))?;
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        anyhow::bail!("target key id is invalid");
    }
    Ok((id.to_owned(), SecretKey::parse_base64url(encoded)?))
}

pub fn parse_keyring(current: &str, previous: Option<&str>) -> anyhow::Result<SecretKeyring> {
    let (current_id, current_key) = parse_key(current)?;
    let previous = previous.map(parse_key).transpose()?;
    if previous.as_ref().is_some_and(|(id, _)| id == &current_id) {
        anyhow::bail!("current and previous target key IDs must differ");
    }
    Ok(SecretKeyring::new(current_id, current_key, previous))
}

pub async fn rebuild_projection(store: &dyn ObjectStore, db: &mut AlertsDb) -> anyhow::Result<()> {
    let prefix = ObjectPath::from("_scry/alerts/v1/targets");
    let mut listed = store.list(Some(&prefix));
    let mut ids = Vec::new();
    while let Some(meta) = listed.try_next().await? {
        let key = meta.location.as_ref();
        if let Some(id) = key
            .strip_prefix("_scry/alerts/v1/targets/")
            .and_then(|v| v.strip_suffix("/head.json"))
            .and_then(|v| Uuid::parse_str(v).ok())
        {
            ids.push(NotificationTargetId(id));
        }
    }
    if ids.len() > MAX_TARGETS {
        tracing::warn!(
            target_count = ids.len(),
            limit = MAX_TARGETS,
            "notification-target count exceeds the create limit; rebuilding all targets and refusing further creates"
        );
    }
    ids.sort_unstable_by_key(|id| id.0);
    let alert_store = AlertStore::new(store);
    for id in ids {
        let head = alert_store.read_target_head(id).await?;
        if head.value.target_id != id || head.value.schema_version != ALERT_RECORD_SCHEMA_VERSION {
            anyhow::bail!("notification-target head ownership mismatch");
        }
        if head.value.deleted {
            db.delete_notification_target(id)?;
        } else {
            let target = alert_store.read_target(id).await?.value;
            if target.id != id || target.revision != head.value.revision {
                anyhow::bail!("notification-target revision does not match head");
            }
            target.validate()?;
            db.fold_notification_target(&target)?;
        }
    }
    Ok(())
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/notification-targets", route_get(list).post(create))
        .route("/v1/notification-targets/validate", post(validate))
        .route(
            "/v1/notification-targets/{id}",
            route_get(get).put(update).delete(delete),
        )
        .route("/v1/notification-targets/{id}/preview", post(preview))
        .route("/v1/notification-targets/{id}/test", post(test_send))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TargetWrite {
    schema_version: u32,
    name: String,
    enabled: bool,
    kind: KindDto,
    timeout_ms: u32,
    headers: Vec<TargetHeader>,
    format: FormatDto,
    secret: SecretAction,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum KindDto {
    Webhook { url: String },
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum FormatDto {
    Builtin { format_id: FormatId },
    CustomJson { template: String },
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum FormatId {
    GenericJson,
    SlackCompatible,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum SecretAction {
    Set { value: Zeroizing<String> },
    Unchanged,
    Clear,
}

#[derive(Serialize)]
struct SecretFreeRequest<'a> {
    schema_version: u32,
    name: &'a str,
    enabled: bool,
    kind: &'a KindDto,
    timeout_ms: u32,
    headers: &'a [TargetHeader],
    format: &'a FormatDto,
    secret_action: &'static str,
}

fn secret_free_request(write: &TargetWrite) -> SecretFreeRequest<'_> {
    SecretFreeRequest {
        schema_version: write.schema_version,
        name: &write.name,
        enabled: write.enabled,
        kind: &write.kind,
        timeout_ms: write.timeout_ms,
        headers: &write.headers,
        format: &write.format,
        secret_action: match &write.secret {
            SecretAction::Set { .. } => "set",
            SecretAction::Unchanged => "unchanged",
            SecretAction::Clear => "clear",
        },
    }
}
#[derive(Serialize)]
struct TargetView {
    schema_version: u32,
    id: String,
    revision: String,
    name: String,
    enabled: bool,
    kind: KindDto,
    timeout_ms: u32,
    headers: Vec<TargetHeader>,
    format: FormatDto,
    secret_configured: bool,
    created_at_unix_nano: String,
    updated_at_unix_nano: String,
}
#[derive(Serialize)]
struct TargetList {
    targets: Vec<TargetView>,
    next: Option<String>,
}
#[derive(Serialize)]
struct Preview {
    content_type: &'static str,
    body: String,
    body_sha256: String,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct TestIntent {
    schema_version: u32,
    event_id: String,
    target_id: String,
    target_revision: u64,
    body_sha256: String,
    created_at_unix_nano: u64,
}

#[derive(Deserialize, Serialize)]
struct TestResult {
    event_id: String,
    outcome: String,
    http_status: Option<u16>,
    error_class: Option<String>,
    duration_ms: u64,
}

impl TargetView {
    fn from_projection(target: &NotificationTargetProjection) -> Self {
        let (url, headers) = match &target.kind {
            NotificationTargetKind::GenericWebhook { url, headers } => {
                (url.clone(), headers.clone())
            }
            NotificationTargetKind::SlackWebhook => (String::new(), Vec::new()),
        };
        let format = format_view(&target.format);
        Self {
            schema_version: 1,
            id: target.id.to_string(),
            revision: target.revision.to_string(),
            name: target.name.clone(),
            enabled: target.enabled,
            kind: KindDto::Webhook { url },
            timeout_ms: target.timeout_millis,
            headers,
            format,
            secret_configured: target.has_secret,
            created_at_unix_nano: target.created_at_unix_nano.to_string(),
            updated_at_unix_nano: target.updated_at_unix_nano.to_string(),
        }
    }

    fn from_target(target: &NotificationTarget) -> Self {
        let (url, headers) = match &target.kind {
            NotificationTargetKind::GenericWebhook { url, headers } => {
                (url.clone(), headers.clone())
            }
            NotificationTargetKind::SlackWebhook => (String::new(), Vec::new()),
        };
        let format = format_view(&target.format);
        Self {
            schema_version: 1,
            id: target.id.to_string(),
            revision: target.revision.to_string(),
            name: target.name.clone(),
            enabled: target.enabled,
            kind: KindDto::Webhook { url },
            timeout_ms: target.timeout_millis,
            headers,
            format,
            secret_configured: target.secret_generation > 0,
            created_at_unix_nano: target.created_at_unix_nano.to_string(),
            updated_at_unix_nano: target.updated_at_unix_nano.to_string(),
        }
    }
}

fn format_view(format: &TargetFormat) -> FormatDto {
    match format {
        TargetFormat::BuiltIn { format } => FormatDto::Builtin {
            format_id: match format {
                BuiltInTargetFormat::GenericJson => FormatId::GenericJson,
                BuiltInTargetFormat::Slack => FormatId::SlackCompatible,
            },
        },
        TargetFormat::CustomJson { template } => FormatDto::CustomJson {
            template: serde_json::to_string(template.ast()).expect("JSON value serializes"),
        },
    }
}

fn target_id(value: &str) -> Result<NotificationTargetId, ApiError> {
    Uuid::parse_str(value)
        .map(NotificationTargetId)
        .map_err(|_| ApiError::BadRequest("invalid notification target ID".into()))
}
fn expected_revision(headers: &HeaderMap) -> Result<u64, ApiError> {
    headers
        .get(header::IF_MATCH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim_matches('"').parse().ok())
        .ok_or_else(|| ApiError::BadRequest("If-Match revision is required".into()))
}
fn now() -> u64 {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_default()
        .max(0) as u64
}

fn command_uuid(command: &str, domain: &[u8]) -> Uuid {
    use sha2::Digest;
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(command.as_bytes());
    let hash = digest.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hash[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn validate_write(
    write: &TargetWrite,
    creating: bool,
) -> Result<(NotificationTargetKind, TargetFormat), ApiError> {
    if write.schema_version != 1 {
        return Err(ApiError::BadRequest(
            "unsupported notification target schema version".into(),
        ));
    }
    if write.name.is_empty() || write.name.len() > MAX_TARGET_NAME_BYTES {
        return Err(ApiError::BadRequest(
            "notification target name is invalid".into(),
        ));
    }
    if write.timeout_ms == 0 || write.timeout_ms > MAX_TIMEOUT_MILLIS {
        return Err(ApiError::BadRequest(
            "notification target timeout is invalid".into(),
        ));
    }
    if creating && !matches!(write.secret, SecretAction::Set { .. }) {
        return Err(ApiError::BadRequest(
            "new notification target requires a signing secret".into(),
        ));
    }
    if write.enabled && matches!(write.secret, SecretAction::Clear) {
        return Err(ApiError::BadRequest(
            "enabled notification target requires a signing secret".into(),
        ));
    }
    let KindDto::Webhook { url } = &write.kind;
    validate_url(url)?;
    validate_headers(&write.headers)?;
    if let SecretAction::Set { value } = &write.secret {
        if value.is_empty() || value.len() > MAX_SECRET_BYTES {
            return Err(ApiError::BadRequest(format!(
                "signing secret must contain between 1 and {MAX_SECRET_BYTES} bytes"
            )));
        }
    }
    let format = match &write.format {
        FormatDto::Builtin { format_id } => TargetFormat::BuiltIn {
            format: match format_id {
                FormatId::GenericJson => BuiltInTargetFormat::GenericJson,
                FormatId::SlackCompatible => BuiltInTargetFormat::Slack,
            },
        },
        FormatDto::CustomJson { template } => TargetFormat::CustomJson {
            template: CompiledJsonTemplate::compile(template)
                .map_err(|e| ApiError::BadRequest(e.to_string()))?,
        },
    };
    Ok((
        NotificationTargetKind::GenericWebhook {
            url: url.clone(),
            headers: write.headers.clone(),
        },
        format,
    ))
}

fn validate_url(value: &str) -> Result<(), ApiError> {
    if value.len() > MAX_ENDPOINT_BYTES {
        return Err(ApiError::BadRequest("webhook URL is too long".into()));
    }
    let url =
        Url::parse(value).map_err(|_| ApiError::BadRequest("webhook URL is invalid".into()))?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || url.username() != ""
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(ApiError::BadRequest(
            "webhook URL must be absolute HTTPS without userinfo or fragment".into(),
        ));
    }
    if let Some(host) = url.host_str() {
        if let Ok(ip) = IpAddr::from_str(host.trim_matches(['[', ']'])) {
            require_public(ip)?;
        }
    }
    Ok(())
}

fn validate_headers(headers: &[TargetHeader]) -> Result<(), ApiError> {
    if headers.len() > MAX_HEADERS
        || headers
            .iter()
            .any(|header| header.name.len() + header.value.len() > MAX_HEADER_BYTES)
    {
        return Err(ApiError::BadRequest("webhook headers exceed bounds".into()));
    }
    const BLOCKED: &[&str] = &[
        "host",
        "content-length",
        "connection",
        "transfer-encoding",
        "upgrade",
        "proxy-authorization",
        "proxy-authenticate",
        "te",
        "trailer",
        "cookie",
        "idempotency-key",
        SIGNATURE_HEADER,
        SIGNATURE_TIMESTAMP_HEADER,
    ];
    for item in headers {
        let name = HeaderName::from_bytes(item.name.as_bytes())
            .map_err(|_| ApiError::BadRequest("invalid header name".into()))?;
        HeaderValue::from_str(&item.value)
            .map_err(|_| ApiError::BadRequest("invalid header value".into()))?;
        if BLOCKED
            .iter()
            .any(|blocked| name.as_str().eq_ignore_ascii_case(blocked))
        {
            return Err(ApiError::BadRequest("reserved webhook header".into()));
        }
    }
    Ok(())
}

async fn list(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(page): Query<PageQuery>,
) -> Result<Json<TargetList>, ApiError> {
    authenticate(&state, &headers)?;
    let _permit = admit_control(&state)?;
    let after = page.after.as_deref().map(target_id).transpose()?;
    let limit = page.limit.unwrap_or(100).clamp(1, 500);
    let rows = state
        .db
        .lock()
        .await
        .list_notification_targets(after, limit)?;
    let next = (rows.len() == limit)
        .then(|| rows.last().map(|v| v.id.to_string()))
        .flatten();
    let targets = rows.iter().map(TargetView::from_projection).collect();
    Ok(Json(TargetList { targets, next }))
}
async fn get(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<TargetView>, ApiError> {
    authenticate(&state, &headers)?;
    let _permit = admit_control(&state)?;
    let id = target_id(&id)?;
    let target = AlertStore::new(state.store.as_ref())
        .read_target(id)
        .await
        .map_err(map_missing)?
        .value;
    Ok(Json(TargetView::from_target(&target)))
}
async fn validate(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(write): Json<TargetWrite>,
) -> Result<StatusCode, ApiError> {
    authenticate(&state, &headers)?;
    let _permit = admit_control(&state)?;
    validate_write(&write, false)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn create(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(write): Json<TargetWrite>,
) -> Result<(StatusCode, Json<TargetView>), ApiError> {
    authenticate(&state, &headers)?;
    let _permit = admit_control(&state)?;
    let command = require_idempotency(&headers)?;
    let view = persist(&state, None, 0, command, write, TargetMutationKind::Create).await?;
    Ok((StatusCode::CREATED, Json(view)))
}
async fn update(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(write): Json<TargetWrite>,
) -> Result<Json<TargetView>, ApiError> {
    authenticate(&state, &headers)?;
    let _permit = admit_control(&state)?;
    let command = require_idempotency(&headers)?;
    let id = target_id(&id)?;
    let expected = expected_revision(&headers)?;
    persist(
        &state,
        Some(id),
        expected,
        command,
        write,
        TargetMutationKind::Update,
    )
    .await
    .map(Json)
}

async fn persist(
    state: &AppState,
    requested_id: Option<NotificationTargetId>,
    expected: u64,
    command: &str,
    write: TargetWrite,
    kind: TargetMutationKind,
) -> Result<TargetView, ApiError> {
    let _mutation = state
        .target_mutations
        .try_acquire()
        .map_err(|_| ApiError::Overloaded)?;
    let (target_kind, format) = validate_write(&write, requested_id.is_none())?;
    let request_sha256 = sha256_hex(
        &canonical_json(&secret_free_request(&write))
            .map_err(|e| ApiError::BadRequest(e.to_string()))?,
    );
    let store = AlertStore::new(state.store.as_ref());
    let staged = store
        .read_target_mutation(command)
        .await
        .map_err(ApiError::Store)?;
    let (target, expected_head) = if let Some(receipt) = staged {
        if receipt.request_sha256 != request_sha256
            || receipt.kind != kind
            || requested_id.is_some_and(|id| id != receipt.target_id)
        {
            return Err(ApiError::Conflict);
        }
        let candidate = receipt.candidate;
        let expected_head = match store.read_target_head(candidate.id).await {
            Ok(head) if !head.value.deleted && head.value.revision + 1 == candidate.revision => {
                Some(head.version)
            }
            Ok(head)
                if !head.value.deleted
                    && head.value.revision == candidate.revision
                    && head.value.revision_key
                        == scry_alert::target_revision_key(candidate.id, candidate.revision) =>
            {
                None
            }
            Err(scry_alert::AlertStoreError::Missing { .. }) if candidate.revision == 1 => None,
            _ => return Err(ApiError::Conflict),
        };
        (candidate, expected_head)
    } else {
        let id = requested_id
            .unwrap_or_else(|| NotificationTargetId(command_uuid(command, b"notification-target")));
        let (revision, created_at, old_logical, old_generation, head_version) =
            if requested_id.is_some() {
                let head = store.read_target_head(id).await.map_err(map_missing)?;
                if head.value.deleted || head.value.revision != expected {
                    return Err(ApiError::Conflict);
                }
                let current = store.read_target(id).await.map_err(map_missing)?.value;
                (
                    expected + 1,
                    current.created_at_unix_nano,
                    current.logical_secret_id,
                    current.secret_generation,
                    Some(head.version),
                )
            } else {
                if expected != 0 {
                    return Err(ApiError::Conflict);
                }
                if state.db.lock().await.notification_target_count()? >= MAX_TARGETS {
                    return Err(ApiError::BadRequest(format!(
                        "notification target limit of {MAX_TARGETS} reached"
                    )));
                }
                (1, now(), LogicalSecretId(Uuid::nil()), 0, None)
            };
        let (logical, generation) = match &write.secret {
            SecretAction::Set { .. } => (
                LogicalSecretId(command_uuid(command, b"notification-target-secret")),
                1,
            ),
            SecretAction::Unchanged => (old_logical, old_generation),
            SecretAction::Clear => (LogicalSecretId(Uuid::nil()), 0),
        };
        if write.enabled && generation == 0 {
            return Err(ApiError::BadRequest(
                "enabled notification target requires a signing secret".into(),
            ));
        }
        let candidate = NotificationTarget {
            schema_version: ALERT_RECORD_SCHEMA_VERSION,
            id,
            revision,
            name: write.name.clone(),
            enabled: write.enabled,
            kind: target_kind,
            format,
            timeout_millis: write.timeout_ms,
            logical_secret_id: logical,
            secret_generation: generation,
            created_at_unix_nano: created_at,
            updated_at_unix_nano: now(),
        };
        candidate
            .validate()
            .map_err(|e| ApiError::BadRequest(e.to_string()))?;
        (candidate, head_version)
    };
    if let SecretAction::Set { value } = &write.secret {
        commit_secret(
            state,
            target.id,
            target.logical_secret_id,
            target.secret_generation,
            value.as_bytes(),
        )
        .await?;
    }
    store
        .record_target_mutation(&TargetMutationReceipt {
            schema_version: ALERT_RECORD_SCHEMA_VERSION,
            command_id: command.into(),
            kind,
            target_id: target.id,
            revision: target.revision,
            request_sha256,
            candidate: target.clone(),
        })
        .await
        .map_err(ApiError::Store)?;
    store
        .create_target_revision(&target, expected_head)
        .await
        .map_err(ApiError::Store)?;
    state.db.lock().await.fold_notification_target(&target)?;
    Ok(TargetView::from_target(&target))
}

async fn commit_secret(
    state: &AppState,
    id: NotificationTargetId,
    logical: LogicalSecretId,
    generation: u64,
    plaintext: &[u8],
) -> Result<(), ApiError> {
    let binding = SecretBinding {
        deployment_id: &state.deployment_id,
        target_id: id,
        logical_secret_id: logical,
        generation,
    };
    let alert_store = AlertStore::new(state.store.as_ref());
    if let Ok(existing) = alert_store
        .read_secret_generation(id, logical, generation)
        .await
    {
        let decrypted = state
            .keyring
            .decrypt(&binding, &existing.value.envelope)
            .map_err(|_| ApiError::Conflict)?;
        if decrypted.as_slice() != plaintext || existing.value.rotation_of_generation.is_some() {
            return Err(ApiError::Conflict);
        }
        let head = alert_store
            .read_secret_head(id, logical)
            .await
            .map_err(ApiError::Store)?;
        if let Some(head) = head {
            if head.value.generation < generation {
                return Err(ApiError::Conflict);
            }
            if head.value.generation == generation {
                return Ok(());
            }
            let latest = resolve_secret_lineage(
                &alert_store,
                &state.deployment_id,
                id,
                logical,
                generation,
                &head.value,
            )
            .await
            .map_err(ApiError::Store)?;
            state
                .keyring
                .decrypt(
                    &SecretBinding {
                        deployment_id: &state.deployment_id,
                        target_id: id,
                        logical_secret_id: logical,
                        generation: latest.generation,
                    },
                    &latest.envelope,
                )
                .map_err(|_| ApiError::Conflict)?;
            return Ok(());
        }
        let timestamp = now();
        return alert_store
            .commit_secret_generation(
                &existing.value,
                &SecretHead {
                    schema_version: ALERT_RECORD_SCHEMA_VERSION,
                    deployment_id: state.deployment_id.clone(),
                    target_id: id,
                    logical_secret_id: logical,
                    generation,
                    generation_key: scry_alert::secret_generation_key(id, logical, generation),
                    updated_at_unix_nano: timestamp,
                },
                None,
            )
            .await
            .map_err(ApiError::Store);
    }
    let envelope = state
        .keyring
        .encrypt(&binding, plaintext)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let timestamp = now();
    let record = SecretGenerationRecord {
        schema_version: ALERT_RECORD_SCHEMA_VERSION,
        deployment_id: state.deployment_id.clone(),
        target_id: id,
        logical_secret_id: logical,
        generation,
        envelope,
        rotation_of_generation: None,
        created_at_unix_nano: timestamp,
    };
    let head = SecretHead {
        schema_version: ALERT_RECORD_SCHEMA_VERSION,
        deployment_id: state.deployment_id.clone(),
        target_id: id,
        logical_secret_id: logical,
        generation,
        generation_key: scry_alert::secret_generation_key(id, logical, generation),
        updated_at_unix_nano: timestamp,
    };
    let alert_store = AlertStore::new(state.store.as_ref());
    let previous = alert_store
        .read_secret_head(id, logical)
        .await
        .map_err(ApiError::Store)?;
    alert_store
        .commit_secret_generation(&record, &head, previous.map(|v| v.version))
        .await
        .map_err(ApiError::Store)
}

async fn delete(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    authenticate(&state, &headers)?;
    let _permit = admit_control(&state)?;
    let command = require_idempotency(&headers)?;
    let id = target_id(&id)?;
    let expected = expected_revision(&headers)?;
    let store = AlertStore::new(state.store.as_ref());
    if let Some(old) = store
        .read_target_tombstone(id, command)
        .await
        .map_err(ApiError::Store)?
    {
        if old.revision != expected {
            return Err(ApiError::Conflict);
        }
        let head = store.read_target_head(id).await.map_err(map_missing)?;
        if head.value.deleted
            && head.value.tombstone_key.as_deref()
                == Some(scry_alert::target_tombstone_key(id, command).as_str())
        {
            state.db.lock().await.delete_notification_target(id)?;
            return Ok(StatusCode::NO_CONTENT);
        }
        if head.value.deleted || head.value.revision != expected {
            return Err(ApiError::Conflict);
        }
        store
            .tombstone_target(&old, head.version)
            .await
            .map_err(ApiError::Store)?;
        state.db.lock().await.delete_notification_target(id)?;
        return Ok(StatusCode::NO_CONTENT);
    }
    let head = store.read_target_head(id).await.map_err(map_missing)?;
    if head.value.deleted || head.value.revision != expected {
        return Err(ApiError::Conflict);
    }
    store
        .tombstone_target(
            &TargetTombstone {
                schema_version: ALERT_RECORD_SCHEMA_VERSION,
                target_id: id,
                revision: expected,
                command_id: command.into(),
                deleted_at_unix_nano: now(),
            },
            head.version,
        )
        .await
        .map_err(ApiError::Store)?;
    state.db.lock().await.delete_notification_target(id)?;
    Ok(StatusCode::NO_CONTENT)
}

fn render(target: &NotificationTarget, event_id: &str) -> Result<Vec<u8>, ApiError> {
    match &target.format {
        TargetFormat::BuiltIn { format: BuiltInTargetFormat::GenericJson } => canonical_json(&serde_json::json!({"schema_version":1,"event_id":event_id,"transition":"test","monitor_name":"Notification target test","status":"firing","value":null,"scry_url":""})).map_err(|e| ApiError::BadRequest(e.to_string())),
        TargetFormat::BuiltIn { format: BuiltInTargetFormat::Slack } => canonical_json(&serde_json::json!({"text":"Scry test notification: firing","blocks":[{"type":"section","text":{"type":"mrkdwn","text":"*Scry test notification* — firing"}}],"event_id":event_id})).map_err(|e| ApiError::BadRequest(e.to_string())),
        TargetFormat::CustomJson { template } => template.render(TemplateValues { event_id, transition:"test", monitor_name:"Notification target test", status:"firing", value:"", scry_url:"" }).map_err(|e| ApiError::BadRequest(e.to_string())),
    }
}
async fn checked_target(
    state: &AppState,
    headers: &HeaderMap,
    id: &str,
) -> Result<NotificationTarget, ApiError> {
    let id = target_id(id)?;
    let expected = expected_revision(headers)?;
    let target = AlertStore::new(state.store.as_ref())
        .read_target(id)
        .await
        .map_err(map_missing)?
        .value;
    if target.revision != expected {
        return Err(ApiError::Conflict);
    }
    Ok(target)
}
async fn preview(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<Preview>, ApiError> {
    authenticate(&state, &headers)?;
    let _permit = admit_control(&state)?;
    let target = checked_target(&state, &headers, &id).await?;
    let body = render(&target, "preview")?;
    Ok(Json(Preview {
        content_type: "application/json",
        body: String::from_utf8(body.clone()).expect("renderer emits JSON UTF-8"),
        body_sha256: sha256_hex(&body),
    }))
}

async fn test_send(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<TestResult>, ApiError> {
    authenticate(&state, &headers)?;
    let _permit = admit_control(&state)?;
    let command = require_idempotency(&headers)?;
    let target = checked_target(&state, &headers, &id).await?;
    if target.secret_generation == 0 {
        return Err(ApiError::BadRequest(
            "notification target has no signing secret".into(),
        ));
    }
    let event_id = format!(
        "test-{}",
        command_uuid(command, b"notification-target-test")
    );
    if let Some(existing) = read_test_result(state.store.as_ref(), &event_id).await? {
        return Ok(Json(existing));
    }
    let body = render(&target, &event_id)?;
    let intent = stage_test_intent(
        state.store.as_ref(),
        TestIntent {
            schema_version: 1,
            event_id: event_id.clone(),
            target_id: target.id.to_string(),
            target_revision: target.revision,
            body_sha256: sha256_hex(&body),
            created_at_unix_nano: now(),
        },
    )
    .await?;
    if intent.target_id != target.id.to_string()
        || intent.target_revision != target.revision
        || intent.body_sha256 != sha256_hex(&body)
    {
        return Err(ApiError::Conflict);
    }
    let lease_key = format!("lease/alert/deliver/{event_id}");
    let lease_ttl = Duration::from_millis(u64::from(target.timeout_millis) + 30_000);
    let Some(lease) = state
        .delivery_leases
        .try_acquire(&lease_key, lease_ttl)
        .await
        .map_err(|error| ApiError::Unavailable(error.to_string()))?
    else {
        let wait_until =
            Instant::now() + Duration::from_millis(u64::from(target.timeout_millis) + 1_000);
        loop {
            if let Some(existing) = read_test_result(state.store.as_ref(), &event_id).await? {
                return Ok(Json(existing));
            }
            if Instant::now() >= wait_until {
                return Err(ApiError::Overloaded);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    };
    if let Some(existing) = read_test_result(state.store.as_ref(), &event_id).await? {
        lease.release().await;
        return Ok(Json(existing));
    }
    let _send = state
        .test_sends
        .try_acquire()
        .map_err(|_| ApiError::Overloaded)?;
    lease
        .fence()
        .check()
        .map_err(|error| ApiError::Unavailable(error.to_string()))?;
    let secret = read_secret(&state, &target).await?;
    let timestamp = chrono::Utc::now().timestamp().max(0) as u64;
    let signature = sign(&secret, timestamp, &event_id, &body);
    let started = Instant::now();
    let result = tokio::time::timeout(
        Duration::from_millis(target.timeout_millis.into()),
        state
            .transport
            .send(&target, &event_id, timestamp, &body, &signature),
    )
    .await
    .unwrap_or(Err("timeout"));
    let duration = started.elapsed().as_millis().min(u64::MAX as u128) as u64;
    let response = match result {
        Ok(status) if (200..300).contains(&status) => TestResult {
            event_id: event_id.clone(),
            outcome: "accepted".into(),
            http_status: Some(status),
            error_class: None,
            duration_ms: duration,
        },
        Ok(status) if matches!(status, 408 | 425 | 429 | 500..=599) => TestResult {
            event_id: event_id.clone(),
            outcome: "retryable_failure".into(),
            http_status: Some(status),
            error_class: Some("http_status".into()),
            duration_ms: duration,
        },
        Ok(status) => TestResult {
            event_id: event_id.clone(),
            outcome: "permanent_failure".into(),
            http_status: Some(status),
            error_class: Some("http_status".into()),
            duration_ms: duration,
        },
        Err(class) => TestResult {
            event_id: event_id.clone(),
            outcome: "retryable_failure".into(),
            http_status: None,
            error_class: Some(class.into()),
            duration_ms: duration,
        },
    };
    lease
        .fence()
        .check()
        .map_err(|error| ApiError::Unavailable(error.to_string()))?;
    persist_test_record(state.store.as_ref(), &event_id, "result", &response).await?;
    lease.release().await;
    Ok(Json(response))
}

async fn stage_test_intent(
    store: &dyn ObjectStore,
    intent: TestIntent,
) -> Result<TestIntent, ApiError> {
    let path = ObjectPath::from(format!(
        "_scry/alerts/v1/test-deliveries/{}/intent.json",
        intent.event_id
    ));
    let bytes = canonical_json(&intent).map_err(|e| ApiError::BadRequest(e.to_string()))?;
    match scry_objstore::put_create(store, &path, PutPayload::from(bytes)).await {
        Ok(_) => Ok(intent),
        Err(object_store::Error::AlreadyExists { .. }) => {
            let existing = store.get(&path).await.map_err(|source| {
                ApiError::Store(scry_alert::AlertStoreError::Object {
                    path: path.to_string(),
                    source,
                })
            })?;
            let bytes = existing.bytes().await.map_err(|source| {
                ApiError::Store(scry_alert::AlertStoreError::Object {
                    path: path.to_string(),
                    source,
                })
            })?;
            serde_json::from_slice(&bytes).map_err(|error| ApiError::BadRequest(error.to_string()))
        }
        Err(source) => Err(ApiError::Store(scry_alert::AlertStoreError::Object {
            path: path.to_string(),
            source,
        })),
    }
}

async fn persist_test_record(
    store: &dyn ObjectStore,
    event: &str,
    kind: &str,
    value: &impl Serialize,
) -> Result<(), ApiError> {
    let path = ObjectPath::from(format!(
        "_scry/alerts/v1/test-deliveries/{event}/{kind}.json"
    ));
    let bytes = canonical_json(value).map_err(|e| ApiError::BadRequest(e.to_string()))?;
    match scry_objstore::put_create(store, &path, PutPayload::from(bytes.clone())).await {
        Ok(_) => Ok(()),
        Err(object_store::Error::AlreadyExists { .. }) => {
            let result = store.get(&path).await.map_err(|source| {
                ApiError::Store(scry_alert::AlertStoreError::Object {
                    path: path.to_string(),
                    source,
                })
            })?;
            let existing = result.bytes().await.map_err(|source| {
                ApiError::Store(scry_alert::AlertStoreError::Object {
                    path: path.to_string(),
                    source,
                })
            })?;
            if existing.as_ref() == bytes.as_slice() {
                Ok(())
            } else {
                Err(ApiError::Conflict)
            }
        }
        Err(source) => Err(ApiError::Store(scry_alert::AlertStoreError::Object {
            path: path.to_string(),
            source,
        })),
    }
}
async fn read_test_result(
    store: &dyn ObjectStore,
    event: &str,
) -> Result<Option<TestResult>, ApiError> {
    let path = ObjectPath::from(format!(
        "_scry/alerts/v1/test-deliveries/{event}/result.json"
    ));
    let result = match store.get(&path).await {
        Ok(result) => result,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(source) => {
            return Err(ApiError::Store(scry_alert::AlertStoreError::Object {
                path: path.to_string(),
                source,
            }))
        }
    };
    let bytes = result.bytes().await.map_err(|source| {
        ApiError::Store(scry_alert::AlertStoreError::Object {
            path: path.to_string(),
            source,
        })
    })?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| ApiError::BadRequest(error.to_string()))
}
async fn resolve_secret_lineage(
    store: &AlertStore<'_>,
    deployment_id: &str,
    target_id: NotificationTargetId,
    logical_id: LogicalSecretId,
    committed_generation: u64,
    head: &SecretHead,
) -> Result<SecretGenerationRecord, scry_alert::AlertStoreError> {
    let corrupt = |message| scry_alert::AlertStoreError::Corrupt {
        path: scry_alert::secret_generation_key(target_id, logical_id, head.generation),
        message,
    };
    if head.deployment_id != deployment_id
        || head.target_id != target_id
        || head.logical_secret_id != logical_id
        || head.generation < committed_generation
        || head.generation_key
            != scry_alert::secret_generation_key(target_id, logical_id, head.generation)
    {
        return Err(corrupt("secret head ownership or generation mismatch"));
    }
    let mut generation = head.generation;
    let mut seen = HashSet::new();
    let mut latest = None;
    for _ in 0..MAX_SECRET_LINEAGE {
        if !seen.insert(generation) {
            return Err(corrupt("secret rotation lineage contains a cycle"));
        }
        let record = store
            .read_secret_generation(target_id, logical_id, generation)
            .await?
            .value;
        if record.deployment_id != deployment_id
            || record.target_id != target_id
            || record.logical_secret_id != logical_id
            || record.generation != generation
        {
            return Err(corrupt("secret generation ownership mismatch"));
        }
        if latest.is_none() {
            latest = Some(record.clone());
        }
        if generation == committed_generation {
            return latest.ok_or_else(|| corrupt("secret rotation lineage is empty"));
        }
        let Some(previous) = record.rotation_of_generation else {
            return Err(corrupt("secret rotation lineage has a gap"));
        };
        if previous >= generation || previous + 1 != generation {
            return Err(corrupt("secret rotation lineage is non-contiguous"));
        }
        generation = previous;
    }
    Err(corrupt("secret rotation lineage exceeds its bound"))
}

async fn read_secret(
    state: &AppState,
    target: &NotificationTarget,
) -> Result<Zeroizing<Vec<u8>>, ApiError> {
    let store = AlertStore::new(state.store.as_ref());
    let committed = store
        .read_secret_generation(
            target.id,
            target.logical_secret_id,
            target.secret_generation,
        )
        .await
        .map_err(ApiError::Store)?
        .value;
    let record = if let Some(head) = store
        .read_secret_head(target.id, target.logical_secret_id)
        .await
        .map_err(ApiError::Store)?
    {
        resolve_secret_lineage(
            &store,
            &state.deployment_id,
            target.id,
            target.logical_secret_id,
            target.secret_generation,
            &head.value,
        )
        .await
        .map_err(ApiError::Store)?
    } else {
        committed
    };
    let binding = SecretBinding {
        deployment_id: &state.deployment_id,
        target_id: target.id,
        logical_secret_id: target.logical_secret_id,
        generation: record.generation,
    };
    state
        .keyring
        .decrypt(&binding, &record.envelope)
        .map_err(|_| ApiError::BadRequest("notification target secret cannot be decrypted".into()))
}
fn sign(secret: &[u8], timestamp: u64, event_id: &str, body: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts arbitrary key lengths");
    let mut prefix = String::with_capacity(32 + event_id.len());
    write!(&mut prefix, "v1\n{timestamp}\n{event_id}\n").expect("String write");
    mac.update(prefix.as_bytes());
    mac.update(body);
    let mut out = String::with_capacity(71);
    out.push_str("v1=");
    for byte in mac.finalize().into_bytes() {
        use std::fmt::Write;
        write!(out, "{byte:02x}").expect("String write");
    }
    out
}

#[async_trait]
pub trait WebhookTransport: Send + Sync {
    async fn send(
        &self,
        target: &NotificationTarget,
        event_id: &str,
        timestamp: u64,
        body: &[u8],
        signature: &str,
    ) -> Result<u16, &'static str>;
}
pub struct SecureWebhookTransport;
#[async_trait]
impl WebhookTransport for SecureWebhookTransport {
    async fn send(
        &self,
        target: &NotificationTarget,
        event_id: &str,
        timestamp: u64,
        body: &[u8],
        signature: &str,
    ) -> Result<u16, &'static str> {
        let NotificationTargetKind::GenericWebhook { url, headers } = &target.kind else {
            return Err("unsupported_target");
        };
        let parsed = Url::parse(url).map_err(|_| "invalid_url")?;
        let host = parsed.host_str().ok_or("invalid_url")?.to_owned();
        let port = parsed.port_or_known_default().ok_or("invalid_url")?;
        let addresses = tokio::net::lookup_host((host.as_str(), port))
            .await
            .map_err(|_| "dns")?
            .collect::<Vec<_>>();
        if addresses.is_empty() {
            return Err("dns");
        };
        for address in &addresses {
            require_public(address.ip()).map_err(|_| "network_policy")?;
        }
        let pinned = SocketAddr::new(addresses[0].ip(), port);
        let client = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .pool_max_idle_per_host(2)
            .resolve(&host, pinned)
            .build()
            .map_err(|_| "client")?;
        let mut request = client
            .post(url)
            .timeout(Duration::from_millis(target.timeout_millis.into()))
            .header(header::CONTENT_TYPE, "application/json")
            .header("idempotency-key", event_id)
            .header(SIGNATURE_TIMESTAMP_HEADER, timestamp)
            .header(SIGNATURE_HEADER, signature);
        for item in headers {
            request = request.header(&item.name, &item.value);
        }
        request
            .body(body.to_vec())
            .send()
            .await
            .map(|r| r.status().as_u16())
            .map_err(|e| {
                if e.is_timeout() {
                    "timeout"
                } else if e.is_connect() {
                    "connect"
                } else {
                    "transport"
                }
            })
    }
}

fn require_public(ip: IpAddr) -> Result<(), ApiError> {
    let public = match ip {
        IpAddr::V4(v) => {
            let o = v.octets();
            !(v.is_private()
                || v.is_loopback()
                || v.is_link_local()
                || v.is_multicast()
                || v.is_broadcast()
                || v.is_documentation()
                || v.is_unspecified()
                || o[0] == 0
                || o[0] >= 224
                || o[0] == 100 && (64..=127).contains(&o[1])
                || o[0] == 192 && o[1] == 0 && o[2] == 0
                || o[0] == 198 && (o[1] == 18 || o[1] == 19))
        }
        IpAddr::V6(v) => {
            let value = u128::from_be_bytes(v.octets());
            let in_prefix = |network: u128, bits: u32| {
                let mask = u128::MAX.checked_shl(128 - bits).unwrap_or(0);
                value & mask == network & mask
            };
            // Use an explicit stable allow/exclude policy rather than the unstable
            // `Ipv6Addr::is_global`. Start with global unicast and reject IANA
            // special-purpose ranges which can otherwise expose local infrastructure.
            in_prefix(0x2000_0000_0000_0000_0000_0000_0000_0000, 3)
                && !in_prefix(0x2001_0000_0000_0000_0000_0000_0000_0000, 23)
                && !in_prefix(0x2001_0db8_0000_0000_0000_0000_0000_0000, 32)
                && !in_prefix(0x2002_0000_0000_0000_0000_0000_0000_0000, 16)
                && !in_prefix(0x3fff_0000_0000_0000_0000_0000_0000_0000, 20)
                && !in_prefix(0x5f00_0000_0000_0000_0000_0000_0000_0000, 16)
                && !in_prefix(0x2620_004f_8000_0000_0000_0000_0000_0000, 48)
        }
    };
    if public {
        Ok(())
    } else {
        Err(ApiError::BadRequest(
            "webhook endpoint resolves to a non-public address".into(),
        ))
    }
}
fn map_missing(error: scry_alert::AlertStoreError) -> ApiError {
    match error {
        scry_alert::AlertStoreError::Missing { .. } => ApiError::NotFound,
        other => ApiError::Store(other),
    }
}

pub async fn rotate(
    store: &dyn ObjectStore,
    deployment_id: &str,
    keyring: &SecretKeyring,
    dry_run: bool,
) -> anyhow::Result<(usize, usize)> {
    let prefix = ObjectPath::from("_scry/alerts/v1/target-secrets");
    let mut listed = store.list(Some(&prefix));
    let mut heads = Vec::new();
    while let Some(meta) = listed.try_next().await? {
        let parts: Vec<_> = meta.location.as_ref().split('/').collect();
        if parts.len() == 7 && parts[6] == "head.json" {
            if let (Ok(target), Ok(logical)) =
                (Uuid::parse_str(parts[4]), Uuid::parse_str(parts[5]))
            {
                heads.push((NotificationTargetId(target), LogicalSecretId(logical)));
            }
        }
        if heads.len() > MAX_TARGETS {
            anyhow::bail!("target rotation exceeds {MAX_TARGETS} retained secret heads");
        }
    }
    let alert_store = AlertStore::new(store);
    let mut scanned = 0;
    let mut rotated = 0;
    for &(id, logical) in &heads {
        scanned += 1;
        let Some(old_head) = alert_store.read_secret_head(id, logical).await? else {
            anyhow::bail!("listed target secret head disappeared")
        };
        let old = alert_store
            .read_secret_generation(id, logical, old_head.value.generation)
            .await?
            .value;
        if old.envelope.key_id == keyring.current_key_id() {
            continue;
        }
        let old_binding = SecretBinding {
            deployment_id,
            target_id: id,
            logical_secret_id: logical,
            generation: old.generation,
        };
        let plaintext = keyring.decrypt(&old_binding, &old.envelope)?;
        rotated += 1;
        if dry_run {
            continue;
        }
        let generation = old.generation + 1;
        let binding = SecretBinding {
            deployment_id,
            target_id: id,
            logical_secret_id: logical,
            generation,
        };
        let record = match alert_store
            .read_secret_generation(id, logical, generation)
            .await
        {
            Ok(existing) => {
                let record = existing.value;
                if record.deployment_id != deployment_id
                    || record.target_id != id
                    || record.logical_secret_id != logical
                    || record.generation != generation
                    || record.rotation_of_generation != Some(old.generation)
                    || record.envelope.key_id != keyring.current_key_id()
                    || keyring.decrypt(&binding, &record.envelope)?.as_slice()
                        != plaintext.as_slice()
                {
                    anyhow::bail!("existing next secret generation is not the resumable rotation");
                }
                record
            }
            Err(scry_alert::AlertStoreError::Missing { .. }) => {
                let envelope = keyring.encrypt(&binding, &plaintext)?;
                SecretGenerationRecord {
                    schema_version: ALERT_RECORD_SCHEMA_VERSION,
                    deployment_id: deployment_id.into(),
                    target_id: id,
                    logical_secret_id: logical,
                    generation,
                    envelope,
                    rotation_of_generation: Some(old.generation),
                    created_at_unix_nano: now(),
                }
            }
            Err(error) => return Err(error.into()),
        };
        let head = SecretHead {
            schema_version: ALERT_RECORD_SCHEMA_VERSION,
            deployment_id: deployment_id.into(),
            target_id: id,
            logical_secret_id: logical,
            generation,
            generation_key: scry_alert::secret_generation_key(id, logical, generation),
            updated_at_unix_nano: record.created_at_unix_nano,
        };
        alert_store
            .commit_secret_generation(&record, &head, Some(old_head.version))
            .await?;
    }
    if !dry_run {
        for (id, logical) in &heads {
            let head = alert_store
                .read_secret_head(*id, *logical)
                .await?
                .ok_or_else(|| anyhow::anyhow!("retained secret head disappeared"))?;
            let record = alert_store
                .read_secret_generation(*id, *logical, head.value.generation)
                .await?
                .value;
            if record.envelope.key_id != keyring.current_key_id() {
                anyhow::bail!("previous-key target secret remains after rotation");
            }
        }
    }
    Ok((scanned, rotated))
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    };

    use scry_cluster::LocalLeaseProvider;

    use super::*;

    struct RecordingTransport {
        calls: AtomicUsize,
        timestamp: AtomicU64,
    }

    #[async_trait]
    impl WebhookTransport for RecordingTransport {
        async fn send(
            &self,
            _target: &NotificationTarget,
            _event_id: &str,
            timestamp: u64,
            _body: &[u8],
            _signature: &str,
        ) -> Result<u16, &'static str> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.timestamp.store(timestamp, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(204)
        }
    }

    async fn test_send_state() -> (AppState, NotificationTarget, Arc<RecordingTransport>) {
        use object_store::memory::InMemory;

        let deployment = Uuid::new_v4();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = AlertsDb::open_in_memory(deployment).unwrap();
        let keyring = Arc::new(
            parse_keyring("test:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", None).unwrap(),
        );
        let transport = Arc::new(RecordingTransport {
            calls: AtomicUsize::new(0),
            timestamp: AtomicU64::new(0),
        });
        let state = AppState {
            token: Arc::from("0123456789abcdef0123456789abcdef"),
            store,
            db: Arc::new(tokio::sync::Mutex::new(db)),
            permits: Arc::new(tokio::sync::Semaphore::new(16)),
            target_mutations: Arc::new(tokio::sync::Semaphore::new(1)),
            test_sends: Arc::new(tokio::sync::Semaphore::new(4)),
            query_targets: Arc::new(Vec::new()),
            deployment_id: deployment.to_string(),
            keyring,
            transport: transport.clone(),
            delivery_leases: Arc::new(crate::DeliveryLeases::new(LocalLeaseProvider::new())),
            _local_lock: Arc::new(None),
        };
        let target = NotificationTarget {
            schema_version: ALERT_RECORD_SCHEMA_VERSION,
            id: NotificationTargetId::new(),
            revision: 1,
            name: "test target".into(),
            enabled: true,
            kind: NotificationTargetKind::GenericWebhook {
                url: "https://example.com/hook".into(),
                headers: Vec::new(),
            },
            format: TargetFormat::BuiltIn {
                format: BuiltInTargetFormat::GenericJson,
            },
            timeout_millis: 1_000,
            logical_secret_id: LogicalSecretId::new(),
            secret_generation: 1,
            created_at_unix_nano: 1,
            updated_at_unix_nano: 1,
        };
        commit_secret(
            &state,
            target.id,
            target.logical_secret_id,
            target.secret_generation,
            b"secret",
        )
        .await
        .unwrap();
        AlertStore::new(state.store.as_ref())
            .create_target_revision(&target, None)
            .await
            .unwrap();
        (state, target, transport)
    }

    #[tokio::test]
    async fn concurrent_test_send_replays_one_durable_result_with_fresh_timestamp() {
        let (state, target, transport) = test_send_state().await;
        let command = Uuid::new_v4().to_string();
        let event_id = format!(
            "test-{}",
            command_uuid(&command, b"notification-target-test")
        );
        let body = render(&target, &event_id).unwrap();
        stage_test_intent(
            state.store.as_ref(),
            TestIntent {
                schema_version: 1,
                event_id: event_id.clone(),
                target_id: target.id.to_string(),
                target_revision: target.revision,
                body_sha256: sha256_hex(&body),
                created_at_unix_nano: 1,
            },
        )
        .await
        .unwrap();
        let headers = || {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::AUTHORIZATION,
                HeaderValue::from_static("Bearer 0123456789abcdef0123456789abcdef"),
            );
            headers.insert("idempotency-key", HeaderValue::from_str(&command).unwrap());
            headers.insert(header::IF_MATCH, HeaderValue::from_static("\"1\""));
            headers
        };
        let first = test_send(State(state.clone()), headers(), Path(target.id.to_string()));
        let second = test_send(State(state.clone()), headers(), Path(target.id.to_string()));
        let (first, second) = tokio::join!(first, second);
        assert_eq!(first.unwrap().0.event_id, event_id);
        assert_eq!(second.unwrap().0.event_id, event_id);
        assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
        assert!(
            transport.timestamp.load(Ordering::SeqCst)
                >= chrono::Utc::now().timestamp().max(0) as u64 - 1,
            "signature timestamp must be generated for the send, not copied from the old intent"
        );
    }

    #[test]
    fn keyring_and_network_validation() {
        let key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        assert!(parse_keyring(&format!("new:{key}"), Some(&format!("old:{key}"))).is_ok());
        assert!(parse_keyring(key, None).is_err());
        let cases = [
            ("127.0.0.1", false),
            ("10.0.0.1", false),
            ("169.254.169.254", false),
            ("8.8.8.8", true),
            ("::1", false),
            ("fc00::1", false),
            ("fe80::1", false),
            ("ff00::1", false),
            ("2001:db8::1", false),
            ("2001::1", false),
            ("2002::1", false),
            ("64:ff9b:1::1", false),
            ("100::1", false),
            ("2001:2::1", false),
            ("2001:10::1", false),
            ("2001:20::1", false),
            ("3fff::1", false),
            ("2606:4700:4700::1111", true),
        ];
        for (ip, expected) in cases {
            assert_eq!(
                require_public(ip.parse().unwrap()).is_ok(),
                expected,
                "{ip}"
            );
        }
    }
    #[test]
    fn hmac_covers_exact_body() {
        let a = sign(b"secret", 123, "event-1", b"{}");
        assert_eq!(
            a,
            "v1=53c2cf4b3944ab3cc280b569d50fe398261e9408465f0858b54d22f63975c02b"
        );
        assert_ne!(a, sign(b"secret", 124, "event-1", b"{}"));
        assert_ne!(a, sign(b"secret", 123, "event-2", b"{}"));
        assert_ne!(a, sign(b"secret", 123, "event-1", b"{}\n"));
    }

    #[tokio::test]
    async fn rotation_is_resumable_and_dry_run_does_not_write() {
        use object_store::memory::InMemory;

        let deployment = Uuid::new_v4().to_string();
        let target_id = NotificationTargetId::new();
        let logical_id = LogicalSecretId::new();
        let store = InMemory::new();
        let old = parse_keyring("old:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", None).unwrap();
        let keys = parse_keyring(
            "new:AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE",
            Some("old:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
        )
        .unwrap();
        let binding = SecretBinding {
            deployment_id: &deployment,
            target_id,
            logical_secret_id: logical_id,
            generation: 1,
        };
        let envelope = old.encrypt(&binding, b"secret").unwrap();
        let record = SecretGenerationRecord {
            schema_version: ALERT_RECORD_SCHEMA_VERSION,
            deployment_id: deployment.clone(),
            target_id,
            logical_secret_id: logical_id,
            generation: 1,
            envelope,
            rotation_of_generation: None,
            created_at_unix_nano: 1,
        };
        let head = SecretHead {
            schema_version: ALERT_RECORD_SCHEMA_VERSION,
            deployment_id: deployment.clone(),
            target_id,
            logical_secret_id: logical_id,
            generation: 1,
            generation_key: scry_alert::secret_generation_key(target_id, logical_id, 1),
            updated_at_unix_nano: 1,
        };
        let target = NotificationTarget {
            schema_version: ALERT_RECORD_SCHEMA_VERSION,
            id: target_id,
            revision: 1,
            name: "target".into(),
            enabled: true,
            kind: NotificationTargetKind::GenericWebhook {
                url: "https://example.com".into(),
                headers: vec![],
            },
            format: TargetFormat::BuiltIn {
                format: BuiltInTargetFormat::GenericJson,
            },
            timeout_millis: 1_000,
            logical_secret_id: logical_id,
            secret_generation: 1,
            created_at_unix_nano: 1,
            updated_at_unix_nano: 1,
        };
        let alerts = AlertStore::new(&store);
        alerts
            .commit_secret_generation(&record, &head, None)
            .await
            .unwrap();
        alerts.create_target_revision(&target, None).await.unwrap();

        assert_eq!(
            rotate(&store, &deployment, &keys, true).await.unwrap(),
            (1, 1)
        );
        assert_eq!(
            alerts
                .read_secret_head(target_id, logical_id)
                .await
                .unwrap()
                .unwrap()
                .value
                .generation,
            1
        );
        // Simulate a crash after the immutable next generation is written but
        // before its head CAS. Rotation must authenticate and reuse these exact
        // randomized ciphertext bytes rather than collide with a fresh nonce.
        let generation_two_binding = SecretBinding {
            deployment_id: &deployment,
            target_id,
            logical_secret_id: logical_id,
            generation: 2,
        };
        let staged_generation_two = SecretGenerationRecord {
            schema_version: ALERT_RECORD_SCHEMA_VERSION,
            deployment_id: deployment.clone(),
            target_id,
            logical_secret_id: logical_id,
            generation: 2,
            envelope: keys.encrypt(&generation_two_binding, b"secret").unwrap(),
            rotation_of_generation: Some(1),
            created_at_unix_nano: 2,
        };
        let staged_path =
            ObjectPath::from(scry_alert::secret_generation_key(target_id, logical_id, 2));
        scry_objstore::put_create(
            &store,
            &staged_path,
            PutPayload::from(canonical_json(&staged_generation_two).unwrap()),
        )
        .await
        .unwrap();

        assert_eq!(
            rotate(&store, &deployment, &keys, false).await.unwrap(),
            (1, 1)
        );
        let rotated = alerts
            .read_secret_head(target_id, logical_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rotated.value.generation, 2);
        let generation = alerts
            .read_secret_generation(target_id, logical_id, 2)
            .await
            .unwrap()
            .value;
        assert_eq!(generation.envelope.key_id, "new");
        assert_eq!(generation, staged_generation_two);
        assert_eq!(
            rotate(&store, &deployment, &keys, false).await.unwrap(),
            (1, 0)
        );

        // A later key rotation creates a second lineage hop. Revisions still
        // pinned to generation one must resolve the latest authenticated bytes.
        let newest = parse_keyring(
            "newest:AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI",
            Some("new:AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE"),
        )
        .unwrap();
        assert_eq!(
            rotate(&store, &deployment, &newest, false).await.unwrap(),
            (1, 1)
        );
        let latest_head = alerts
            .read_secret_head(target_id, logical_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest_head.value.generation, 3);
        let latest = resolve_secret_lineage(
            &alerts,
            &deployment,
            target_id,
            logical_id,
            1,
            &latest_head.value,
        )
        .await
        .unwrap();
        assert_eq!(latest.generation, 3);
        let plaintext = newest
            .decrypt(
                &SecretBinding {
                    deployment_id: &deployment,
                    target_id,
                    logical_secret_id: logical_id,
                    generation: 3,
                },
                &latest.envelope,
            )
            .unwrap();
        assert_eq!(plaintext.as_slice(), b"secret");
    }
}
