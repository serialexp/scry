//! Private query-worker control-plane primitives.
//!
//! This module is deliberately transport-neutral. It provides the authenticated
//! handshake and non-blocking reservation state machine used by the private
//! binschema listener; it does not accept public query requests or execute plans.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Context;
use hmac::{Hmac, Mac};
use rand::{rngs::OsRng, RngCore};
use scry_proto::framing::{read_frame_with_limit, write_frame};
use scry_proto::generated_query_worker::{
    QueryWorkerFrame, QueryWorkerFrameMsg, WorkerAuthenticatedInput, WorkerBidDeclineInput,
    WorkerBidResponseInput, WorkerBlockLocality, WorkerCancelAckInput, WorkerReleaseAckInput,
    WorkerServerHelloInput,
};
use scry_query::{BloomCache, BloomCacheResidency, PostingsCache};
use sha2::Sha256;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

pub const WORKER_PROTOCOL_VERSION: u16 = 1;
pub const AUTH_MAC_BYTES: usize = 32;
pub const NONCE_BYTES: usize = 32;
pub const RESERVATION_TOKEN_BYTES: usize = 32;
pub const MAX_HANDSHAKE_NAMESPACE_BYTES: usize = 256;
pub const MAX_RESERVATIONS: usize = 16_384;
pub const MAX_MEMORY_UNITS_PER_RESERVATION: u32 = 16_384;
pub const MAX_WORKER_CONTROL_FRAME_BYTES: usize = 1024 * 1024;
pub const MAX_BID_BLOCKS: usize = 4096;
pub const MAX_REQUIRED_COLUMNS: usize = 256;
pub const MAX_COLUMN_NAME_BYTES: usize = 255;

const AUTH_DOMAIN: &[u8] = b"scry-query-worker-auth-v1\0";
const SERVER_AUTH_DOMAIN: &[u8] = b"scry-query-worker-server-v1\0";
const RECORD_AUTH_DOMAIN: &[u8] = b"scry-query-worker-record-v1\0";
const CLIENT_TO_SERVER: u8 = 1;
const SERVER_TO_CLIENT: u8 = 2;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct AuthKey {
    pub id: String,
    secret: Arc<[u8]>,
}

impl AuthKey {
    pub fn new(id: impl Into<String>, secret: impl Into<Vec<u8>>) -> Result<Self, AuthError> {
        let id = id.into();
        let secret = secret.into();
        if id.is_empty() || id.len() > 64 {
            return Err(AuthError::InvalidConfiguration);
        }
        if secret.len() < 32 {
            return Err(AuthError::InvalidConfiguration);
        }
        Ok(Self {
            id,
            secret: secret.into(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientHello {
    pub protocol_version: u16,
    pub coordinator_id: [u8; 16],
    pub expected_worker_id: [u8; 16],
    pub deployment: String,
    pub timestamp_unix_ms: u64,
    pub nonce: [u8; NONCE_BYTES],
    pub key_id: String,
    pub mac: [u8; AUTH_MAC_BYTES],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerHello {
    pub protocol_version: u16,
    pub worker_id: [u8; 16],
    pub coordinator_nonce: [u8; NONCE_BYTES],
    pub worker_nonce: [u8; NONCE_BYTES],
    pub timestamp_unix_ms: u64,
    pub key_id: String,
    pub mac: [u8; AUTH_MAC_BYTES],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthError {
    InvalidConfiguration,
    InvalidMessage,
    UnsupportedVersion,
    WrongWorker,
    UnknownKey,
    Stale,
    BadMac,
    Replay,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlFrameError {
    UnexpectedBeforeAuthentication,
    UnexpectedAfterAuthentication,
    InvalidIdentityLength,
    InvalidDigestLength,
    TooManyBlocks,
    TooManyColumns,
    InvalidColumn,
    InvalidBoolean,
    InvalidPressure,
    InvalidMemoryUnits,
}

/// Validate allocation-sensitive and semantic constraints immediately after a
/// worker control frame is decoded. Binschema length prefixes alone permit up to
/// `u16::MAX` entries, which is intentionally larger than the worker protocol.
pub fn validate_control_frame(
    frame: &QueryWorkerFrame,
    authenticated: bool,
) -> Result<(), ControlFrameError> {
    use QueryWorkerFrameMsg::*;
    match (&frame.msg, authenticated) {
        (WorkerClientHello(hello), false) => {
            if hello.coordinator_id.len() != 16
                || hello.expected_worker_id.len() != 16
                || hello.nonce.len() != NONCE_BYTES
                || hello.mac.len() != AUTH_MAC_BYTES
            {
                return Err(ControlFrameError::InvalidIdentityLength);
            }
            if hello.deployment.is_empty()
                || hello.deployment.len() > MAX_HANDSHAKE_NAMESPACE_BYTES
                || hello.key_id.is_empty()
                || hello.key_id.len() > 64
            {
                return Err(ControlFrameError::InvalidColumn);
            }
            Ok(())
        }
        (WorkerClientHello(_), true) | (WorkerAuthenticated(_), true) => {
            Err(ControlFrameError::UnexpectedAfterAuthentication)
        }
        (_, false) => Err(ControlFrameError::UnexpectedBeforeAuthentication),
        (WorkerBidRequest(request), true) => {
            if request.coordinator_id.len() != 16 || request.offer_id.len() != 16 {
                return Err(ControlFrameError::InvalidIdentityLength);
            }
            if request.block_set_digest.len() != 32 {
                return Err(ControlFrameError::InvalidDigestLength);
            }
            if request.blocks.is_empty() || request.blocks.len() > MAX_BID_BLOCKS {
                return Err(ControlFrameError::TooManyBlocks);
            }
            if request.required_columns.len() > MAX_REQUIRED_COLUMNS {
                return Err(ControlFrameError::TooManyColumns);
            }
            if request
                .required_columns
                .iter()
                .any(|column| column.is_empty() || column.len() > MAX_COLUMN_NAME_BYTES)
            {
                return Err(ControlFrameError::InvalidColumn);
            }
            if request.requires_postings > 1 || request.requires_bloom > 1 {
                return Err(ControlFrameError::InvalidBoolean);
            }
            if !matches!(request.signal, 1..=4) || !matches!(request.operation, 1..=3) {
                return Err(ControlFrameError::InvalidColumn);
            }
            if request.memory_units == 0 || request.memory_units > MAX_MEMORY_UNITS_PER_RESERVATION
            {
                return Err(ControlFrameError::InvalidMemoryUnits);
            }
            if request.blocks.iter().any(|block| block.uuid.len() != 16) {
                return Err(ControlFrameError::InvalidIdentityLength);
            }
            Ok(())
        }
        (WorkerRelease(release), true) => {
            if release.coordinator_id.len() != 16
                || release.reservation_token.len() != RESERVATION_TOKEN_BYTES
            {
                return Err(ControlFrameError::InvalidIdentityLength);
            }
            Ok(())
        }
        (WorkerCancel(cancel), true) => {
            if cancel.coordinator_id.len() != 16 || cancel.fragment_id.len() != 16 {
                return Err(ControlFrameError::InvalidIdentityLength);
            }
            Ok(())
        }
        (WorkerBidResponse(response), true) => {
            if response.offer_id.len() != 16
                || response.worker_id.len() != 16
                || response.reservation_token.len() != RESERVATION_TOKEN_BYTES
                || response.locality.len() > MAX_BID_BLOCKS
                || response
                    .locality
                    .iter()
                    .any(|item| item.uuid.len() != 16 || item.locality > 5)
            {
                return Err(ControlFrameError::InvalidIdentityLength);
            }
            if response.memory_pressure_per_mille > 1000 {
                return Err(ControlFrameError::InvalidPressure);
            }
            Ok(())
        }
        (WorkerReleaseAck(ack), true) if ack.released <= 1 => Ok(()),
        (WorkerCancelAck(ack), true) if ack.cancelled <= 1 => Ok(()),
        (WorkerReleaseAck(_), true) | (WorkerCancelAck(_), true) => {
            Err(ControlFrameError::InvalidBoolean)
        }
        (WorkerServerHello(_), true) | (WorkerBidDecline(_), true) | (WorkerError(_), true) => {
            Ok(())
        }
    }
}

pub struct WorkerAuthenticator {
    worker_id: [u8; 16],
    deployment: String,
    current: AuthKey,
    previous: Option<AuthKey>,
    maximum_clock_skew_ms: u64,
    replay_capacity: usize,
    replay: Mutex<ReplayState>,
}

#[derive(Default)]
struct ReplayState {
    seen: HashMap<([u8; 16], [u8; NONCE_BYTES]), u64>,
    order: VecDeque<([u8; 16], [u8; NONCE_BYTES])>,
}

impl WorkerAuthenticator {
    pub fn new(
        worker_id: [u8; 16],
        deployment: impl Into<String>,
        current: AuthKey,
        previous: Option<AuthKey>,
        maximum_clock_skew_ms: u64,
        replay_capacity: usize,
    ) -> Result<Self, AuthError> {
        let deployment = deployment.into();
        if deployment.is_empty()
            || deployment.len() > MAX_HANDSHAKE_NAMESPACE_BYTES
            || maximum_clock_skew_ms == 0
            || replay_capacity == 0
            || previous.as_ref().is_some_and(|key| key.id == current.id)
        {
            return Err(AuthError::InvalidConfiguration);
        }
        Ok(Self {
            worker_id,
            deployment,
            current,
            previous,
            maximum_clock_skew_ms,
            replay_capacity,
            replay: Mutex::new(ReplayState::default()),
        })
    }

    /// Verify a client handshake and atomically consume its nonce.
    ///
    /// A nonce enters the replay set only after successful MAC verification, so
    /// unauthenticated traffic cannot evict valid replay entries.
    pub fn authenticate(
        &self,
        hello: &ClientHello,
        now_unix_ms: u64,
    ) -> Result<ServerHello, AuthError> {
        if hello.protocol_version != WORKER_PROTOCOL_VERSION {
            return Err(AuthError::UnsupportedVersion);
        }
        if hello.expected_worker_id != self.worker_id {
            return Err(AuthError::WrongWorker);
        }
        if hello.deployment != self.deployment
            || hello.deployment.len() > MAX_HANDSHAKE_NAMESPACE_BYTES
        {
            return Err(AuthError::InvalidMessage);
        }
        let age = now_unix_ms.abs_diff(hello.timestamp_unix_ms);
        if age > self.maximum_clock_skew_ms {
            return Err(AuthError::Stale);
        }
        let key = self.key(&hello.key_id).ok_or(AuthError::UnknownKey)?;
        verify_client_mac(key, hello)?;

        let replay_key = (hello.coordinator_id, hello.nonce);
        let mut replay = self.replay.lock().expect("worker replay mutex poisoned");
        replay.seen.retain(|_, expiry| *expiry >= now_unix_ms);
        let live = replay.seen.clone();
        replay.order.retain(|key| live.contains_key(key));
        if replay.seen.contains_key(&replay_key) {
            return Err(AuthError::Replay);
        }
        while replay.order.len() >= self.replay_capacity {
            if let Some(oldest) = replay.order.pop_front() {
                replay.seen.remove(&oldest);
            }
        }
        replay.order.push_back(replay_key);
        replay.seen.insert(
            replay_key,
            now_unix_ms.saturating_add(self.maximum_clock_skew_ms),
        );
        drop(replay);

        let mut worker_nonce = [0; NONCE_BYTES];
        OsRng.fill_bytes(&mut worker_nonce);
        let mut response = ServerHello {
            protocol_version: WORKER_PROTOCOL_VERSION,
            worker_id: self.worker_id,
            coordinator_nonce: hello.nonce,
            worker_nonce,
            timestamp_unix_ms: now_unix_ms,
            key_id: key.id.clone(),
            mac: [0; AUTH_MAC_BYTES],
        };
        response.mac = server_mac(key, hello, &response);
        Ok(response)
    }

    fn key(&self, id: &str) -> Option<&AuthKey> {
        if self.current.id == id {
            Some(&self.current)
        } else {
            self.previous.as_ref().filter(|key| key.id == id)
        }
    }

    fn key_owned(&self, id: &str) -> Option<AuthKey> {
        self.key(id).cloned()
    }
}

pub fn sign_client_hello(key: &AuthKey, hello: &mut ClientHello) {
    hello.key_id.clone_from(&key.id);
    hello.mac = client_mac(key, hello);
}

pub fn verify_server_hello(
    key: &AuthKey,
    client: &ClientHello,
    server: &ServerHello,
    now_unix_ms: u64,
    maximum_clock_skew_ms: u64,
) -> Result<(), AuthError> {
    if server.protocol_version != client.protocol_version
        || server.worker_id != client.expected_worker_id
        || server.coordinator_nonce != client.nonce
        || server.key_id != key.id
    {
        return Err(AuthError::InvalidMessage);
    }
    if now_unix_ms.abs_diff(server.timestamp_unix_ms) > maximum_clock_skew_ms {
        return Err(AuthError::Stale);
    }
    let mut mac = HmacSha256::new_from_slice(&key.secret).expect("HMAC accepts any key size");
    write_server_transcript(&mut mac, client, server);
    mac.verify_slice(&server.mac).map_err(|_| AuthError::BadMac)
}

fn verify_client_mac(key: &AuthKey, hello: &ClientHello) -> Result<(), AuthError> {
    let mut mac = HmacSha256::new_from_slice(&key.secret).expect("HMAC accepts any key size");
    write_client_transcript(&mut mac, hello);
    mac.verify_slice(&hello.mac).map_err(|_| AuthError::BadMac)
}

fn client_mac(key: &AuthKey, hello: &ClientHello) -> [u8; AUTH_MAC_BYTES] {
    let mut mac = HmacSha256::new_from_slice(&key.secret).expect("HMAC accepts any key size");
    write_client_transcript(&mut mac, hello);
    mac.finalize().into_bytes().into()
}

fn server_mac(key: &AuthKey, client: &ClientHello, server: &ServerHello) -> [u8; AUTH_MAC_BYTES] {
    let mut mac = HmacSha256::new_from_slice(&key.secret).expect("HMAC accepts any key size");
    write_server_transcript(&mut mac, client, server);
    mac.finalize().into_bytes().into()
}

fn write_server_transcript(mac: &mut HmacSha256, client: &ClientHello, server: &ServerHello) {
    mac.update(SERVER_AUTH_DOMAIN);
    write_client_fields(mac, client);
    mac.update(&server.protocol_version.to_be_bytes());
    mac.update(&server.worker_id);
    mac.update(&server.coordinator_nonce);
    mac.update(&server.worker_nonce);
    mac.update(&server.timestamp_unix_ms.to_be_bytes());
    write_len_bytes(mac, server.key_id.as_bytes());
}

fn write_client_transcript(mac: &mut HmacSha256, hello: &ClientHello) {
    mac.update(AUTH_DOMAIN);
    write_client_fields(mac, hello);
}

fn write_client_fields(mac: &mut HmacSha256, hello: &ClientHello) {
    mac.update(&hello.protocol_version.to_be_bytes());
    mac.update(&hello.coordinator_id);
    mac.update(&hello.expected_worker_id);
    write_len_bytes(mac, hello.deployment.as_bytes());
    mac.update(&hello.timestamp_unix_ms.to_be_bytes());
    mac.update(&hello.nonce);
    write_len_bytes(mac, hello.key_id.as_bytes());
}

fn write_len_bytes(mac: &mut HmacSha256, value: &[u8]) {
    mac.update(&(value.len() as u32).to_be_bytes());
    mac.update(value);
}

fn record_mac(
    key: &AuthKey,
    client: &ClientHello,
    server: &ServerHello,
    direction: u8,
    sequence: u64,
    payload: &[u8],
) -> [u8; AUTH_MAC_BYTES] {
    let mut mac = HmacSha256::new_from_slice(&key.secret).expect("HMAC accepts any key size");
    mac.update(RECORD_AUTH_DOMAIN);
    mac.update(&client.coordinator_id);
    mac.update(&server.worker_id);
    mac.update(&client.nonce);
    mac.update(&server.worker_nonce);
    mac.update(&[direction]);
    mac.update(&sequence.to_be_bytes());
    write_len_bytes(&mut mac, payload);
    mac.finalize().into_bytes().into()
}

fn seal_record(
    key: &AuthKey,
    client: &ClientHello,
    server: &ServerHello,
    direction: u8,
    sequence: u64,
    frame: QueryWorkerFrame,
) -> anyhow::Result<QueryWorkerFrame> {
    if matches!(frame.msg, QueryWorkerFrameMsg::WorkerAuthenticated(_)) {
        anyhow::bail!("nested authenticated worker record");
    }
    let payload = frame.encode()?;
    anyhow::ensure!(
        payload.len() <= MAX_WORKER_CONTROL_FRAME_BYTES,
        "worker record payload exceeds protocol limit"
    );
    let mac = record_mac(key, client, server, direction, sequence, &payload);
    Ok(QueryWorkerFrame {
        msg: QueryWorkerFrameMsg::WorkerAuthenticated(
            WorkerAuthenticatedInput {
                sequence,
                payload,
                mac: mac.to_vec(),
            }
            .into(),
        ),
    })
}

fn open_record(
    key: &AuthKey,
    client: &ClientHello,
    server: &ServerHello,
    direction: u8,
    expected_sequence: u64,
    envelope: scry_proto::WorkerAuthenticated,
) -> anyhow::Result<QueryWorkerFrame> {
    anyhow::ensure!(
        envelope.sequence == expected_sequence,
        "worker record sequence mismatch"
    );
    anyhow::ensure!(
        envelope.payload.len() <= MAX_WORKER_CONTROL_FRAME_BYTES
            && envelope.mac.len() == AUTH_MAC_BYTES,
        "invalid worker record lengths"
    );
    let mut verifier = HmacSha256::new_from_slice(&key.secret).expect("HMAC accepts any key size");
    verifier.update(RECORD_AUTH_DOMAIN);
    verifier.update(&client.coordinator_id);
    verifier.update(&server.worker_id);
    verifier.update(&client.nonce);
    verifier.update(&server.worker_nonce);
    verifier.update(&[direction]);
    verifier.update(&envelope.sequence.to_be_bytes());
    write_len_bytes(&mut verifier, &envelope.payload);
    verifier
        .verify_slice(&envelope.mac)
        .map_err(|_| anyhow::anyhow!("worker record MAC mismatch"))?;
    let decoded = QueryWorkerFrame::decode(&envelope.payload)?;
    anyhow::ensure!(
        decoded.encode()?.len() == envelope.payload.len(),
        "worker record contains trailing bytes"
    );
    anyhow::ensure!(
        !matches!(decoded.msg, QueryWorkerFrameMsg::WorkerAuthenticated(_)),
        "nested authenticated worker record"
    );
    Ok(decoded)
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct OfferKey {
    pub coordinator_id: [u8; 16],
    pub offer_id: [u8; 16],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReservationRequest {
    pub key: OfferKey,
    pub block_set_digest: [u8; 32],
    pub operation: u8,
    pub memory_units: u32,
    pub now_unix_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReservationGrant {
    pub token: [u8; RESERVATION_TOKEN_BYTES],
    pub expires_unix_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReservationDecline {
    Invalid,
    Busy,
    MemoryPressure,
    Capacity,
}

pub struct WorkerAdmission {
    slots: Arc<Semaphore>,
    memory: Arc<Semaphore>,
    ttl_ms: u64,
    maximum_reservations: usize,
    active: Arc<AtomicU64>,
    state: Mutex<ReservationState>,
}

#[derive(Default)]
struct ReservationState {
    by_token: HashMap<[u8; RESERVATION_TOKEN_BYTES], Reservation>,
    by_offer: HashMap<OfferKey, [u8; RESERVATION_TOKEN_BYTES]>,
}

struct Reservation {
    grant: ReservationGrant,
    request: ReservationRequest,
    _slot: OwnedSemaphorePermit,
    _memory: OwnedSemaphorePermit,
}

impl WorkerAdmission {
    pub fn new(
        fragment_slots: usize,
        memory_units: usize,
        ttl_ms: u64,
        maximum_reservations: usize,
    ) -> Result<Self, ReservationDecline> {
        if fragment_slots == 0
            || fragment_slots > Semaphore::MAX_PERMITS
            || memory_units == 0
            || memory_units > Semaphore::MAX_PERMITS
            || ttl_ms == 0
            || maximum_reservations == 0
            || maximum_reservations > MAX_RESERVATIONS
        {
            return Err(ReservationDecline::Invalid);
        }
        Ok(Self {
            slots: Arc::new(Semaphore::new(fragment_slots)),
            memory: Arc::new(Semaphore::new(memory_units)),
            ttl_ms,
            maximum_reservations,
            active: Arc::new(AtomicU64::new(0)),
            state: Mutex::new(ReservationState::default()),
        })
    }

    pub fn reserve(
        &self,
        request: ReservationRequest,
    ) -> Result<ReservationGrant, ReservationDecline> {
        if request.memory_units == 0 || request.memory_units > MAX_MEMORY_UNITS_PER_RESERVATION {
            return Err(ReservationDecline::Invalid);
        }
        self.reap_expired(request.now_unix_ms);
        {
            let state = self.state.lock().expect("reservation mutex poisoned");
            if let Some(token) = state.by_offer.get(&request.key) {
                let existing = state
                    .by_token
                    .get(token)
                    .expect("offer index is consistent");
                return if existing.request.block_set_digest == request.block_set_digest
                    && existing.request.operation == request.operation
                    && existing.request.memory_units == request.memory_units
                {
                    Ok(existing.grant.clone())
                } else {
                    Err(ReservationDecline::Invalid)
                };
            }
            if state.by_token.len() >= self.maximum_reservations {
                return Err(ReservationDecline::Capacity);
            }
        }

        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| ReservationDecline::Busy)?;
        let memory = self
            .memory
            .clone()
            .try_acquire_many_owned(request.memory_units)
            .map_err(|_| ReservationDecline::MemoryPressure)?;
        let mut token = [0; RESERVATION_TOKEN_BYTES];
        OsRng.fill_bytes(&mut token);
        let grant = ReservationGrant {
            token,
            expires_unix_ms: request.now_unix_ms.saturating_add(self.ttl_ms),
        };
        let reservation = Reservation {
            grant: grant.clone(),
            request: request.clone(),
            _slot: slot,
            _memory: memory,
        };
        let mut state = self.state.lock().expect("reservation mutex poisoned");
        // A concurrent duplicate can only happen through separate threads after
        // permits were acquired. Preserve idempotency and let these permits drop.
        if let Some(existing_token) = state.by_offer.get(&request.key) {
            let existing = state
                .by_token
                .get(existing_token)
                .expect("offer index is consistent");
            return if existing.request.block_set_digest == request.block_set_digest
                && existing.request.operation == request.operation
                && existing.request.memory_units == request.memory_units
            {
                Ok(existing.grant.clone())
            } else {
                Err(ReservationDecline::Invalid)
            };
        }
        // The optimistic pre-permit check can race. Enforce the hard map bound
        // again while holding the insertion lock.
        if state.by_token.len() >= self.maximum_reservations {
            return Err(ReservationDecline::Capacity);
        }
        state.by_offer.insert(request.key, token);
        state.by_token.insert(token, reservation);
        self.active.fetch_add(1, Ordering::Relaxed);
        Ok(grant)
    }

    pub fn release(&self, coordinator_id: [u8; 16], token: [u8; RESERVATION_TOKEN_BYTES]) -> bool {
        let mut state = self.state.lock().expect("reservation mutex poisoned");
        let is_owner = state
            .by_token
            .get(&token)
            .is_some_and(|reservation| reservation.request.key.coordinator_id == coordinator_id);
        if !is_owner {
            return false;
        }
        if let Some(reservation) = state.by_token.remove(&token) {
            state.by_offer.remove(&reservation.request.key);
            self.active.fetch_sub(1, Ordering::Relaxed);
        }
        true
    }

    pub fn reap_expired(&self, now_unix_ms: u64) -> usize {
        let mut state = self.state.lock().expect("reservation mutex poisoned");
        let expired: Vec<_> = state
            .by_token
            .iter()
            .filter_map(|(token, reservation)| {
                (reservation.grant.expires_unix_ms <= now_unix_ms).then_some(*token)
            })
            .collect();
        for token in &expired {
            if let Some(reservation) = state.by_token.remove(token) {
                state.by_offer.remove(&reservation.request.key);
                self.active.fetch_sub(1, Ordering::Relaxed);
            }
        }
        expired.len()
    }

    pub fn active(&self) -> usize {
        self.active.load(Ordering::Relaxed) as usize
    }

    pub fn active_counter(&self) -> Arc<AtomicU64> {
        self.active.clone()
    }

    pub fn available_slots(&self) -> usize {
        self.slots.available_permits()
    }

    pub fn available_memory_units(&self) -> usize {
        self.memory.available_permits()
    }
}

/// Phase-1 worker listener configuration. The caller must bind this only on a
/// private interface; public query traffic uses a different protocol and port.
pub struct WorkerService {
    authenticator: Arc<WorkerAuthenticator>,
    admission: Arc<WorkerAdmission>,
    postings: Arc<PostingsCache>,
    blooms: Arc<BloomCache>,
    connection_permits: Arc<Semaphore>,
    handshake_timeout: std::time::Duration,
}

impl WorkerService {
    pub fn new(
        authenticator: Arc<WorkerAuthenticator>,
        admission: Arc<WorkerAdmission>,
        postings: Arc<PostingsCache>,
        blooms: Arc<BloomCache>,
        maximum_connections: usize,
        handshake_timeout: std::time::Duration,
    ) -> Result<Self, AuthError> {
        if maximum_connections == 0
            || maximum_connections > Semaphore::MAX_PERMITS
            || handshake_timeout.is_zero()
        {
            return Err(AuthError::InvalidConfiguration);
        }
        Ok(Self {
            authenticator,
            admission,
            postings,
            blooms,
            connection_permits: Arc::new(Semaphore::new(maximum_connections)),
            handshake_timeout,
        })
    }

    pub async fn serve_with_shutdown(
        self: Arc<Self>,
        listener: TcpListener,
        shutdown: impl std::future::Future<Output = ()>,
    ) -> anyhow::Result<()> {
        tokio::pin!(shutdown);
        let mut reaper = tokio::time::interval(std::time::Duration::from_secs(1));
        reaper.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = &mut shutdown => return Ok(()),
                _ = reaper.tick() => {
                    self.admission.reap_expired(unix_ms_now());
                }
                accepted = listener.accept() => {
                    let (stream, _) = accepted?;
                    let Ok(permit) = self.connection_permits.clone().try_acquire_owned() else {
                        continue;
                    };
                    let service = self.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        let _ = service.handle_connection(stream).await;
                    });
                }
            }
        }
    }

    async fn handle_connection(&self, stream: TcpStream) -> anyhow::Result<()> {
        let (read, write) = stream.into_split();
        let mut read = BufReader::new(read);
        let mut write = BufWriter::new(write);
        let first = tokio::time::timeout(
            self.handshake_timeout,
            read_frame_with_limit::<QueryWorkerFrame, _>(&mut read, MAX_WORKER_CONTROL_FRAME_BYTES),
        )
        .await
        .map_err(|_| anyhow::anyhow!("query-worker handshake timed out"))??;
        validate_control_frame(&first, false)
            .map_err(|error| anyhow::anyhow!("invalid worker handshake: {error:?}"))?;
        let QueryWorkerFrameMsg::WorkerClientHello(wire_hello) = first.msg else {
            anyhow::bail!("worker client hello required");
        };
        let hello = ClientHello::try_from(wire_hello)?;
        let server = self.authenticator.authenticate(&hello, unix_ms_now())?;
        let session_key = self
            .authenticator
            .key_owned(&hello.key_id)
            .ok_or(AuthError::UnknownKey)?;
        write_frame(
            &mut write,
            &QueryWorkerFrame {
                msg: QueryWorkerFrameMsg::WorkerServerHello(
                    WorkerServerHelloInput::from(&server).into(),
                ),
            },
        )
        .await?;
        write.flush().await?;

        let mut receive_sequence = 0_u64;
        let mut send_sequence = 0_u64;
        loop {
            let envelope = match tokio::time::timeout(
                self.handshake_timeout,
                read_frame_with_limit::<QueryWorkerFrame, _>(
                    &mut read,
                    MAX_WORKER_CONTROL_FRAME_BYTES,
                ),
            )
            .await
            .map_err(|_| anyhow::anyhow!("query-worker connection idle timeout"))?
            {
                Ok(frame) => frame,
                Err(scry_proto::framing::FrameError::Io(error))
                    if error.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    return Ok(())
                }
                Err(error) => return Err(error.into()),
            };
            let QueryWorkerFrameMsg::WorkerAuthenticated(authenticated) = envelope.msg else {
                anyhow::bail!("authenticated worker record required");
            };
            let frame = open_record(
                &session_key,
                &hello,
                &server,
                CLIENT_TO_SERVER,
                receive_sequence,
                authenticated,
            )?;
            receive_sequence = receive_sequence
                .checked_add(1)
                .context("worker receive sequence exhausted")?;
            validate_control_frame(&frame, true)
                .map_err(|error| anyhow::anyhow!("invalid worker control frame: {error:?}"))?;
            let response = match frame.msg {
                QueryWorkerFrameMsg::WorkerBidRequest(request) => {
                    self.handle_bid(hello.coordinator_id, request)
                }
                QueryWorkerFrameMsg::WorkerRelease(release) => {
                    let coordinator = fixed::<16>(&release.coordinator_id)?;
                    if coordinator != hello.coordinator_id {
                        anyhow::bail!("release coordinator does not match authenticated peer");
                    }
                    let token = fixed::<RESERVATION_TOKEN_BYTES>(&release.reservation_token)?;
                    QueryWorkerFrame {
                        msg: QueryWorkerFrameMsg::WorkerReleaseAck(
                            WorkerReleaseAckInput {
                                reservation_token: token.to_vec(),
                                released: u8::from(self.admission.release(coordinator, token)),
                            }
                            .into(),
                        ),
                    }
                }
                QueryWorkerFrameMsg::WorkerCancel(cancel) => {
                    if fixed::<16>(&cancel.coordinator_id)? != hello.coordinator_id {
                        anyhow::bail!("cancel coordinator does not match authenticated peer");
                    }
                    QueryWorkerFrame {
                        // Execute is deliberately absent in Phase 1, so cancellation
                        // is idempotently acknowledged as having no active fragment.
                        msg: QueryWorkerFrameMsg::WorkerCancelAck(
                            WorkerCancelAckInput {
                                fragment_id: cancel.fragment_id,
                                fragment_attempt: cancel.fragment_attempt,
                                cancelled: 0,
                            }
                            .into(),
                        ),
                    }
                }
                _ => anyhow::bail!("unexpected query-worker message direction"),
            };
            let response = seal_record(
                &session_key,
                &hello,
                &server,
                SERVER_TO_CLIENT,
                send_sequence,
                response,
            )?;
            send_sequence = send_sequence
                .checked_add(1)
                .context("worker send sequence exhausted")?;
            write_frame(&mut write, &response).await?;
            write.flush().await?;
        }
    }

    fn handle_bid(
        &self,
        authenticated_coordinator: [u8; 16],
        request: scry_proto::WorkerBidRequest,
    ) -> QueryWorkerFrame {
        let decline = |reason: u16, message: &str| QueryWorkerFrame {
            msg: QueryWorkerFrameMsg::WorkerBidDecline(
                WorkerBidDeclineInput {
                    offer_id: request.offer_id.clone(),
                    reason,
                    message: message.into(),
                }
                .into(),
            ),
        };
        let Ok(coordinator) = fixed::<16>(&request.coordinator_id) else {
            return decline(
                scry_proto::constants::QUERY_WORKER_DECLINE_UNSUPPORTED,
                "bad coordinator",
            );
        };
        if coordinator != authenticated_coordinator {
            return decline(
                scry_proto::constants::QUERY_WORKER_DECLINE_UNSUPPORTED,
                "foreign coordinator",
            );
        }
        let now = unix_ms_now();
        if request.deadline_unix_ms <= now {
            return decline(
                scry_proto::constants::QUERY_WORKER_DECLINE_DEADLINE,
                "deadline expired",
            );
        }
        let grant = match self.admission.reserve(ReservationRequest {
            key: OfferKey {
                coordinator_id: coordinator,
                offer_id: match fixed::<16>(&request.offer_id) {
                    Ok(value) => value,
                    Err(_) => {
                        return decline(
                            scry_proto::constants::QUERY_WORKER_DECLINE_UNSUPPORTED,
                            "bad offer",
                        )
                    }
                },
            },
            block_set_digest: match fixed::<32>(&request.block_set_digest) {
                Ok(value) => value,
                Err(_) => {
                    return decline(
                        scry_proto::constants::QUERY_WORKER_DECLINE_UNSUPPORTED,
                        "bad digest",
                    )
                }
            },
            operation: request.operation,
            memory_units: request.memory_units,
            now_unix_ms: now,
        }) {
            Ok(grant) => grant,
            Err(reason) => {
                let code = match reason {
                    ReservationDecline::Busy | ReservationDecline::Capacity => {
                        scry_proto::constants::QUERY_WORKER_DECLINE_BUSY
                    }
                    ReservationDecline::MemoryPressure => {
                        scry_proto::constants::QUERY_WORKER_DECLINE_MEMORY_PRESSURE
                    }
                    ReservationDecline::Invalid => {
                        scry_proto::constants::QUERY_WORKER_DECLINE_UNSUPPORTED
                    }
                };
                return decline(code, "worker admission declined");
            }
        };
        let locality = request
            .blocks
            .iter()
            .filter_map(|block| {
                let uuid = Uuid::from_slice(&block.uuid).ok()?;
                let postings_ready = request.requires_postings == 0 || self.postings.resident(uuid);
                let bloom_ready = request.requires_bloom == 0
                    || self.blooms.residency(uuid) == BloomCacheResidency::Usable;
                let class = if postings_ready
                    && bloom_ready
                    && (request.requires_postings == 1 || request.requires_bloom == 1)
                {
                    4
                } else {
                    5
                };
                Some(WorkerBlockLocality {
                    uuid: block.uuid.clone(),
                    locality: class,
                })
            })
            .collect();
        QueryWorkerFrame {
            msg: QueryWorkerFrameMsg::WorkerBidResponse(
                WorkerBidResponseInput {
                    offer_id: request.offer_id,
                    worker_id: self.authenticator.worker_id.to_vec(),
                    locality_generation: 0,
                    locality,
                    reservation_token: grant.token.to_vec(),
                    reservation_expires_unix_ms: grant.expires_unix_ms,
                    estimated_start_delay_ms: 0,
                    available_fragment_slots: self
                        .admission
                        .available_slots()
                        .min(u16::MAX as usize)
                        as u16,
                    memory_pressure_per_mille: 0,
                }
                .into(),
            ),
        }
    }
}

impl TryFrom<scry_proto::WorkerClientHello> for ClientHello {
    type Error = anyhow::Error;

    fn try_from(value: scry_proto::WorkerClientHello) -> Result<Self, Self::Error> {
        Ok(Self {
            protocol_version: value.protocol_version,
            coordinator_id: fixed(&value.coordinator_id)?,
            expected_worker_id: fixed(&value.expected_worker_id)?,
            deployment: value.deployment,
            timestamp_unix_ms: value.timestamp_unix_ms,
            nonce: fixed(&value.nonce)?,
            key_id: value.key_id,
            mac: fixed(&value.mac)?,
        })
    }
}

impl From<&ServerHello> for WorkerServerHelloInput {
    fn from(value: &ServerHello) -> Self {
        Self {
            protocol_version: value.protocol_version,
            worker_id: value.worker_id.to_vec(),
            coordinator_nonce: value.coordinator_nonce.to_vec(),
            worker_nonce: value.worker_nonce.to_vec(),
            timestamp_unix_ms: value.timestamp_unix_ms,
            key_id: value.key_id.clone(),
            mac: value.mac.to_vec(),
        }
    }
}

fn fixed<const N: usize>(value: &[u8]) -> anyhow::Result<[u8; N]> {
    value
        .try_into()
        .map_err(|_| anyhow::anyhow!("expected {N} bytes, got {}", value.len()))
}

fn unix_ms_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "query-worker authentication failed: {self:?}")
    }
}

impl std::error::Error for AuthError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> AuthKey {
        AuthKey::new("current", vec![0x5a; 32]).unwrap()
    }

    fn hello() -> ClientHello {
        let mut hello = ClientHello {
            protocol_version: WORKER_PROTOCOL_VERSION,
            coordinator_id: [1; 16],
            expected_worker_id: [2; 16],
            deployment: "test".into(),
            timestamp_unix_ms: 1_000,
            nonce: [3; NONCE_BYTES],
            key_id: String::new(),
            mac: [0; AUTH_MAC_BYTES],
        };
        sign_client_hello(&key(), &mut hello);
        hello
    }

    #[test]
    fn mutual_authentication_binds_transcript_and_rejects_replay() {
        let auth = WorkerAuthenticator::new([2; 16], "test", key(), None, 100, 8).unwrap();
        let client = hello();
        let server = auth.authenticate(&client, 1_050).unwrap();
        verify_server_hello(&key(), &client, &server, 1_050, 100).unwrap();
        let mut wrong_version = server.clone();
        wrong_version.protocol_version += 1;
        assert_eq!(
            verify_server_hello(&key(), &client, &wrong_version, 1_050, 100),
            Err(AuthError::InvalidMessage)
        );
        assert_eq!(auth.authenticate(&client, 1_050), Err(AuthError::Replay));
        let mut tampered = hello();
        tampered.deployment = "other".into();
        assert_eq!(
            auth.authenticate(&tampered, 1_050),
            Err(AuthError::InvalidMessage)
        );
    }

    #[test]
    fn bad_mac_and_stale_handshakes_are_rejected() {
        let auth = WorkerAuthenticator::new([2; 16], "test", key(), None, 100, 8).unwrap();
        let mut bad = hello();
        bad.mac[0] ^= 1;
        assert_eq!(auth.authenticate(&bad, 1_050), Err(AuthError::BadMac));
        assert_eq!(auth.authenticate(&hello(), 1_101), Err(AuthError::Stale));
    }

    #[test]
    fn client_mac_has_stable_test_vector() {
        let signed = hello();
        assert_eq!(
            signed.mac,
            [
                0x0a, 0x06, 0x3e, 0xf2, 0x77, 0x2f, 0xfa, 0x50, 0xaa, 0x0b, 0x26, 0x3a, 0xe7, 0xb3,
                0x22, 0x2b, 0x02, 0x3a, 0x9a, 0x40, 0x89, 0xa7, 0x1f, 0x2d, 0xf5, 0xf7, 0xce, 0x5d,
                0xb0, 0xcb, 0xa8, 0x26,
            ]
        );
    }

    fn request(offer: u8, units: u32, now: u64) -> ReservationRequest {
        ReservationRequest {
            key: OfferKey {
                coordinator_id: [1; 16],
                offer_id: [offer; 16],
            },
            block_set_digest: [offer; 32],
            operation: 1,
            memory_units: units,
            now_unix_ms: now,
        }
    }

    #[test]
    fn reservations_are_nonblocking_idempotent_and_release_capacity() {
        let admission = WorkerAdmission::new(1, 4, 100, 8).unwrap();
        let first = admission.reserve(request(1, 3, 1_000)).unwrap();
        assert_eq!(admission.reserve(request(1, 3, 1_001)).unwrap(), first);
        assert_eq!(admission.active(), 1);
        assert_eq!(admission.available_slots(), 0);
        assert_eq!(admission.available_memory_units(), 1);
        assert_eq!(
            admission.reserve(request(2, 1, 1_001)),
            Err(ReservationDecline::Busy)
        );
        assert!(!admission.release([9; 16], first.token));
        assert!(admission.release([1; 16], first.token));
        assert!(admission.release([1; 16], first.token) == false);
        assert_eq!(admission.available_slots(), 1);
        assert_eq!(admission.available_memory_units(), 4);
    }

    #[test]
    fn expiry_and_memory_pressure_restore_permits() {
        let admission = WorkerAdmission::new(2, 3, 100, 8).unwrap();
        admission.reserve(request(1, 3, 1_000)).unwrap();
        assert_eq!(
            admission.reserve(request(2, 1, 1_001)),
            Err(ReservationDecline::MemoryPressure)
        );
        assert_eq!(admission.reap_expired(1_100), 1);
        assert_eq!(admission.available_slots(), 2);
        assert_eq!(admission.available_memory_units(), 3);
    }

    #[test]
    fn authenticated_records_bind_direction_sequence_and_payload() {
        let key = key();
        let client = hello();
        let auth = WorkerAuthenticator::new([2; 16], "test", key.clone(), None, 100, 8).unwrap();
        let server = auth.authenticate(&client, 1_050).unwrap();
        let payload = QueryWorkerFrame {
            msg: QueryWorkerFrameMsg::WorkerCancelAck(
                WorkerCancelAckInput {
                    fragment_id: vec![7; 16],
                    fragment_attempt: 1,
                    cancelled: 0,
                }
                .into(),
            ),
        };
        let sealed = seal_record(&key, &client, &server, SERVER_TO_CLIENT, 0, payload).unwrap();
        let QueryWorkerFrameMsg::WorkerAuthenticated(envelope) = sealed.msg else {
            panic!("authenticated envelope expected")
        };
        assert!(open_record(
            &key,
            &client,
            &server,
            SERVER_TO_CLIENT,
            0,
            envelope.clone()
        )
        .is_ok());
        assert!(open_record(
            &key,
            &client,
            &server,
            CLIENT_TO_SERVER,
            0,
            envelope.clone()
        )
        .is_err());
        assert!(open_record(
            &key,
            &client,
            &server,
            SERVER_TO_CLIENT,
            1,
            envelope.clone()
        )
        .is_err());
        let mut tampered = envelope;
        tampered.payload[1] ^= 1;
        assert!(open_record(&key, &client, &server, SERVER_TO_CLIENT, 0, tampered).is_err());
    }

    #[test]
    fn configuration_rejects_semaphore_overflow() {
        assert!(matches!(
            WorkerAdmission::new(Semaphore::MAX_PERMITS + 1, 1, 1, 1),
            Err(ReservationDecline::Invalid)
        ));
    }
}
