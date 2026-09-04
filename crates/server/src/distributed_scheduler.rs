//! Pure scheduling primitives for cache-aware distributed query execution.
//!
//! This module deliberately contains no discovery, admission, or transport code.
//! Status snapshots and exact bid results are inputs; the output is a deterministic,
//! disjoint block assignment which callers may subsequently dispatch.

use std::collections::{BTreeMap, BTreeSet};

/// Hard bounds prevent configuration mistakes from turning one query into
/// unbounded scheduler work.
pub const MAX_WORKERS_PER_QUERY: usize = 64;
pub const MAX_WORKER_SNAPSHOTS: usize = 256;
pub const MAX_BLOCKS_PER_QUERY: usize = 100_000;
pub const MAX_BLOCKS_PER_WORKER: usize = 50_000;
pub const MAX_OFFERS_PER_BLOCK: usize = MAX_WORKER_SNAPSHOTS;
pub const MAX_BYTES_PER_WORKER: u64 = 1 << 50; // 1 PiB

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerRole {
    Query,
    Other,
}

/// The coarse status used before an exact bid is requested.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerSnapshot {
    pub worker_id: String,
    pub role: WorkerRole,
    pub deployment: String,
    pub fragment_protocol_version: u32,
    pub schema_versions: BTreeSet<u32>,
    pub heartbeat_unix_ms: u64,
    pub draining: bool,
    pub fragment_slots_limit: u32,
    pub fragment_slots_in_use: u32,
    pub admission_waiters: u32,
    pub recently_rejected: bool,
    /// Maximum of the relevant DataFusion and cgroup pressure, in per-mille.
    pub memory_pressure_per_mille: u16,
    /// A normalized current-load cost. It must use the same unit as bid costs.
    pub load_cost: u64,
    /// Conservative dispatch plus work estimate used for deadline filtering.
    pub minimum_completion_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EligibilityRequirements {
    pub deployment: String,
    pub protocol_version: u32,
    pub schema_version: u32,
    pub now_unix_ms: u64,
    pub remaining_deadline_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EligibilityPolicy {
    pub freshness_ms: u64,
    pub maximum_future_skew_ms: u64,
    pub maximum_memory_pressure_per_mille: u16,
}

impl Default for EligibilityPolicy {
    fn default() -> Self {
        Self {
            freshness_ms: 10_000,
            maximum_future_skew_ms: 2_000,
            maximum_memory_pressure_per_mille: 700,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IneligibilityReason {
    InvalidSnapshot,
    Stale,
    IncompatibleRole,
    IncompatibleDeployment,
    IncompatibleProtocol,
    IncompatibleSchema,
    Draining,
    Busy,
    AdmissionQueued,
    RecentlyRejecting,
    MemoryPressure,
    Deadline,
}

impl WorkerSnapshot {
    pub fn eligibility(
        &self,
        requirements: &EligibilityRequirements,
        policy: &EligibilityPolicy,
    ) -> Result<(), IneligibilityReason> {
        if self.worker_id.is_empty()
            || self.fragment_slots_limit == 0
            || self.fragment_slots_in_use > self.fragment_slots_limit
            || self.memory_pressure_per_mille > 1_000
        {
            return Err(IneligibilityReason::InvalidSnapshot);
        }
        if self.heartbeat_unix_ms
            > requirements
                .now_unix_ms
                .saturating_add(policy.maximum_future_skew_ms)
            || requirements
                .now_unix_ms
                .saturating_sub(self.heartbeat_unix_ms)
                > policy.freshness_ms
        {
            return Err(IneligibilityReason::Stale);
        }
        if self.role != WorkerRole::Query {
            return Err(IneligibilityReason::IncompatibleRole);
        }
        if self.deployment != requirements.deployment {
            return Err(IneligibilityReason::IncompatibleDeployment);
        }
        if self.fragment_protocol_version != requirements.protocol_version {
            return Err(IneligibilityReason::IncompatibleProtocol);
        }
        if !self.schema_versions.contains(&requirements.schema_version) {
            return Err(IneligibilityReason::IncompatibleSchema);
        }
        if self.draining {
            return Err(IneligibilityReason::Draining);
        }
        if self.fragment_slots_in_use == self.fragment_slots_limit {
            return Err(IneligibilityReason::Busy);
        }
        if self.admission_waiters != 0 {
            return Err(IneligibilityReason::AdmissionQueued);
        }
        if self.recently_rejected {
            return Err(IneligibilityReason::RecentlyRejecting);
        }
        if self.memory_pressure_per_mille > policy.maximum_memory_pressure_per_mille {
            return Err(IneligibilityReason::MemoryPressure);
        }
        if self.minimum_completion_ms > requirements.remaining_deadline_ms {
            return Err(IneligibilityReason::Deadline);
        }
        Ok(())
    }
}

/// Exact, application-observable residency returned by a bid.
/// Variant order is the preference order (best to cold).
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum LocalityClass {
    CompleteMain,
    AllRequiredRanges,
    SomeRequiredRanges,
    FooterAndRequiredSidecars,
    RequiredSidecars,
    Cold,
}

impl LocalityClass {
    const fn rank(self) -> u64 {
        self as u64
    }
}

/// One exact per-block result from a worker bid. An offer is usable only when
/// admission succeeded; retaining declines in the input is useful for diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockOffer {
    pub worker_id: String,
    pub block_id: String,
    pub locality: LocalityClass,
    pub admitted: bool,
    /// Incremental completion cost excluding the snapshot's current load.
    pub estimated_cost: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchedulableBlock {
    pub block_id: String,
    pub estimated_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchedulerConfig {
    pub maximum_workers_per_query: usize,
    pub maximum_blocks_per_query: usize,
    pub maximum_blocks_per_worker: usize,
    pub maximum_bytes_per_worker: u64,
    /// Estimated bytes represented by one normalized load-cost unit.
    pub bytes_per_load_cost: u64,
    /// Soft cost between adjacent locality classes. Load can override it.
    pub locality_step_cost: u64,
    /// Soft cost paid by cold workers other than the rendezvous winner.
    pub cold_non_affinity_cost: u64,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            maximum_workers_per_query: 8,
            maximum_blocks_per_query: 10_000,
            maximum_blocks_per_worker: 2_000,
            maximum_bytes_per_worker: 1 << 40,
            bytes_per_load_cost: 1 << 20,
            locality_step_cost: 100,
            cold_non_affinity_cost: 25,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigError {
    pub field: &'static str,
    pub message: &'static str,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid {}: {}", self.field, self.message)
    }
}

impl std::error::Error for ConfigError {}

impl EligibilityPolicy {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !(100..=300_000).contains(&self.freshness_ms) {
            return Err(config_error("freshness_ms", "must be in 100..=300000"));
        }
        if self.maximum_future_skew_ms > 60_000 {
            return Err(config_error("maximum_future_skew_ms", "must be <= 60000"));
        }
        if self.maximum_memory_pressure_per_mille > 1_000 {
            return Err(config_error(
                "maximum_memory_pressure_per_mille",
                "must be <= 1000",
            ));
        }
        Ok(())
    }
}

impl SchedulerConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !(1..=MAX_WORKERS_PER_QUERY).contains(&self.maximum_workers_per_query) {
            return Err(config_error(
                "maximum_workers_per_query",
                "outside hard bounds",
            ));
        }
        if !(1..=MAX_BLOCKS_PER_QUERY).contains(&self.maximum_blocks_per_query) {
            return Err(config_error(
                "maximum_blocks_per_query",
                "outside hard bounds",
            ));
        }
        if !(1..=MAX_BLOCKS_PER_WORKER).contains(&self.maximum_blocks_per_worker) {
            return Err(config_error(
                "maximum_blocks_per_worker",
                "outside hard bounds",
            ));
        }
        if !(1..=MAX_BYTES_PER_WORKER).contains(&self.maximum_bytes_per_worker) {
            return Err(config_error(
                "maximum_bytes_per_worker",
                "outside hard bounds",
            ));
        }
        if self.bytes_per_load_cost == 0 {
            return Err(config_error("bytes_per_load_cost", "must be non-zero"));
        }
        if self.locality_step_cost > 1_000_000_000 {
            return Err(config_error("locality_step_cost", "must be <= 1000000000"));
        }
        if self.cold_non_affinity_cost > 1_000_000_000 {
            return Err(config_error(
                "cold_non_affinity_cost",
                "must be <= 1000000000",
            ));
        }
        Ok(())
    }
}

const fn config_error(field: &'static str, message: &'static str) -> ConfigError {
    ConfigError { field, message }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssignmentReason {
    Locality(LocalityClass),
    RendezvousAffinity,
    LowerLoad,
    CapacityOverride,
    LocalFallback,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockAssignment {
    pub block_id: String,
    pub worker_id: String,
    pub estimated_bytes: u64,
    pub locality: LocalityClass,
    pub reason: AssignmentReason,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerExclusion {
    pub worker_id: String,
    pub reason: IneligibilityReason,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Schedule {
    pub assignments: Vec<BlockAssignment>,
    pub excluded_workers: Vec<WorkerExclusion>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScheduleError {
    InvalidConfig(ConfigError),
    TooManyBlocks { count: usize, maximum: usize },
    DuplicateBlock(String),
    DuplicateWorker(String),
    DuplicateOffer { worker_id: String, block_id: String },
    TooManyWorkers { count: usize, maximum: usize },
    TooManyOffers { count: usize, maximum: usize },
    UnknownOfferBlock(String),
    UnknownOfferWorker(String),
    DeploymentMismatch,
    MissingCoordinator,
}

impl std::fmt::Display for ScheduleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "distributed scheduling failed: {self:?}")
    }
}

impl std::error::Error for ScheduleError {}

/// Assign every block exactly once. Remote assignments obey worker/count/byte
/// caps; anything which cannot safely be dispatched is retained by the local
/// coordinator. Input order does not affect the result.
pub fn schedule_blocks(
    deployment: &str,
    signal: &str,
    coordinator_id: &str,
    blocks: &[SchedulableBlock],
    workers: &[WorkerSnapshot],
    offers: &[BlockOffer],
    requirements: &EligibilityRequirements,
    eligibility_policy: &EligibilityPolicy,
    config: &SchedulerConfig,
) -> Result<Schedule, ScheduleError> {
    eligibility_policy
        .validate()
        .map_err(ScheduleError::InvalidConfig)?;
    config.validate().map_err(ScheduleError::InvalidConfig)?;
    if coordinator_id.is_empty() {
        return Err(ScheduleError::MissingCoordinator);
    }
    if deployment != requirements.deployment {
        return Err(ScheduleError::DeploymentMismatch);
    }
    if workers.len() > MAX_WORKER_SNAPSHOTS {
        return Err(ScheduleError::TooManyWorkers {
            count: workers.len(),
            maximum: MAX_WORKER_SNAPSHOTS,
        });
    }
    let maximum_offers = blocks.len().saturating_mul(MAX_OFFERS_PER_BLOCK);
    if offers.len() > maximum_offers {
        return Err(ScheduleError::TooManyOffers {
            count: offers.len(),
            maximum: maximum_offers,
        });
    }
    if blocks.len() > config.maximum_blocks_per_query {
        return Err(ScheduleError::TooManyBlocks {
            count: blocks.len(),
            maximum: config.maximum_blocks_per_query,
        });
    }

    let mut block_ids = BTreeSet::new();
    for block in blocks {
        if block.block_id.is_empty() || !block_ids.insert(block.block_id.as_str()) {
            return Err(ScheduleError::DuplicateBlock(block.block_id.clone()));
        }
    }

    let mut eligible = BTreeMap::new();
    let mut exclusions = Vec::new();
    for worker in workers {
        if eligible.contains_key(worker.worker_id.as_str())
            || exclusions
                .iter()
                .any(|e: &WorkerExclusion| e.worker_id == worker.worker_id)
        {
            return Err(ScheduleError::DuplicateWorker(worker.worker_id.clone()));
        }
        match worker.eligibility(requirements, eligibility_policy) {
            Ok(()) => {
                eligible.insert(worker.worker_id.as_str(), worker);
            }
            Err(reason) => exclusions.push(WorkerExclusion {
                worker_id: worker.worker_id.clone(),
                reason,
            }),
        }
    }
    exclusions.sort_by(|a, b| a.worker_id.cmp(&b.worker_id));

    let mut offer_map = BTreeMap::new();
    for offer in offers {
        if !block_ids.contains(offer.block_id.as_str()) {
            return Err(ScheduleError::UnknownOfferBlock(offer.block_id.clone()));
        }
        if !eligible.contains_key(offer.worker_id.as_str()) {
            return Err(ScheduleError::UnknownOfferWorker(offer.worker_id.clone()));
        }
        let key = (offer.block_id.as_str(), offer.worker_id.as_str());
        if offer_map.insert(key, offer).is_some() {
            return Err(ScheduleError::DuplicateOffer {
                worker_id: offer.worker_id.clone(),
                block_id: offer.block_id.clone(),
            });
        }
    }

    let mut ordered: Vec<_> = blocks.iter().collect();
    ordered.sort_by(|a, b| {
        let best_a = best_locality(&a.block_id, &eligible, &offer_map);
        let best_b = best_locality(&b.block_id, &eligible, &offer_map);
        best_a
            .cmp(&best_b)
            .then_with(|| b.estimated_bytes.cmp(&a.estimated_bytes))
            .then_with(|| a.block_id.cmp(&b.block_id))
    });

    let affinity_workers: Vec<&str> = eligible
        .keys()
        .copied()
        .filter(|id| *id != coordinator_id)
        .collect();
    let mut states: BTreeMap<&str, WorkerState> = BTreeMap::new();
    let mut active_remote = BTreeSet::new();
    let mut assignments = Vec::with_capacity(blocks.len());

    for block in ordered {
        let affinity = rendezvous_worker(
            deployment,
            signal,
            &block.block_id,
            affinity_workers.iter().copied(),
        );
        let mut candidates = Vec::new();
        for (&worker_id, worker) in &eligible {
            let Some(offer) = offer_map.get(&(block.block_id.as_str(), worker_id)) else {
                continue;
            };
            if !offer.admitted {
                continue;
            }
            let is_local = worker_id == coordinator_id;
            let state = states.get(worker_id).copied().unwrap_or_default();
            if !is_local {
                if state.blocks >= config.maximum_blocks_per_worker
                    || state.bytes.saturating_add(block.estimated_bytes)
                        > config.maximum_bytes_per_worker
                    || (!active_remote.contains(worker_id)
                        && active_remote.len() >= config.maximum_workers_per_query)
                {
                    continue;
                }
            }
            let projected_units = div_ceil(
                state.bytes.saturating_add(block.estimated_bytes),
                config.bytes_per_load_cost,
            );
            let affinity_penalty = if offer.locality == LocalityClass::Cold
                && affinity.is_some_and(|winner| winner != worker_id)
            {
                config.cold_non_affinity_cost
            } else {
                0
            };
            let score = worker
                .load_cost
                .saturating_add(projected_units)
                .saturating_add(offer.estimated_cost)
                .saturating_add(
                    offer
                        .locality
                        .rank()
                        .saturating_mul(config.locality_step_cost),
                )
                .saturating_add(affinity_penalty);
            candidates.push(Candidate {
                worker_id,
                locality: offer.locality,
                score,
                is_local,
            });
        }
        candidates.sort_by(|a, b| {
            a.score
                .cmp(&b.score)
                .then_with(|| a.worker_id.cmp(b.worker_id))
        });

        if let Some(chosen) = candidates.first() {
            let preferred_was_capped = affinity.is_some_and(|winner| {
                offer_map.contains_key(&(block.block_id.as_str(), winner))
                    && !candidates
                        .iter()
                        .any(|candidate| candidate.worker_id == winner)
            });
            let reason = if chosen.locality != LocalityClass::Cold {
                AssignmentReason::Locality(chosen.locality)
            } else if affinity == Some(chosen.worker_id) {
                AssignmentReason::RendezvousAffinity
            } else if preferred_was_capped {
                AssignmentReason::CapacityOverride
            } else {
                AssignmentReason::LowerLoad
            };
            let state = states.entry(chosen.worker_id).or_default();
            state.blocks += 1;
            state.bytes = state.bytes.saturating_add(block.estimated_bytes);
            if !chosen.is_local {
                active_remote.insert(chosen.worker_id);
            }
            assignments.push(BlockAssignment {
                block_id: block.block_id.clone(),
                worker_id: chosen.worker_id.to_owned(),
                estimated_bytes: block.estimated_bytes,
                locality: chosen.locality,
                reason,
            });
        } else {
            assignments.push(BlockAssignment {
                block_id: block.block_id.clone(),
                worker_id: coordinator_id.to_owned(),
                estimated_bytes: block.estimated_bytes,
                locality: LocalityClass::Cold,
                reason: AssignmentReason::LocalFallback,
            });
        }
    }
    assignments.sort_by(|a, b| a.block_id.cmp(&b.block_id));
    Ok(Schedule {
        assignments,
        excluded_workers: exclusions,
    })
}

#[derive(Clone, Copy, Default)]
struct WorkerState {
    blocks: usize,
    bytes: u64,
}

struct Candidate<'a> {
    worker_id: &'a str,
    locality: LocalityClass,
    score: u64,
    is_local: bool,
}

fn best_locality<'a>(
    block_id: &str,
    eligible: &BTreeMap<&str, &'a WorkerSnapshot>,
    offers: &BTreeMap<(&str, &str), &'a BlockOffer>,
) -> LocalityClass {
    eligible
        .keys()
        .filter_map(|worker| offers.get(&(block_id, *worker)))
        .filter(|offer| offer.admitted)
        .map(|offer| offer.locality)
        .min()
        .unwrap_or(LocalityClass::Cold)
}

const fn div_ceil(value: u64, divisor: u64) -> u64 {
    value / divisor + if value % divisor == 0 { 0 } else { 1 }
}

/// Return the highest-scoring rendezvous member. Adding one worker can only move
/// a key to that worker; removing one only moves keys previously owned by it.
pub fn rendezvous_worker<'a>(
    deployment: &str,
    signal: &str,
    block_id: &str,
    workers: impl IntoIterator<Item = &'a str>,
) -> Option<&'a str> {
    workers.into_iter().max_by(|left, right| {
        rendezvous_score(deployment, signal, block_id, left)
            .cmp(&rendezvous_score(deployment, signal, block_id, right))
            .then_with(|| right.cmp(left)) // lexicographically smaller wins a hash tie
    })
}

fn rendezvous_score(deployment: &str, signal: &str, block_id: &str, worker: &str) -> u64 {
    // Length framing prevents tuple ambiguity. FNV-1a plus a SplitMix64 finalizer
    // is stable across processes and Rust releases (unlike DefaultHasher).
    let mut hash = 0xcbf29ce484222325_u64;
    for component in [deployment, signal, block_id, worker] {
        for byte in (component.len() as u64)
            .to_le_bytes()
            .iter()
            .chain(component.as_bytes())
        {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    hash ^= hash >> 30;
    hash = hash.wrapping_mul(0xbf58476d1ce4e5b9);
    hash ^= hash >> 27;
    hash = hash.wrapping_mul(0x94d049bb133111eb);
    hash ^ (hash >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn worker(id: &str) -> WorkerSnapshot {
        WorkerSnapshot {
            worker_id: id.into(),
            role: WorkerRole::Query,
            deployment: "prod".into(),
            fragment_protocol_version: 1,
            schema_versions: BTreeSet::from([7]),
            heartbeat_unix_ms: 10_000,
            draining: false,
            fragment_slots_limit: 2,
            fragment_slots_in_use: 0,
            admission_waiters: 0,
            recently_rejected: false,
            memory_pressure_per_mille: 100,
            load_cost: 0,
            minimum_completion_ms: 10,
        }
    }

    fn requirements() -> EligibilityRequirements {
        EligibilityRequirements {
            deployment: "prod".into(),
            protocol_version: 1,
            schema_version: 7,
            now_unix_ms: 10_000,
            remaining_deadline_ms: 1_000,
        }
    }

    fn offer(worker: &str, block: &str, locality: LocalityClass) -> BlockOffer {
        BlockOffer {
            worker_id: worker.into(),
            block_id: block.into(),
            locality,
            admitted: true,
            estimated_cost: 0,
        }
    }

    fn run(
        blocks: &[SchedulableBlock],
        workers: &[WorkerSnapshot],
        offers: &[BlockOffer],
        config: &SchedulerConfig,
    ) -> Schedule {
        schedule_blocks(
            "prod",
            "logs",
            "local",
            blocks,
            workers,
            offers,
            &requirements(),
            &EligibilityPolicy::default(),
            config,
        )
        .unwrap()
    }

    #[test]
    fn excludes_stale_busy_pressured_and_incompatible_workers() {
        let mut stale = worker("stale");
        stale.heartbeat_unix_ms = 20_000;
        let mut busy = worker("busy");
        busy.fragment_slots_in_use = busy.fragment_slots_limit;
        let mut pressured = worker("pressured");
        pressured.memory_pressure_per_mille = 701;
        let mut incompatible = worker("old");
        incompatible.fragment_protocol_version = 2;
        let workers = [stale, busy, pressured, incompatible];
        let schedule = run(&[], &workers, &[], &SchedulerConfig::default());
        assert_eq!(
            schedule.excluded_workers,
            vec![
                WorkerExclusion {
                    worker_id: "busy".into(),
                    reason: IneligibilityReason::Busy
                },
                WorkerExclusion {
                    worker_id: "old".into(),
                    reason: IneligibilityReason::IncompatibleProtocol
                },
                WorkerExclusion {
                    worker_id: "pressured".into(),
                    reason: IneligibilityReason::MemoryPressure
                },
                WorkerExclusion {
                    worker_id: "stale".into(),
                    reason: IneligibilityReason::Stale
                },
            ]
        );
    }

    #[test]
    fn locality_is_preferred_at_similar_load() {
        let blocks = [SchedulableBlock {
            block_id: "b".into(),
            estimated_bytes: 10,
        }];
        let workers = [worker("cold"), worker("warm")];
        let offers = [
            offer("cold", "b", LocalityClass::Cold),
            offer("warm", "b", LocalityClass::CompleteMain),
        ];
        let result = run(&blocks, &workers, &offers, &SchedulerConfig::default());
        assert_eq!(result.assignments[0].worker_id, "warm");
        assert_eq!(
            result.assignments[0].reason,
            AssignmentReason::Locality(LocalityClass::CompleteMain)
        );
    }

    #[test]
    fn sufficiently_lower_load_overrides_locality() {
        let blocks = [SchedulableBlock {
            block_id: "b".into(),
            estimated_bytes: 10,
        }];
        let mut warm = worker("warm");
        warm.load_cost = 1_000;
        let workers = [worker("cold"), warm];
        let offers = [
            offer("cold", "b", LocalityClass::Cold),
            offer("warm", "b", LocalityClass::CompleteMain),
        ];
        assert_eq!(
            run(&blocks, &workers, &offers, &SchedulerConfig::default()).assignments[0].worker_id,
            "cold"
        );
    }

    #[test]
    fn assignments_are_disjoint_and_byte_balanced() {
        let blocks: Vec<_> = [80, 70, 30, 20]
            .into_iter()
            .enumerate()
            .map(|(i, bytes)| SchedulableBlock {
                block_id: format!("b{i}"),
                estimated_bytes: bytes,
            })
            .collect();
        let workers = [worker("a"), worker("b")];
        let offers: Vec<_> = blocks
            .iter()
            .flat_map(|block| {
                [
                    offer("a", &block.block_id, LocalityClass::Cold),
                    offer("b", &block.block_id, LocalityClass::Cold),
                ]
            })
            .collect();
        let mut config = SchedulerConfig::default();
        config.bytes_per_load_cost = 1;
        config.cold_non_affinity_cost = 0;
        let result = run(&blocks, &workers, &offers, &config);
        assert_eq!(result.assignments.len(), blocks.len());
        assert_eq!(
            result
                .assignments
                .iter()
                .map(|a| &a.block_id)
                .collect::<BTreeSet<_>>()
                .len(),
            blocks.len()
        );
        let mut totals = BTreeMap::new();
        for assignment in result.assignments {
            *totals.entry(assignment.worker_id).or_insert(0_u64) += assignment.estimated_bytes;
        }
        assert_eq!(totals.values().copied().collect::<Vec<_>>(), vec![100, 100]);
    }

    #[test]
    fn rendezvous_is_stable_and_minimally_remaps() {
        let old = ["a", "b", "c"];
        let added = ["a", "b", "c", "d"];
        for i in 0..1_000 {
            let key = format!("block-{i}");
            let before = rendezvous_worker("prod", "logs", &key, old).unwrap();
            let after = rendezvous_worker("prod", "logs", &key, added).unwrap();
            assert!(after == before || after == "d");
            assert_eq!(
                before,
                rendezvous_worker("prod", "logs", &key, old).unwrap()
            );
        }
        let removed = ["a", "c"];
        for i in 0..1_000 {
            let key = format!("block-{i}");
            let before = rendezvous_worker("prod", "logs", &key, old).unwrap();
            let after = rendezvous_worker("prod", "logs", &key, removed).unwrap();
            assert!(before == "b" || after == before);
        }
    }

    #[test]
    fn caps_force_other_workers_then_local_fallback() {
        let blocks: Vec<_> = (0..3)
            .map(|i| SchedulableBlock {
                block_id: format!("b{i}"),
                estimated_bytes: 10,
            })
            .collect();
        let workers = [worker("a"), worker("b")];
        let offers: Vec<_> = blocks
            .iter()
            .flat_map(|block| {
                [
                    offer("a", &block.block_id, LocalityClass::Cold),
                    offer("b", &block.block_id, LocalityClass::Cold),
                ]
            })
            .collect();
        let mut config = SchedulerConfig::default();
        config.maximum_workers_per_query = 1;
        config.maximum_blocks_per_worker = 1;
        let result = run(&blocks, &workers, &offers, &config);
        assert_eq!(
            result
                .assignments
                .iter()
                .filter(|a| a.worker_id != "local")
                .count(),
            1
        );
        assert_eq!(
            result
                .assignments
                .iter()
                .filter(|a| a.reason == AssignmentReason::LocalFallback)
                .count(),
            2
        );
    }

    #[test]
    fn oversized_block_and_no_eligible_peers_fall_back_locally() {
        let blocks = [SchedulableBlock {
            block_id: "huge".into(),
            estimated_bytes: 101,
        }];
        let workers = [worker("a")];
        let offers = [offer("a", "huge", LocalityClass::CompleteMain)];
        let mut config = SchedulerConfig::default();
        config.maximum_bytes_per_worker = 100;
        let assignment = &run(&blocks, &workers, &offers, &config).assignments[0];
        assert_eq!(assignment.worker_id, "local");
        assert_eq!(assignment.reason, AssignmentReason::LocalFallback);
    }

    #[test]
    fn configuration_is_bounded() {
        let mut scheduler = SchedulerConfig::default();
        scheduler.maximum_workers_per_query = MAX_WORKERS_PER_QUERY + 1;
        assert_eq!(
            scheduler.validate().unwrap_err().field,
            "maximum_workers_per_query"
        );
        let mut policy = EligibilityPolicy::default();
        policy.maximum_memory_pressure_per_mille = 1_001;
        assert_eq!(
            policy.validate().unwrap_err().field,
            "maximum_memory_pressure_per_mille"
        );
    }
}
