//! Per-block body bloom — the v0.7 full-text skip index for logs.
//!
//! A [`BodyBloom`] is a classic bit-array bloom filter over the **byte
//! n-grams** (trigrams by default) of every log body in a block. It is
//! built once, at block-seal time, from the complete set of bodies — so
//! the filter is sized optimally for its exact distinct-gram count, and
//! it exists the instant the block does (no out-of-band compaction step).
//!
//! The query side decomposes a search substring into the same n-grams and
//! tests them: if **any** gram is absent from the bloom, the substring
//! cannot occur in any body in the block, so the whole block is skipped
//! without reading its parquet. The crucial property is one-sided error —
//! **false positives** (a wasted scan) are possible, **false negatives**
//! (skipping a block that actually matches) are not. The exact
//! `contains(body, pat)` predicate in the query stays in place as the
//! correctness backstop, so the bloom is a pure accelerator.
//!
//! ## Correctness invariant
//!
//! Build and query MUST n-gram and hash identically. We n-gram over raw
//! **bytes** (not chars) and hash the gram bytes case-sensitively, which
//! matches the case-sensitive `contains` predicate exactly: if a pattern
//! `P` (with `P.len() >= ngram`) occurs in a body, then every n-gram
//! window of `P` occurs in that body and was inserted, so all of `P`'s
//! grams test present and the block is kept. Patterns shorter than
//! `ngram` produce no grams; [`BodyBloom::contains_pattern`] returns
//! `true` for them (the bloom can't rule them out — the query scans).
//!
//! ## Serialised form (the `<uuid>.body.bloom` sidecar)
//!
//! ```text
//! magic "SBLM" (4) | version u8 | ngram u8 | k u32 BE | m_bits u64 BE | bitset
//! ```
//!
//! Big-endian, like everything else scry puts on the wire / in the bucket.

use std::hash::Hasher;
use twox_hash::XxHash64;

const MAGIC: [u8; 4] = *b"SBLM";
const FORMAT_VERSION: u8 = 1;
/// Two independent seeds → two hashes per gram, combined by the
/// Kirsch–Mitzenmacher double-hashing scheme `g_i = h1 + i·h2`.
const SEED1: u64 = 0;
const SEED2: u64 = 0x9E37_79B9_7F4A_7C15;
const HEADER_LEN: usize = 4 + 1 + 1 + 4 + 8;

/// A built body bloom: bit array plus the parameters needed to probe it.
#[derive(Debug, Clone)]
pub struct BodyBloom {
    ngram: u8,
    k: u32,
    m_bits: u64,
    bits: Vec<u8>,
}

/// Streaming accumulator for a [`BodyBloom`].
///
/// Bodies are fed in one at a time via [`add_body`](Self::add_body) and
/// the filter is materialised by [`finish`](Self::finish). This is the
/// shape the compactor needs: it merges blocks by streaming sorted
/// record batches through DataFusion and can feed each batch's bodies
/// here without ever holding the full merged body set in memory. The
/// only state that grows is the distinct-gram set (the `(h1, h2)` pairs)
/// — exactly the same bound as the one-shot [`BodyBloom::build_from_bodies`],
/// which is now a thin wrapper over this builder.
#[derive(Debug, Clone)]
pub struct BodyBloomBuilder {
    ngram: usize,
    /// Dedup on `h1` (a 64-bit collision is negligible and would only
    /// nudge sizing). Mirrors the one-shot path exactly so the two
    /// produce bit-identical filters for the same body set.
    seen: std::collections::HashSet<u64>,
    grams: Vec<(u64, u64)>,
}

impl BodyBloomBuilder {
    /// Start a builder n-gramming over bytes at width `cfg_ngram` (`0`
    /// is treated as `1`, matching `build_from_bodies`).
    pub fn new(cfg_ngram: usize) -> Self {
        Self {
            ngram: cfg_ngram.max(1).min(u8::MAX as usize),
            seen: std::collections::HashSet::new(),
            grams: Vec::new(),
        }
    }

    /// Feed one body into the accumulator. Bodies shorter than the
    /// n-gram width contribute no grams (the bloom can't index them; the
    /// query scans for short patterns regardless).
    pub fn add_body(&mut self, body: &str) {
        let bytes = body.as_bytes();
        if bytes.len() < self.ngram {
            return;
        }
        for window in bytes.windows(self.ngram) {
            let (h1, h2) = hash_pair(window);
            if self.seen.insert(h1) {
                self.grams.push((h1, h2));
            }
        }
    }

    /// Size `(m, k)` for the exact distinct-gram count at `target_fpr`
    /// and fill the bit array. Consumes the builder.
    pub fn finish(self, target_fpr: f64) -> BodyBloom {
        let n = self.grams.len() as u64;
        let (m_bits, k) = optimal_params(n, target_fpr);
        let mut bloom = BodyBloom {
            ngram: self.ngram as u8,
            k,
            m_bits,
            bits: vec![0u8; m_bits.div_ceil(8) as usize],
        };
        for (h1, h2) in self.grams {
            bloom.set_gram(h1, h2);
        }
        bloom
    }
}

/// `(h1, h2)` for a gram. Two seeded xxhashes → two independent 64-bit
/// values for double hashing.
fn hash_pair(gram: &[u8]) -> (u64, u64) {
    let mut a = XxHash64::with_seed(SEED1);
    a.write(gram);
    let mut b = XxHash64::with_seed(SEED2);
    b.write(gram);
    (a.finish(), b.finish())
}

impl BodyBloom {
    /// Build a bloom from an iterator of body strings, n-gramming over
    /// bytes at width `cfg_ngram` and sizing for `target_fpr`.
    ///
    /// `target_fpr` is clamped to a sane open interval; `ngram` of 0 is
    /// treated as 1. Empty input (no grams at all) yields a 1-bit empty
    /// filter that rejects every probe — correct, since a block with no
    /// grammable bodies contains no substring of length `>= ngram`.
    pub fn build_from_bodies<'a, I>(bodies: I, cfg_ngram: usize, target_fpr: f64) -> Self
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut builder = BodyBloomBuilder::new(cfg_ngram);
        for body in bodies {
            builder.add_body(body);
        }
        builder.finish(target_fpr)
    }

    /// Does this block possibly contain `pattern` as a literal byte
    /// substring? `false` = definitely not (safe to skip the block).
    /// `true` = maybe (scan to be sure). Patterns shorter than `ngram`
    /// always return `true` (the bloom can't rule them out).
    pub fn contains_pattern(&self, pattern: &str) -> bool {
        let bytes = pattern.as_bytes();
        let ngram = self.ngram as usize;
        if bytes.len() < ngram {
            return true;
        }
        // Every gram of the pattern must be present. The first absent one
        // proves the pattern can't occur anywhere in the block.
        for window in bytes.windows(ngram) {
            let (h1, h2) = hash_pair(window);
            if !self.test_gram(h1, h2) {
                return false;
            }
        }
        true
    }

    /// On-disk / in-bucket byte size of this filter.
    pub fn byte_len(&self) -> usize {
        HEADER_LEN + self.bits.len()
    }

    /// Serialise to the `<uuid>.body.bloom` sidecar bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.byte_len());
        out.extend_from_slice(&MAGIC);
        out.push(FORMAT_VERSION);
        out.push(self.ngram);
        out.extend_from_slice(&self.k.to_be_bytes());
        out.extend_from_slice(&self.m_bits.to_be_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    /// Parse a `<uuid>.body.bloom` sidecar. Returns `None` on a bad
    /// magic / version / truncated buffer — callers treat that as "no
    /// usable bloom" and fall back to scanning (never a correctness risk).
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < HEADER_LEN || buf[0..4] != MAGIC || buf[4] != FORMAT_VERSION {
            return None;
        }
        let ngram = buf[5];
        let k = u32::from_be_bytes(buf[6..10].try_into().ok()?);
        let m_bits = u64::from_be_bytes(buf[10..18].try_into().ok()?);
        let expected_bytes = m_bits.div_ceil(8) as usize;
        let bits = buf.get(HEADER_LEN..HEADER_LEN + expected_bytes)?.to_vec();
        Some(BodyBloom {
            ngram,
            k,
            m_bits,
            bits,
        })
    }

    fn set_gram(&mut self, h1: u64, h2: u64) {
        for i in 0..self.k as u64 {
            let pos = h1.wrapping_add(i.wrapping_mul(h2)) % self.m_bits;
            self.bits[(pos / 8) as usize] |= 1 << (pos % 8);
        }
    }

    fn test_gram(&self, h1: u64, h2: u64) -> bool {
        for i in 0..self.k as u64 {
            let pos = h1.wrapping_add(i.wrapping_mul(h2)) % self.m_bits;
            if self.bits[(pos / 8) as usize] & (1 << (pos % 8)) == 0 {
                return false;
            }
        }
        true
    }
}

/// Optimal `(m_bits, k)` for `n` distinct items at false-positive rate
/// `p`: `m = -n·ln p / (ln2)²`, `k = (m/n)·ln2`. Both clamped to `>= 1`.
/// `n == 0` yields the smallest filter that rejects everything.
fn optimal_params(n: u64, p: f64) -> (u64, u32) {
    if n == 0 {
        return (1, 1);
    }
    let p = p.clamp(1e-6, 0.5);
    let ln2 = std::f64::consts::LN_2;
    let m = (-(n as f64) * p.ln() / (ln2 * ln2)).ceil();
    let m_bits = (m as u64).max(1);
    let k = (((m_bits as f64) / (n as f64)) * ln2).round() as u32;
    (m_bits, k.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tiny deterministic xorshift so the property test needs no `rand`
    /// dependency and is reproducible.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn byte(&mut self) -> u8 {
            // Restrict to a small alphabet so substrings actually recur.
            b'a' + (self.next() % 12) as u8
        }
    }

    #[test]
    fn roundtrip_preserves_membership() {
        let bodies = ["connection refused", "req_id=8f3a91c2 ok", "timeout"];
        let bloom = BodyBloom::build_from_bodies(bodies.iter().copied(), 3, 0.01);
        let bytes = bloom.to_bytes();
        let back = BodyBloom::from_bytes(&bytes).expect("parse");
        for needle in ["refused", "8f3a", "timeout", "req_id"] {
            assert_eq!(
                bloom.contains_pattern(needle),
                back.contains_pattern(needle),
                "needle {needle:?} differs after roundtrip"
            );
            assert!(
                back.contains_pattern(needle),
                "real substring {needle:?} lost"
            );
        }
    }

    #[test]
    fn short_pattern_is_never_pruned() {
        let bloom = BodyBloom::build_from_bodies(["hello world"].iter().copied(), 3, 0.01);
        // Patterns shorter than the n-gram width can't be grammed.
        assert!(bloom.contains_pattern("he"));
        assert!(bloom.contains_pattern("x")); // even one absent from the data
        assert!(bloom.contains_pattern(""));
    }

    #[test]
    fn empty_block_rejects_real_grams() {
        // No grammable bodies → empty filter → any >=ngram probe is pruned.
        let bloom = BodyBloom::build_from_bodies(["ab", ""].iter().copied(), 3, 0.01);
        assert!(!bloom.contains_pattern("abc"));
    }

    #[test]
    fn no_false_negatives_property() {
        // Generate random bodies; assert every substring that ACTUALLY
        // occurs (length >= ngram) tests present. This is the load-bearing
        // guarantee: the bloom must never drop a block that matches.
        let ngram = 3;
        let mut rng = Rng(0x1234_5678_9abc_def0);
        let mut bodies: Vec<String> = Vec::new();
        for _ in 0..50 {
            let len = 3 + (rng.next() % 30) as usize;
            let s: String = (0..len).map(|_| rng.byte() as char).collect();
            bodies.push(s);
        }
        let bloom = BodyBloom::build_from_bodies(bodies.iter().map(|s| s.as_str()), ngram, 0.01);

        for body in &bodies {
            let b = body.as_bytes();
            // Test several real substrings of varying length.
            for start in 0..b.len() {
                for end in (start + ngram)..=b.len() {
                    let sub = std::str::from_utf8(&b[start..end]).unwrap();
                    assert!(
                        bloom.contains_pattern(sub),
                        "false negative: {sub:?} occurs in {body:?} but bloom pruned it"
                    );
                }
            }
        }
    }

    #[test]
    fn streaming_builder_matches_one_shot() {
        // The compactor feeds bodies through BodyBloomBuilder batch by
        // batch; that path must produce a bit-identical filter to the
        // one-shot build_from_bodies over the same set (same dedup, same
        // sizing, same fill order).
        let bodies = [
            "connection refused from 10.0.0.1",
            "req_id=8f3a91c2 status=200 ok",
            "timeout after 30s",
            "connection refused from 10.0.0.2",
        ];
        let one_shot = BodyBloom::build_from_bodies(bodies.iter().copied(), 3, 0.01);

        let mut builder = BodyBloomBuilder::new(3);
        for b in &bodies {
            builder.add_body(b);
        }
        let streamed = builder.finish(0.01);

        assert_eq!(
            one_shot.to_bytes(),
            streamed.to_bytes(),
            "streaming builder must produce a bit-identical filter"
        );
    }

    #[test]
    fn sizing_hits_target_fpr_order_of_magnitude() {
        // Build over a known distinct-gram set, then probe with grams that
        // were NOT inserted and confirm the empirical FP rate is near the
        // 1% target (loose bound — this is a sanity check, not a proof).
        let mut rng = Rng(0xdead_beef_cafe_babe);
        let mut bodies = Vec::new();
        for _ in 0..200 {
            let len = 10 + (rng.next() % 20) as usize;
            let s: String = (0..len).map(|_| rng.byte() as char).collect();
            bodies.push(s);
        }
        let bloom = BodyBloom::build_from_bodies(bodies.iter().map(|s| s.as_str()), 3, 0.01);

        // Probe with random trigrams over a DISJOINT alphabet so they were
        // never inserted; every "present" is a false positive.
        let mut fp = 0u32;
        let trials = 5000u32;
        for _ in 0..trials {
            let g: String = (0..3)
                .map(|_| (b'A' + (rng.next() % 12) as u8) as char)
                .collect();
            if bloom.contains_pattern(&g) {
                fp += 1;
            }
        }
        let rate = fp as f64 / trials as f64;
        assert!(
            rate < 0.05,
            "false-positive rate {rate} far above 1% target"
        );
    }
}
