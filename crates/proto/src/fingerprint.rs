//! Series / log-stream label fingerprinting.
//!
//! `xxh3-64` over canonically-sorted labels encoded as `key\0value\0`
//! repeated. The hash is byte-for-byte stable across agents written in
//! different languages provided they agree on sort order and the NUL
//! separators.
//!
//! Schema-level discussion: see `proto/README.md` ("Conventions") and
//! ARCHITECTURE.md → Metrics → "Series fingerprint as the canonical
//! identifier."

use crate::generated::LabelPair;
use std::hash::Hasher;
use twox_hash::XxHash64;

/// xxh3-64 of canonically sorted labels.
///
/// We use the `twox_hash` 1.x port of xxh3 (`XxHash64` here as a
/// stand-in). When we move to xxh3-64 specifically, swap the hasher
/// without changing the wire layout — the fingerprint is purely a
/// content-derived identifier; nothing depends on its hash family
/// beyond agents and servers agreeing.
pub fn fingerprint(labels: &[LabelPair]) -> u64 {
    let mut pairs: Vec<&LabelPair> = labels.iter().collect();
    pairs.sort_by(|a, b| a.key.cmp(&b.key).then_with(|| a.value.cmp(&b.value)));

    let mut h = XxHash64::with_seed(0);
    for p in pairs {
        h.write(p.key.as_bytes());
        h.write(&[0u8]);
        h.write(p.value.as_bytes());
        h.write(&[0u8]);
    }
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lp(k: &str, v: &str) -> LabelPair {
        LabelPair { key: k.into(), value: v.into() }
    }

    #[test]
    fn fingerprint_is_order_independent() {
        let a = fingerprint(&[lp("host", "h1"), lp("job", "scry")]);
        let b = fingerprint(&[lp("job", "scry"), lp("host", "h1")]);
        assert_eq!(a, b);
    }

    #[test]
    fn fingerprint_distinguishes_labels() {
        let a = fingerprint(&[lp("host", "h1")]);
        let b = fingerprint(&[lp("host", "h2")]);
        assert_ne!(a, b);
    }

    #[test]
    fn fingerprint_distinguishes_keys_from_values() {
        // Catches a classic bug where you concat without separators:
        // key="ab", value="" would collide with key="a", value="b".
        let a = fingerprint(&[lp("ab", "")]);
        let b = fingerprint(&[lp("a", "b")]);
        assert_ne!(a, b);
    }
}
