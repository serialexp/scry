//! The one human-readable duration parser for scry's CLIs.
//!
//! Six near-identical copies of this used to live in `scry-agent`,
//! `scry-gateway`, `noise-spewer`, `scry-replay-opensearch`,
//! `scry-retention`, and `scry-ingestd`. They disagreed on which units they
//! accepted (some knew `h`/`d`, some stopped at `m`) and they all multiplied
//! **unchecked**, which release builds do not trap: `--ttl 300000000000000000000d`
//! wrapped to a small number of seconds. For a flag like `--ttl`, paired with
//! `--apply`, that turns a typo into an irreversible mass deletion.
//!
//! One parser, one grammar, one overflow policy:
//!
//! - `<integer><unit>` with optional surrounding whitespace.
//! - Units: `ms`, `s`, `m` (minutes), `h`, `d`. A bare integer means seconds.
//! - Every multiplication is checked; anything that would wrap is a parse
//!   **error**, never a silently-smaller duration.
//!
//! Returns `Result<Duration, String>` because that is the shape clap's
//! `value_parser` wants.

use std::time::Duration;

/// Seconds per unit. `ms` is handled separately (sub-second).
const MINUTE: u64 = 60;
const HOUR: u64 = 60 * MINUTE;
const DAY: u64 = 24 * HOUR;

/// Parse a human-readable duration: `500ms`, `30s`, `5m`, `2h`, `7d`, or a
/// bare integer (seconds).
///
/// # Errors
/// Non-integer numbers, unknown units, and values whose unit conversion would
/// overflow `u64` seconds.
///
/// ```
/// use std::time::Duration;
/// use scry_duration::parse_duration;
///
/// assert_eq!(parse_duration("7d").unwrap(), Duration::from_secs(7 * 86_400));
/// assert_eq!(parse_duration("30").unwrap(), Duration::from_secs(30));
/// assert!(parse_duration("18446744073709551615d").is_err()); // overflow, not a wrap
/// ```
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let trimmed = s.trim();
    let (num, unit) = trimmed
        .find(|c: char| c.is_alphabetic())
        .map(|i| (&trimmed[..i], &trimmed[i..]))
        .unwrap_or((trimmed, "s"));

    let n: u64 = num
        .trim()
        .parse()
        .map_err(|_| format!("bad number in {s:?}"))?;

    // Checked throughout: an overflowing duration must fail loudly rather
    // than wrap into a plausible-looking small one.
    let secs = |mult: u64| -> Result<Duration, String> {
        n.checked_mul(mult)
            .map(Duration::from_secs)
            .ok_or_else(|| format!("duration {s:?} overflows"))
    };

    match unit.trim() {
        "ms" => Ok(Duration::from_millis(n)),
        "s" | "" => secs(1),
        "m" => secs(MINUTE),
        "h" => secs(HOUR),
        "d" => secs(DAY),
        other => Err(format!("unknown duration unit {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_unit() {
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7_200));
        assert_eq!(parse_duration("7d").unwrap(), Duration::from_secs(604_800));
    }

    #[test]
    fn a_bare_integer_is_seconds() {
        assert_eq!(parse_duration("30").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("0").unwrap(), Duration::ZERO);
    }

    #[test]
    fn tolerates_surrounding_and_internal_whitespace() {
        assert_eq!(parse_duration("  10m ").unwrap(), Duration::from_secs(600));
        assert_eq!(parse_duration("10 m").unwrap(), Duration::from_secs(600));
    }

    #[test]
    fn rejects_unknown_units_and_bad_numbers() {
        assert!(parse_duration("10x").is_err());
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("").is_err());
        assert!(parse_duration("-5s").is_err(), "no negative durations");
        assert!(parse_duration("1.5s").is_err(), "integers only");
    }

    /// The reason this crate exists: release builds don't trap overflow, so an
    /// absurd `--ttl` used to wrap to a *tiny* TTL — and a tiny TTL with
    /// `--apply` reaps almost everything.
    #[test]
    fn overflow_is_an_error_not_a_wrap() {
        for input in [
            "18446744073709551615d",
            "18446744073709551615h",
            "18446744073709551615m",
            "1000000000000000000d",
        ] {
            let err = parse_duration(input)
                .expect_err(&format!("{input} must not parse"))
                .to_string();
            assert!(err.contains("overflows"), "{input}: {err}");
        }
    }

    #[test]
    fn the_largest_representable_value_of_each_unit_still_parses() {
        // Boundary check: checked_mul must not reject values that do fit.
        assert_eq!(
            parse_duration(&format!("{}d", u64::MAX / DAY)).unwrap(),
            Duration::from_secs((u64::MAX / DAY) * DAY)
        );
        assert_eq!(
            parse_duration(&format!("{}ms", u64::MAX)).unwrap(),
            Duration::from_millis(u64::MAX),
            "ms needs no multiplication and so has no ceiling below u64::MAX"
        );
    }
}
