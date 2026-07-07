//! Prometheus-style label matchers, shared across scry.
//!
//! A [`LabelFilter`] is a set of matchers ANDed together: a label set passes
//! only if it satisfies **every** matcher (logical AND). An empty filter keeps
//! everything, so any feature built on it is fully opt-in.
//!
//! Two consumers today:
//!
//! - the agent's node-side keep allow-list (`--keep`, D-043) — a container log
//!   stream is shipped only if its labels satisfy the filter;
//! - the ingest server's live-tail subscription (`scry tail`, D-050) — a
//!   record is forwarded to a subscriber only if its labels satisfy the filter.
//!
//! Matchers follow scry's Prometheus-style convention, extended with regex:
//!
//! - `key=value`  — label equals value
//! - `key!=value` — label does not equal value
//! - `key=~regex` — label matches the (whole-string-anchored) regex
//! - `key!~regex` — label does not match the regex
//!
//! Matches run against a label set represented as [`scry_proto::LabelPair`]s. A
//! label the set does not carry is treated as the empty string, so `key=~".+"`
//! means "the label is present and non-empty".

use anyhow::{bail, Context, Result};
use regex::Regex;
use scry_proto::LabelPair;

/// A single matcher operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchOp {
    /// `=` — equals.
    Eq,
    /// `!=` — not equals.
    Ne,
    /// `=~` — regex match.
    Re,
    /// `!~` — regex non-match.
    Nre,
}

/// One parsed `key<op>value` matcher. For the regex ops the pattern is compiled
/// once at parse time and anchored to the whole string.
#[derive(Debug, Clone)]
pub struct Matcher {
    key: String,
    op: MatchOp,
    value: String,
    re: Option<Regex>,
}

impl Matcher {
    /// Parse a single matcher spec, e.g. `namespace=~"prod-.*"`.
    pub fn parse(spec: &str) -> Result<Self> {
        let spec = spec.trim();
        let (key, op, rest) = split_op(spec).with_context(|| {
            format!("matcher `{spec}` must be key=value | key!=value | key=~regex | key!~regex")
        })?;
        if key.is_empty() {
            bail!("matcher `{spec}` has an empty label key");
        }
        // A value may be optionally double-quoted; strip a single matching pair.
        let value = strip_quotes(rest).to_string();

        let re = match op {
            MatchOp::Re | MatchOp::Nre => {
                // Anchor to the whole string, exactly like Prometheus label
                // matchers, so `=~"prod"` does not match `production`.
                let anchored = format!("^(?:{value})$");
                Some(
                    Regex::new(&anchored)
                        .with_context(|| format!("invalid regex in matcher `{spec}`"))?,
                )
            }
            MatchOp::Eq | MatchOp::Ne => None,
        };

        Ok(Self {
            key: key.to_string(),
            op,
            value,
            re,
        })
    }

    /// Evaluate this matcher against a label set. A missing label is treated as
    /// the empty string.
    fn matches(&self, labels: &[LabelPair]) -> bool {
        let actual = labels
            .iter()
            .find(|l| l.key == self.key)
            .map(|l| l.value.as_str())
            .unwrap_or("");
        match self.op {
            MatchOp::Eq => actual == self.value,
            MatchOp::Ne => actual != self.value,
            MatchOp::Re => self.re.as_ref().is_some_and(|re| re.is_match(actual)),
            MatchOp::Nre => self.re.as_ref().is_some_and(|re| !re.is_match(actual)),
        }
    }
}

/// A set of matchers, ANDed together. Empty ⇒ keep everything.
#[derive(Debug, Clone, Default)]
pub struct LabelFilter {
    matchers: Vec<Matcher>,
}

impl LabelFilter {
    /// Build a filter from a list of matcher specs (e.g. the repeated `--keep`
    /// flag values, or a tail subscription's matchers). An empty list yields a
    /// keep-everything filter.
    pub fn parse(specs: &[String]) -> Result<Self> {
        let matchers = specs
            .iter()
            .map(|s| Matcher::parse(s))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { matchers })
    }

    /// `true` when no matchers are configured (keep everything).
    pub fn is_empty(&self) -> bool {
        self.matchers.is_empty()
    }

    /// Number of configured matchers.
    pub fn len(&self) -> usize {
        self.matchers.len()
    }

    /// Whether a label set should be kept. A set is kept only if it satisfies
    /// **every** matcher; an empty filter keeps all.
    pub fn keeps(&self, labels: &[LabelPair]) -> bool {
        self.matchers.iter().all(|m| m.matches(labels))
    }
}

/// Split `spec` into `(key, op, value)` at the first operator. Two-character
/// operators (`=~`, `!~`, `!=`) are detected before the single-character `=`.
fn split_op(spec: &str) -> Option<(&str, MatchOp, &str)> {
    let bytes = spec.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'=' => {
                if bytes.get(i + 1) == Some(&b'~') {
                    return Some((&spec[..i], MatchOp::Re, &spec[i + 2..]));
                }
                return Some((&spec[..i], MatchOp::Eq, &spec[i + 1..]));
            }
            b'!' => match bytes.get(i + 1) {
                Some(&b'=') => return Some((&spec[..i], MatchOp::Ne, &spec[i + 2..])),
                Some(&b'~') => return Some((&spec[..i], MatchOp::Nre, &spec[i + 2..])),
                _ => return None, // bare `!` is not a valid operator
            },
            _ => {}
        }
    }
    None
}

/// Strip one matching pair of surrounding double quotes, if present.
fn strip_quotes(s: &str) -> &str {
    let s = s.trim();
    if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
        &s[1..s.len() - 1]
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lbl(pairs: &[(&str, &str)]) -> Vec<LabelPair> {
        pairs
            .iter()
            .map(|(k, v)| LabelPair {
                key: (*k).into(),
                value: (*v).into(),
            })
            .collect()
    }

    fn f(specs: &[&str]) -> LabelFilter {
        LabelFilter::parse(&specs.iter().map(|s| s.to_string()).collect::<Vec<_>>()).unwrap()
    }

    #[test]
    fn parses_all_four_operators() {
        assert_eq!(Matcher::parse("a=b").unwrap().op, MatchOp::Eq);
        assert_eq!(Matcher::parse("a!=b").unwrap().op, MatchOp::Ne);
        assert_eq!(Matcher::parse("a=~b").unwrap().op, MatchOp::Re);
        assert_eq!(Matcher::parse("a!~b").unwrap().op, MatchOp::Nre);
    }

    #[test]
    fn strips_surrounding_quotes_from_value() {
        let m = Matcher::parse("namespace=~\"prod|staging\"").unwrap();
        assert_eq!(m.value, "prod|staging");
        assert!(m.matches(&lbl(&[("namespace", "prod")])));
        assert!(m.matches(&lbl(&[("namespace", "staging")])));
        assert!(!m.matches(&lbl(&[("namespace", "dev")])));
    }

    #[test]
    fn malformed_specs_error() {
        assert!(Matcher::parse("nooperator").is_err());
        assert!(Matcher::parse("=value").is_err()); // empty key
        assert!(Matcher::parse("a!b").is_err()); // bare !
        assert!(Matcher::parse("a=~(").is_err()); // invalid regex
    }

    #[test]
    fn empty_filter_keeps_everything() {
        let filter = LabelFilter::parse(&[]).unwrap();
        assert!(filter.is_empty());
        assert!(filter.keeps(&lbl(&[("namespace", "anything")])));
        assert!(filter.keeps(&[]));
    }

    #[test]
    fn matchers_are_anded() {
        let filter = f(&["namespace=prod", "container=~\"api.*\""]);
        assert!(filter.keeps(&lbl(&[("namespace", "prod"), ("container", "api-server")])));
        // Fails the second matcher.
        assert!(!filter.keeps(&lbl(&[("namespace", "prod"), ("container", "sidecar")])));
        // Fails the first matcher.
        assert!(!filter.keeps(&lbl(&[("namespace", "dev"), ("container", "api-server")])));
    }

    #[test]
    fn absent_label_is_empty_string() {
        // `!=` against an absent label: "" != "prod" ⇒ kept.
        assert!(f(&["namespace!=prod"]).keeps(&lbl(&[("pod", "x")])));
        // `=` against an absent label: "" == "prod" is false ⇒ dropped.
        assert!(!f(&["namespace=prod"]).keeps(&lbl(&[("pod", "x")])));
        // presence test: `.+` requires a non-empty value.
        assert!(!f(&["k8s_app=~\".+\""]).keeps(&lbl(&[("pod", "x")])));
        assert!(f(&["k8s_app=~\".+\""]).keeps(&lbl(&[("k8s_app", "web")])));
    }

    #[test]
    fn regex_is_whole_string_anchored() {
        let filter = f(&["namespace=~prod"]);
        assert!(filter.keeps(&lbl(&[("namespace", "prod")])));
        // Anchored: a substring match must not pass.
        assert!(!filter.keeps(&lbl(&[("namespace", "production")])));
    }

    #[test]
    fn value_may_contain_equals_after_first_operator() {
        // Only the first operator splits; later `=` are part of the value.
        let m = Matcher::parse("k8s_app=a=b").unwrap();
        assert_eq!(m.op, MatchOp::Eq);
        assert!(m.matches(&lbl(&[("k8s_app", "a=b")])));
    }
}
