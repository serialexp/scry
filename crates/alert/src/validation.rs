use std::collections::HashSet;
use std::ops::ControlFlow;

use sqlparser::{
    ast::{visit_relations, ObjectName, Query, SetExpr, Statement, TableFactor, TableWithJoins},
    dialect::GenericDialect,
    parser::Parser,
};
use thiserror::Error;

use crate::{Monitor, MONITOR_SCHEMA_VERSION};

pub const MAX_NAME_BYTES: usize = 256;
pub const MAX_TARGET_ID_BYTES: usize = 64;
pub const MAX_SQL_BYTES: usize = 16 * 1024;
pub const MAX_MATCHERS: usize = 64;
pub const MAX_MAP_ENTRIES: usize = 64;
pub const MAX_KEY_BYTES: usize = 128;
pub const MAX_VALUE_BYTES: usize = 4 * 1024;
pub const MAX_LOOKBACK_SECONDS: u64 = 31 * 24 * 60 * 60;
pub const MAX_HOLD_SECONDS: u64 = 31 * 24 * 60 * 60;
pub const MIN_INTERVAL_SECONDS: u64 = 10;
pub const MAX_INTERVAL_SECONDS: u64 = 24 * 60 * 60;

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ValidationError {
    #[error("{0}")]
    Invalid(&'static str),
    #[error("invalid SQL: {0}")]
    Sql(String),
}

pub fn validate_monitor(monitor: &Monitor) -> Result<(), ValidationError> {
    if monitor.schema_version != MONITOR_SCHEMA_VERSION {
        return Err(ValidationError::Invalid(
            "unsupported monitor schema version",
        ));
    }
    bounded_nonempty(&monitor.name, MAX_NAME_BYTES, "name is empty or too long")?;
    bounded_nonempty(
        &monitor.query.target_id,
        MAX_TARGET_ID_BYTES,
        "target ID is empty or too long",
    )?;
    if monitor.revision == 0 {
        return Err(ValidationError::Invalid("revision must be at least one"));
    }
    if monitor.every_seconds < MIN_INTERVAL_SECONDS || monitor.every_seconds > MAX_INTERVAL_SECONDS
    {
        return Err(ValidationError::Invalid(
            "evaluation interval is out of bounds",
        ));
    }
    if monitor.jitter_seconds >= monitor.every_seconds {
        return Err(ValidationError::Invalid(
            "jitter must be shorter than interval",
        ));
    }
    if monitor.query.lookback_seconds == 0 || monitor.query.lookback_seconds > MAX_LOOKBACK_SECONDS
    {
        return Err(ValidationError::Invalid("lookback is out of bounds"));
    }
    if monitor.for_seconds > MAX_HOLD_SECONDS || monitor.recover_for_seconds > MAX_HOLD_SECONDS {
        return Err(ValidationError::Invalid(
            "pending or recovery duration is out of bounds",
        ));
    }
    if monitor.created_at_unix_nano == 0
        || monitor.updated_at_unix_nano < monitor.created_at_unix_nano
    {
        return Err(ValidationError::Invalid("monitor timestamps are invalid"));
    }
    if monitor.query.matchers.len() > MAX_MATCHERS {
        return Err(ValidationError::Invalid("too many matchers"));
    }
    for matcher in &monitor.query.matchers {
        bounded_nonempty(
            &matcher.name,
            MAX_KEY_BYTES,
            "matcher name is empty or too long",
        )?;
        if matcher.value.len() > MAX_VALUE_BYTES {
            return Err(ValidationError::Invalid("matcher value is too long"));
        }
    }
    validate_map(&monitor.labels)?;
    validate_map(&monitor.annotations)?;
    if !monitor.condition.threshold.is_finite() {
        return Err(ValidationError::Invalid("threshold must be finite"));
    }
    validate_scalar_sql(&monitor.query.sql, monitor.query.signal.table_name())
}

pub fn validate_scalar_sql(sql: &str, expected_table: &str) -> Result<(), ValidationError> {
    bounded_nonempty(sql, MAX_SQL_BYTES, "SQL is empty or too long")?;
    let statements = Parser::parse_sql(&GenericDialect {}, sql)
        .map_err(|error| ValidationError::Sql(error.to_string()))?;
    if statements.len() != 1 {
        return Err(ValidationError::Invalid(
            "SQL must contain exactly one statement",
        ));
    }
    let Statement::Query(query) = &statements[0] else {
        return Err(ValidationError::Invalid(
            "only SELECT/query statements are allowed",
        ));
    };
    let mut tables = Vec::new();
    collect_query_tables(query, &mut tables)?;
    let mut all_relations = Vec::new();
    let visit = visit_relations(query, |relation| {
        match simple_name(relation) {
            Ok(name) => all_relations.push(name.to_owned()),
            Err(error) => return ControlFlow::Break(error),
        }
        ControlFlow::Continue(())
    });
    if let ControlFlow::Break(error) = visit {
        return Err(error);
    }
    if tables.len() != 1
        || tables[0] != expected_table
        || all_relations.len() != 1
        || all_relations[0] != expected_table
    {
        return Err(ValidationError::Invalid(
            "SQL must read exactly the selected signal table",
        ));
    }
    Ok(())
}

fn collect_query_tables<'a>(
    query: &'a Query,
    tables: &mut Vec<&'a str>,
) -> Result<(), ValidationError> {
    if query.with.is_some() {
        return Err(ValidationError::Invalid(
            "CTEs are not allowed in alert SQL",
        ));
    }
    match query.body.as_ref() {
        SetExpr::Select(select) => {
            if select.from.len() != 1 {
                return Err(ValidationError::Invalid(
                    "alert SQL requires exactly one FROM",
                ));
            }
            collect_table(&select.from[0], tables)
        }
        SetExpr::Query(inner) => collect_query_tables(inner, tables),
        _ => Err(ValidationError::Invalid(
            "set operations and non-SELECT query bodies are not allowed",
        )),
    }
}

fn collect_table<'a>(
    from: &'a TableWithJoins,
    tables: &mut Vec<&'a str>,
) -> Result<(), ValidationError> {
    if !from.joins.is_empty() {
        return Err(ValidationError::Invalid(
            "joins are not allowed in alert SQL",
        ));
    }
    let TableFactor::Table { name, args, .. } = &from.relation else {
        return Err(ValidationError::Invalid(
            "derived/external table factors are not allowed",
        ));
    };
    if args.is_some() {
        return Err(ValidationError::Invalid(
            "table-valued functions are not allowed",
        ));
    }
    tables.push(simple_name(name)?);
    Ok(())
}

fn simple_name(name: &ObjectName) -> Result<&str, ValidationError> {
    if name.0.len() != 1 {
        return Err(ValidationError::Invalid(
            "qualified table names are not allowed",
        ));
    }
    name.0[0]
        .as_ident()
        .map(|ident| ident.value.as_str())
        .ok_or(ValidationError::Invalid("invalid table name"))
}

fn validate_map(values: &[(String, String)]) -> Result<(), ValidationError> {
    if values.len() > MAX_MAP_ENTRIES {
        return Err(ValidationError::Invalid("too many labels or annotations"));
    }
    let mut keys = HashSet::with_capacity(values.len());
    for (key, value) in values {
        bounded_nonempty(key, MAX_KEY_BYTES, "map key is empty or too long")?;
        if value.len() > MAX_VALUE_BYTES {
            return Err(ValidationError::Invalid("map value is too long"));
        }
        if !keys.insert(key) {
            return Err(ValidationError::Invalid("duplicate map key"));
        }
    }
    Ok(())
}

fn bounded_nonempty(
    value: &str,
    maximum: usize,
    message: &'static str,
) -> Result<(), ValidationError> {
    if value.is_empty() || value.len() > maximum {
        Err(ValidationError::Invalid(message))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Comparator, ExecutionErrorPolicy, MonitorId, NoDataPolicy, ScalarCondition, ScalarQuery,
        Signal,
    };

    use super::*;

    fn monitor() -> Monitor {
        Monitor {
            schema_version: MONITOR_SCHEMA_VERSION,
            id: MonitorId::new(),
            revision: 1,
            name: "bounded monitor".into(),
            enabled: true,
            query: ScalarQuery {
                target_id: "local".into(),
                signal: Signal::Metrics,
                matchers: vec![],
                lookback_seconds: 60,
                sql: "SELECT count(*) FROM metrics".into(),
            },
            condition: ScalarCondition {
                comparator: Comparator::Gt,
                threshold: 1.0,
            },
            every_seconds: 60,
            jitter_seconds: 0,
            for_seconds: 0,
            recover_for_seconds: 0,
            no_data: NoDataPolicy::NoData,
            execution_error: ExecutionErrorPolicy::Error,
            labels: vec![],
            annotations: vec![],
            created_at_unix_nano: 1,
            updated_at_unix_nano: 1,
        }
    }

    #[test]
    fn accepts_one_query_over_expected_table() {
        validate_scalar_sql("SELECT count(*) AS value FROM metrics", "metrics").unwrap();
    }

    #[test]
    fn rejects_multiple_statements_and_mutations() {
        assert!(validate_scalar_sql("SELECT 1; SELECT 2", "metrics").is_err());
        assert!(validate_scalar_sql("DROP TABLE metrics", "metrics").is_err());
    }

    #[test]
    fn rejects_wrong_table_joins_and_external_functions() {
        assert!(validate_scalar_sql("SELECT count(*) FROM logs", "metrics").is_err());
        assert!(
            validate_scalar_sql("SELECT count(*) FROM metrics JOIN logs ON true", "metrics")
                .is_err()
        );
        assert!(validate_scalar_sql("SELECT * FROM read_csv('x')", "metrics").is_err());
        assert!(
            validate_scalar_sql("SELECT (SELECT count(*) FROM logs) FROM metrics", "metrics")
                .is_err()
        );
        assert!(validate_scalar_sql(
            "SELECT count(*) FROM metrics WHERE EXISTS (SELECT 1 FROM logs)",
            "metrics"
        )
        .is_err());
    }

    #[test]
    fn rejects_unbounded_holds_and_invalid_timestamps() {
        let mut monitor = monitor();
        monitor.for_seconds = MAX_HOLD_SECONDS + 1;
        assert_eq!(
            validate_monitor(&monitor),
            Err(ValidationError::Invalid(
                "pending or recovery duration is out of bounds"
            ))
        );

        monitor.for_seconds = 0;
        monitor.updated_at_unix_nano = monitor.created_at_unix_nano - 1;
        assert_eq!(
            validate_monitor(&monitor),
            Err(ValidationError::Invalid("monitor timestamps are invalid"))
        );
    }
}
