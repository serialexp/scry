use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

pub const MAX_TEMPLATE_BYTES: usize = 32 * 1024;
pub const MAX_TEMPLATE_NODES: usize = 1_024;
pub const MAX_TEMPLATE_DEPTH: usize = 16;
pub const MAX_PLACEHOLDERS: usize = 128;
pub const MAX_RENDERED_BYTES: usize = 64 * 1024;

const ALLOWED: &[&str] = &[
    "event_id",
    "transition",
    "monitor_name",
    "status",
    "value",
    "scry_url",
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledJsonTemplate {
    ast: Value,
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum TemplateError {
    #[error("template is not valid JSON: {0}")]
    Json(String),
    #[error("template exceeds a structural or byte bound")]
    Bounds,
    #[error("placeholder `{0}` is not allowed")]
    UnknownPlaceholder(String),
    #[error("placeholder syntax is invalid")]
    InvalidPlaceholder,
    #[error("rendered template exceeds {MAX_RENDERED_BYTES} bytes")]
    OutputTooLarge,
}

#[derive(Clone, Copy, Debug)]
pub struct TemplateValues<'a> {
    pub event_id: &'a str,
    pub transition: &'a str,
    pub monitor_name: &'a str,
    pub status: &'a str,
    pub value: &'a str,
    pub scry_url: &'a str,
}
impl TemplateValues<'_> {
    fn get(&self, name: &str) -> &str {
        match name {
            "event_id" => self.event_id,
            "transition" => self.transition,
            "monitor_name" => self.monitor_name,
            "status" => self.status,
            "value" => self.value,
            "scry_url" => self.scry_url,
            _ => unreachable!("validated placeholder"),
        }
    }
}

impl CompiledJsonTemplate {
    /// Parse JSON first, then compile placeholders only in JSON string values. Keys and
    /// non-string values are never interpreted, preventing generated JSON structure.
    pub fn compile(source: &str) -> Result<Self, TemplateError> {
        if source.len() > MAX_TEMPLATE_BYTES {
            return Err(TemplateError::Bounds);
        }
        let ast: Value =
            serde_json::from_str(source).map_err(|e| TemplateError::Json(e.to_string()))?;
        validate_ast(&ast)?;
        Ok(Self { ast })
    }

    pub fn render(&self, values: TemplateValues<'_>) -> Result<Vec<u8>, TemplateError> {
        let mut rendered = self.ast.clone();
        render_value(&mut rendered, values);
        let output =
            serde_json::to_vec(&rendered).map_err(|e| TemplateError::Json(e.to_string()))?;
        if output.len() > MAX_RENDERED_BYTES {
            return Err(TemplateError::OutputTooLarge);
        }
        Ok(output)
    }

    pub fn ast(&self) -> &Value {
        &self.ast
    }
}

fn placeholders(
    input: &str,
    mut visit: impl FnMut(&str) -> Result<(), TemplateError>,
) -> Result<(), TemplateError> {
    let mut rest = input;
    while let Some(start) = rest.find("{{") {
        rest = &rest[start + 2..];
        let Some(end) = rest.find("}}") else {
            return Err(TemplateError::InvalidPlaceholder);
        };
        let name = &rest[..end];
        if name.is_empty() || name.contains('{') || name.contains('}') {
            return Err(TemplateError::InvalidPlaceholder);
        }
        visit(name)?;
        rest = &rest[end + 2..];
    }
    if rest.contains("}}") {
        return Err(TemplateError::InvalidPlaceholder);
    }
    Ok(())
}

fn validate_ast(root: &Value) -> Result<(), TemplateError> {
    let mut stack = vec![(root, 1usize)];
    let mut nodes = 0usize;
    let mut count = 0usize;
    while let Some((value, depth)) = stack.pop() {
        nodes += 1;
        if nodes > MAX_TEMPLATE_NODES || depth > MAX_TEMPLATE_DEPTH {
            return Err(TemplateError::Bounds);
        }
        match value {
            Value::String(text) => placeholders(text, |name| {
                count += 1;
                if count > MAX_PLACEHOLDERS {
                    return Err(TemplateError::Bounds);
                }
                if !ALLOWED.contains(&name) {
                    return Err(TemplateError::UnknownPlaceholder(name.to_owned()));
                }
                Ok(())
            })?,
            Value::Array(values) => stack.extend(values.iter().map(|v| (v, depth + 1))),
            Value::Object(values) => stack.extend(values.values().map(|v| (v, depth + 1))),
            _ => {}
        }
    }
    Ok(())
}

fn render_value(value: &mut Value, values: TemplateValues<'_>) {
    match value {
        Value::String(text) => {
            if !text.contains("{{") {
                return;
            }
            let original = std::mem::take(text);
            let mut rest = original.as_str();
            while let Some(start) = rest.find("{{") {
                text.push_str(&rest[..start]);
                let after = &rest[start + 2..];
                let end = after.find("}}").expect("compiled placeholder");
                text.push_str(values.get(&after[..end]));
                rest = &after[end + 2..];
            }
            text.push_str(rest);
        }
        Value::Array(items) => {
            for item in items {
                render_value(item, values);
            }
        }
        Value::Object(items) => {
            for item in items.values_mut() {
                render_value(item, values);
            }
        }
        _ => {}
    }
}

impl Serialize for CompiledJsonTemplate {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.ast.serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for CompiledJsonTemplate {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let ast = Value::deserialize(deserializer)?;
        validate_ast(&ast).map_err(serde::de::Error::custom)?;
        if serde_json::to_vec(&ast)
            .map_err(serde::de::Error::custom)?
            .len()
            > MAX_TEMPLATE_BYTES
        {
            return Err(serde::de::Error::custom(TemplateError::Bounds));
        }
        Ok(Self { ast })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn values<'a>(value: &'a str) -> TemplateValues<'a> {
        TemplateValues {
            event_id: value,
            transition: "firing",
            monitor_name: "m",
            status: "firing",
            value: "1",
            scry_url: "https://scry/",
        }
    }
    #[test]
    fn renders_json_escaped_strings() {
        let t = CompiledJsonTemplate::compile(r#"{"id":"prefix {{event_id}}","literal":{"x":1}}"#)
            .unwrap();
        let out = t.render(values("a\"b")).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&out).unwrap()["id"],
            "prefix a\"b"
        );
    }
    #[test]
    fn rejects_placeholders_outside_allowlist_and_malformed_syntax() {
        assert!(matches!(
            CompiledJsonTemplate::compile(r#"{"x":"{{secret}}"}"#),
            Err(TemplateError::UnknownPlaceholder(_))
        ));
        assert_eq!(
            CompiledJsonTemplate::compile(r#"{"x":"{{event_id"}"#).unwrap_err(),
            TemplateError::InvalidPlaceholder
        );
    }
    #[test]
    fn placeholders_in_keys_are_literal() {
        let t = CompiledJsonTemplate::compile(r#"{"{{event_id}}":"ok"}"#).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&t.render(values("changed")).unwrap()).unwrap()
                ["{{event_id}}"],
            "ok"
        );
    }
}
