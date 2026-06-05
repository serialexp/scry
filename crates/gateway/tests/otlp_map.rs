//! OTLP → TracesBatch mapping fidelity.

use scry_gateway::otlp::{map_traces, sample_request};

#[test]
fn maps_request_to_traces_batch() {
    let req = sample_request(3);
    let batch = map_traces(req);

    // One resource + one scope dict entry.
    assert_eq!(batch.resources.len(), 1);
    assert_eq!(batch.scopes.len(), 1);
    assert_eq!(batch.spans.len(), 3);

    // Resource attributes carried through verbatim (keys preserved) — these
    // feed the traces block's promoted service.* / deployment.environment cols.
    let labels = &batch.resources[0].labels;
    let get = |k: &str| labels.iter().find(|l| l.key == k).map(|l| l.value.as_str());
    assert_eq!(get("service.name"), Some("api"));
    assert_eq!(get("service.namespace"), Some("shop"));
    assert_eq!(get("deployment.environment"), Some("prod"));

    assert_eq!(batch.scopes[0].name, "scry.gateway.probe");
    assert_eq!(batch.scopes[0].version, "0.1.0");

    // Root span: parent None, id widths 16/8, kind SERVER(2), status OK(1).
    let root = &batch.spans[0];
    assert_eq!(root.trace_id.len(), 16);
    assert_eq!(root.span_id.len(), 8);
    assert!(root.parent_span_id.is_none());
    assert_eq!(root.kind, 2);
    assert_eq!(root.status_code, 1);
    assert_eq!(root.resource_idx, 0);
    assert_eq!(root.scope_idx, 0);

    // Child span: parent set to the root span id.
    let child = &batch.spans[1];
    assert_eq!(
        child.parent_span_id.as_deref(),
        Some([0x22u8; 8].as_slice())
    );

    // Span attribute stringified.
    assert_eq!(
        root.attributes
            .iter()
            .find(|l| l.key == "http.method")
            .map(|l| l.value.as_str()),
        Some("GET")
    );

    // Events / links preserved with nested attributes.
    assert_eq!(root.events.len(), 1);
    assert_eq!(root.events[0].name, "checkpoint");
    assert_eq!(
        root.events[0]
            .attributes
            .iter()
            .find(|l| l.key == "phase")
            .map(|l| l.value.as_str()),
        Some("mid")
    );
    assert_eq!(root.links.len(), 1);
    assert_eq!(root.links[0].trace_id.len(), 16);
    assert_eq!(root.links[0].span_id.len(), 8);
}

#[test]
fn empty_request_maps_to_empty_batch() {
    let batch = map_traces(sample_request(0));
    assert!(batch.spans.is_empty());
    // Resource/scope dict still built from the (span-less) ResourceSpans.
    assert_eq!(batch.resources.len(), 1);
}
