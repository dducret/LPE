---
type: Rust Function
title: canonical_query_filter
resource: crates/lpe-jmap/src/service/helpers.rs#L200-L211
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query_changes
---

# Signature

`pub(super) fn canonical_query_filter(data_type: &str, arguments: &Value) -> Option<Value>`

# Called by

- [handle_canonical_query_changes](../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_query_changes.md)