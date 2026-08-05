---
type: Rust Function
title: canonical_create_ids
resource: crates/lpe-jmap/src/service/helpers.rs#L66-L73
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/object_keys
  called_by:
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_unsupported_write
---

# Signature

`pub(super) fn canonical_create_ids(arguments: &Value) -> Vec<String>`

# Calls

- [object_keys](../../../../../../functions/crates/lpe-jmap/src/service/helpers/object_keys.md)

# Called by

- [handle_canonical_unsupported_write](../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_unsupported_write.md)