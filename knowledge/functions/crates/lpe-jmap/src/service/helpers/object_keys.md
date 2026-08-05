---
type: Rust Function
title: object_keys
resource: crates/lpe-jmap/src/service/helpers.rs#L58-L64
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_unsupported_write
  - functions/crates/lpe-jmap/src/service/helpers/canonical_create_ids
---

# Signature

`pub(super) fn object_keys(arguments: &Value, field: &str) -> Vec<String>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [handle_canonical_unsupported_write](../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_unsupported_write.md)
- [canonical_create_ids](../../../../../../functions/crates/lpe-jmap/src/service/helpers/canonical_create_ids.md)