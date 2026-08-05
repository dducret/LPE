---
type: Rust Function
title: project_get_properties
resource: crates/lpe-jmap/src/service/helpers.rs#L36-L56
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_get
---

# Signature

`pub(super) fn project_get_properties(object: Value, properties: Option<&HashSet<String>>) -> Value`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [handle_canonical_get](../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_get.md)