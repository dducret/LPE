---
type: Rust Function
title: property_names_from_arguments
resource: crates/lpe-jmap/src/service/helpers.rs#L23-L34
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_get
---

# Signature

`pub(super) fn property_names_from_arguments(arguments: &Value) -> Option<HashSet<String>>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [handle_canonical_get](../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_get.md)