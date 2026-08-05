---
type: Rust Function
title: string_ids_from_arguments
resource: crates/lpe-jmap/src/service/helpers.rs#L14-L21
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_search_folder_set
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_get
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_unsupported_write
---

# Signature

`pub(super) fn string_ids_from_arguments(arguments: &Value, field: &str) -> Option<Vec<String>>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [handle_search_folder_set](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_search_folder_set.md)
- [handle_canonical_get](../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_get.md)
- [handle_canonical_unsupported_write](../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_unsupported_write.md)