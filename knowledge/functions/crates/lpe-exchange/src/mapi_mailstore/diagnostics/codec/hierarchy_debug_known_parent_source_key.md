---
type: Rust Function
title: hierarchy_debug_known_parent_source_key
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L926-L930
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/finish_hierarchy_debug_folder
---

# Signature

`pub(super) fn hierarchy_debug_known_parent_source_key(source_key: &[u8]) -> bool`

# Calls

- [source_key_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)

# Called by

- [finish_hierarchy_debug_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/finish_hierarchy_debug_folder.md)