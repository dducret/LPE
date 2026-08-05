---
type: Rust Function
title: debug_folder_row_property_value
resource: crates/lpe-exchange/src/mapi/tables/diagnostics.rs#L191-L213
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries
---

# Signature

`pub(super) fn debug_folder_row_property_value<F>( value: F, folder_id: u64, parent_folder_id: u64, property_tag: u32, mailbox_guid: Uuid, associated_count: u32, ) -> Option<MapiValue> where F: FnOnce() -> Option<MapiValue>,`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [source_key_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)

# Called by

- [outlook_bootstrap_row_invariant_summaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries.md)