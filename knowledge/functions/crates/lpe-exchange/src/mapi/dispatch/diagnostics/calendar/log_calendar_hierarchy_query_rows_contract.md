---
type: Rust Function
title: log_calendar_hierarchy_query_rows_contract
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar.rs#L86-L163
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_calendar_hierarchy_query_rows_contract( principal: &AccountPrincipal, object: Option<&MapiObject>, snapshot: &MapiMailStoreSnapshot, )`

# Calls

- [canonical_property_storage_tag](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [source_key_for_store_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [collaboration_folder_for_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)

# Called by

- [append_query_rows_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)