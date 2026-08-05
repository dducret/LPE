---
type: Rust Function
title: imported_property_source_key_global_counter
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L695-L706
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/source_key_global_counter
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response
---

# Signature

`pub(super) fn imported_property_source_key_global_counter( properties: &[(u32, MapiValue)], ) -> Option<u64>`

# Calls

- [source_key_global_counter](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/source_key_global_counter.md)

# Called by

- [append_synchronization_import_message_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response.md)