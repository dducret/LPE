---
type: Rust Function
title: pending_message_is_trash_sync_artifact
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L810-L832
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/imported_message_source_key
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/import_source_key_identity_scope
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
---

# Signature

`pub(super) fn pending_message_is_trash_sync_artifact( folder_id: u64, properties: &HashMap<u32, MapiValue>, recipients: &[PendingRecipient], ) -> bool`

# Calls

- [imported_message_source_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/imported_message_source_key.md)
- [import_source_key_identity_scope](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/import_source_key_identity_scope.md)

# Called by

- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)