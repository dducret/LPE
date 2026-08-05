---
type: Rust Function
title: append_sync_import_dispatch_response
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L95-L198
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move/append_synchronization_import_message_move_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_read_state/append_synchronization_import_read_state_changes_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_local_replica_dispatch_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) async fn append_sync_import_dispatch_response<S>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &mut MapiMailStoreSnapshot, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, ) -> bool where S: ExchangeStore,`

# Calls

- [append_synchronization_import_message_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response.md)
- [append_synchronization_import_hierarchy_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response.md)
- [append_synchronization_import_deletes_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response.md)
- [append_synchronization_import_message_move_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move/append_synchronization_import_message_move_response.md)
- [append_synchronization_import_read_state_changes_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_read_state/append_synchronization_import_read_state_changes_response.md)
- [append_local_replica_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/local_replica_sync/append_local_replica_dispatch_response.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)