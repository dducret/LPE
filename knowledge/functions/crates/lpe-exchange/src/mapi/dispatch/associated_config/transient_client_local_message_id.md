---
type: Rust Function
title: transient_client_local_message_id
resource: crates/lpe-exchange/src/mapi/dispatch/associated_config.rs#L669-L672
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_read_state/append_synchronization_import_read_state_changes_response
---

# Signature

`pub(super) fn transient_client_local_message_id(message_id: u64) -> bool`

# Calls

- [global_counter_from_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)

# Called by

- [append_synchronization_import_deletes_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response.md)
- [append_synchronization_import_read_state_changes_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_read_state/append_synchronization_import_read_state_changes_response.md)